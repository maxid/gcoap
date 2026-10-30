package gcoap

import (
	"context"

	"github.com/maxid/gnety"

	"github.com/maxid/gnety/codec/coap"
)

// MaxNotificationsInFlight is the per-observation cap on confirmable
// notifications that have been sent but not yet acknowledged. A newer
// notification produced while one is in flight is coalesced: only the latest
// one is kept and it is sent after the outstanding notification is ACKed. The
// state per observation is therefore bounded by one in-flight message plus one
// queued message, independent of how often Notify is called (RFC 7641 section
// 4.5.2 allows skipping intermediate states).
const MaxNotificationsInFlight = 1

type observer struct {
	sessionID string
	token     string
	path      string
	seq       coap.Observe

	// The notification state below is guarded by Server.obsMu. Exactly
	// MaxNotificationsInFlight confirmable messages may be in flight; pending
	// holds at most one newer notification coalesced behind it.
	inflight bool
	pending  *coap.Message
	cancel   context.CancelFunc
	gen      uint64 // identifies the current notification loop
}

// observeOption returns the Observe option carrying value. gcoap writes the
// fixed three-byte form: RFC 7641 bounds the value to three bytes, and keeping
// the length fixed makes the encoding independent of the value.
func observeOption(value coap.Observe) coap.Option {
	return coap.Option{
		Number: coap.OptionObserve,
		Value:  []byte{byte(value >> 16), byte(value >> 8), byte(value)},
	}
}

func (s *Server) addObserver(sess *gnety.Session, token []byte, path string) {
	if sess == nil {
		return
	}
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	if s.observers == nil {
		s.observers = make(map[string][]*observer)
	}
	for _, o := range s.observers[path] {
		if o.sessionID == sess.ID && o.token == string(token) {
			return
		}
	}
	s.observers[path] = append(s.observers[path], &observer{
		sessionID: sess.ID,
		token:     string(token),
		path:      path,
	})
}

func (s *Server) cancelObserver(sess *gnety.Session, token []byte, path string) {
	if sess == nil {
		return
	}
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	list := s.observers[path]
	out := list[:0]
	for _, o := range list {
		if o.sessionID == sess.ID && o.token == string(token) {
			s.stopObserverLocked(o)
			continue
		}
		out = append(out, o)
	}
	if len(out) == 0 {
		delete(s.observers, path)
	} else {
		s.observers[path] = out
	}
}

func (s *Server) removeSessionObservers(sessionID string) {
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	for path, list := range s.observers {
		out := list[:0]
		for _, o := range list {
			if o.sessionID != sessionID {
				out = append(out, o)
				continue
			}
			s.stopObserverLocked(o)
		}
		if len(out) == 0 {
			delete(s.observers, path)
		} else {
			s.observers[path] = out
		}
	}
}

// stopObserverLocked cancels an in-flight notification and drops any coalesced
// one. The caller owns the slice bookkeeping.
func (s *Server) stopObserverLocked(o *observer) {
	if o.cancel != nil {
		o.cancel()
		o.cancel = nil
	}
	o.gen++
	o.inflight = false
	o.pending = nil
}

// removeObserverLocked removes o from the observer list of its path, cancelling
// its notification exchange. The caller must hold s.obsMu.
func (s *Server) removeObserverLocked(o *observer) {
	list := s.observers[o.path]
	out := list[:0]
	for _, cur := range list {
		if cur != o {
			out = append(out, cur)
		}
	}
	if len(out) == 0 {
		delete(s.observers, o.path)
	} else {
		s.observers[o.path] = out
	}
	s.stopObserverLocked(o)
}

// Notify sends a notification to every observer of path. It returns the number
// of notifications written.
//
// With ConfirmableNotifications a notification is counted when it is handed to
// the retransmission exchange. One coalesced behind a notification still
// awaiting its ACK is not counted until it is actually sent, so the return value
// can be lower than the number of observers.
func (s *Server) Notify(path string, resp Response) int {
	if resp.Code == 0 {
		resp.Code = coap.CodeContent
	}
	base := s.baseContext()

	type target struct {
		o   *observer
		msg coap.Message
	}
	type running struct {
		o      *observer
		msg    coap.Message
		ctx    context.Context
		cancel context.CancelFunc
		gen    uint64
	}
	var direct []target
	var start []running

	s.obsMu.Lock()
	list := append([]*observer(nil), s.observers[path]...)
	for _, o := range list {
		o.seq++
		opts := append([]coap.Option(nil), resp.Options...)
		opts = append(opts, observeOption(o.seq))
		msg := coap.Message{
			Code: resp.Code, Token: []byte(o.token),
			Options: opts, Payload: resp.Payload,
		}
		if s.confirmableNotifications {
			msg.Type = coap.Confirmable
			msg.MessageID = uint16(s.nextID.Add(1))
			if o.inflight && MaxNotificationsInFlight > 0 {
				// Coalesce to the newest state. Replacing (rather than
				// appending) keeps this bounded no matter how often Notify runs.
				o.pending = &msg
				continue
			}
			o.inflight = true
			o.gen++
			ctx, cancel := context.WithCancel(base)
			o.cancel = cancel
			start = append(start, running{o: o, msg: msg, ctx: ctx, cancel: cancel, gen: o.gen})
			continue
		}
		msg.Type = coap.NonConfirmable
		msg.MessageID = uint16(s.nextID.Add(1))
		direct = append(direct, target{o: o, msg: msg})
	}
	s.obsMu.Unlock()

	sent := 0
	for _, t := range direct {
		if sess := s.sessionByID(t.o.sessionID); sess != nil {
			if err := sess.Send(t.msg); err == nil {
				sent++
			}
		}
	}
	for _, t := range start {
		go s.notificationLoop(t.o, t.msg, t.ctx, t.cancel, t.gen)
		sent++
	}
	return sent
}

// notificationLoop transmits one confirmable notification, follows its
// retransmissions and, after an ACK, sends the newest coalesced notification.
// RST or the final timeout removes the observation, as RFC 7641 section 4.5
// requires for a client that is no longer interested.
func (s *Server) notificationLoop(o *observer, first coap.Message, ctx context.Context, cancel context.CancelFunc, gen uint64) {
	defer cancel()
	msg := first
	for {
		if ctx.Err() != nil {
			s.finishNotification(o, gen)
			return
		}
		sess := s.sessionByID(o.sessionID)
		result := outboundSendFailed
		if sess != nil {
			result = s.sendConfirmable(ctx, sess, msg)
		}

		s.obsMu.Lock()
		if o.gen != gen {
			// The observation was cancelled or replaced while the exchange ran.
			s.obsMu.Unlock()
			return
		}
		switch result {
		case outboundAcked:
			if o.pending == nil {
				o.inflight = false
				o.cancel = nil
				s.obsMu.Unlock()
				return
			}
			msg = *o.pending
			o.pending = nil
			s.obsMu.Unlock()
		case outboundCancelled:
			s.removeObserverLocked(o)
			s.obsMu.Unlock()
			return
		default:
			s.removeObserverLocked(o)
			s.obsMu.Unlock()
			return
		}
	}
}

// finishNotification handles the loop exit before any message was sent.
func (s *Server) finishNotification(o *observer, gen uint64) {
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	if o.gen == gen {
		s.removeObserverLocked(o)
	}
}

func (s *Server) sessionByID(id string) *gnety.Session {
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	return s.sessions[id]
}

// trackSession remembers sessions so notifications can find them.
func (s *Server) trackSession(sess *gnety.Session) {
	if sess == nil {
		return
	}
	s.obsMu.Lock()
	if s.sessions == nil {
		s.sessions = make(map[string]*gnety.Session)
	}
	s.sessions[sess.ID] = sess
	s.obsMu.Unlock()
}

func (s *Server) untrackSession(sess *gnety.Session) {
	if sess == nil {
		return
	}
	s.obsMu.Lock()
	delete(s.sessions, sess.ID)
	s.obsMu.Unlock()
	s.removeSessionObservers(sess.ID)
	s.cancelSessionHandlers(sess.ID)
}
