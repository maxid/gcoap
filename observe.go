package gcoap

import (
	"github.com/maxid/gnety"

	"github.com/maxid/gnety/codec/coap"
)

type observer struct {
	sessionID string
	token     string
	path      string
	seq       uint32
}

// observeValue extracts the Observe option. ok=false means no observe option.
func observeValue(m coap.Message) (value int, ok bool) {
	for _, opt := range m.Options {
		if opt.Number != coap.OptionObserve {
			continue
		}
		if len(opt.Value) == 0 {
			return 0, true
		}
		v := 0
		for _, b := range opt.Value {
			v = v<<8 | int(b)
		}
		return v, true
	}
	return 0, false
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
			}
		}
		if len(out) == 0 {
			delete(s.observers, path)
		} else {
			s.observers[path] = out
		}
	}
}

// Notify sends a notification to every observer of path. It returns the
// number of notifications written.
func (s *Server) Notify(path string, resp Response) int {
	s.obsMu.Lock()
	list := append([]*observer(nil), s.observers[path]...)
	for _, o := range list {
		o.seq++
	}
	s.obsMu.Unlock()

	if resp.Code == 0 {
		resp.Code = coap.CodeContent
	}
	type target struct {
		o   *observer
		msg coap.Message
	}
	targets := make([]target, 0, len(list))
	for _, o := range list {
		opts := append([]coap.Option(nil), resp.Options...)
		seq := o.seq
		opts = append(opts, coap.Option{
			Number: coap.OptionObserve,
			Value:  []byte{byte(seq >> 16), byte(seq >> 8), byte(seq)},
		})
		targets = append(targets, target{o: o, msg: coap.Message{
			Type: coap.NonConfirmable, Code: resp.Code,
			MessageID: uint16(s.nextID.Add(1)),
			Token:     []byte(o.token),
			Options:   opts,
			Payload:   resp.Payload,
		}})
	}
	sent := 0
	for _, t := range targets {
		if sess := s.sessionByID(t.o.sessionID); sess != nil {
			if err := sess.Send(t.msg); err == nil {
				sent++
			}
		}
	}
	return sent
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
}
