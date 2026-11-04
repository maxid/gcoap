package gcoap

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

// ServerObservation is an observation a server started against a peer over the
// session that peer already has with it. It is created by Server.ObserveTo and
// stays registered until it is cancelled, the peer ends it with an error
// notification, or the server stops.
//
// The callback runs on the server's transport read loop, in arrival order, and must
// not block: hand the value to a goroutine or a buffered channel if the consumer
// can be slow.
type ServerObservation struct {
	srv   *Server
	sess  *gnety.Session
	req   ClientRequest
	token string
	addr  string

	notify func(ClientResponse)
	first  chan serverResponse
	done   chan struct{}
	once   sync.Once

	mu          sync.Mutex
	established bool
	ended       bool
	seq         coap.Observe
}

// Done returns a channel that is closed when the observation ends.
func (o *ServerObservation) Done() <-chan struct{} { return o.done }

// deliver hands one inbound message to the observation: the first response to the
// goroutine awaiting the registration, later ones to the callback.
func (o *ServerObservation) deliver(resp ClientResponse) {
	o.mu.Lock()
	if o.ended {
		o.mu.Unlock()
		return
	}
	if !o.established {
		o.established = true
		o.mu.Unlock()
		select {
		case o.first <- serverResponse{msg: coap.Message{Code: resp.Code, Options: resp.Options, Payload: resp.Payload}}:
		default:
		}
		return
	}
	// A final error notification ends the observation.
	if resp.Code.Class() != 2 {
		o.mu.Unlock()
		o.notify(resp)
		o.stop()
		return
	}
	o.mu.Unlock()
	o.notify(resp)
}

// stop unregisters the observation and closes Done once.
func (o *ServerObservation) stop() {
	o.once.Do(func() {
		o.mu.Lock()
		o.ended = true
		o.mu.Unlock()
		o.srv.unregisterServerRequest(o.token)
		close(o.done)
	})
}

// Cancel sends the deregistration (Observe=1) over the session and unregisters the
// observation. It reports a transport failure of the deregistration but always
// releases the observation locally.
func (o *ServerObservation) Cancel(ctx context.Context) error {
	o.mu.Lock()
	ended := o.ended
	o.mu.Unlock()
	if ended {
		return nil
	}
	deregister := o.req
	deregister.Options = append([]coap.Option(nil), o.req.Options...)
	value := coap.ObserveDeregister
	msg, err := o.srv.buildServerMessage(o.req, []byte(o.token), nil)
	if err == nil {
		if setErr := msg.SetObserve(value); setErr == nil {
			if sendErr := o.sess.Send(msg); sendErr != nil {
				err = sendErr
			}
		} else {
			err = setErr
		}
	}
	o.stop()
	if err != nil {
		return err
	}
	return nil
}

// ErrObserveRequiresGet is returned when a server-initiated observation is asked
// for something other than a GET.
var ErrObserveRequiresGet = errors.New("gcoap: ObserveTo requires a GET request")

// ObserveTo registers an observation against a peer over the session the peer
// already has with this server: the GET carries Observe=0, and the registration
// response is returned while later notifications go to notify.
//
// RFC 7641 section 3.2 makes the registration response the first notification, so
// it is returned rather than passed to the callback. A rejected registration
// (a non-2.xx answer or a missing Observe option) is reported as
// ErrObserveRejected and leaves nothing registered.
func (s *Server) ObserveTo(ctx context.Context, addr string, req ClientRequest, notify func(ClientResponse)) (*ServerObservation, ClientResponse, error) {
	if notify == nil {
		return nil, ClientResponse{}, errors.New("gcoap: ObserveTo requires a notification callback")
	}
	if req.Code != coap.CodeGET {
		return nil, ClientResponse{}, ErrObserveRequiresGet
	}
	sess := s.sessionByAddr(addr)
	if sess == nil {
		return nil, ClientResponse{}, fmt.Errorf("%w: %s", ErrNoSession, addr)
	}
	token, err := newServerToken()
	if err != nil {
		return nil, ClientResponse{}, err
	}
	req.Options = append([]coap.Option(nil), req.Options...)
	register := coap.ObserveRegister
	msg, err := s.buildServerMessage(req, token, nil)
	if err != nil {
		return nil, ClientResponse{}, err
	}
	if err := msg.SetObserve(register); err != nil {
		return nil, ClientResponse{}, err
	}
	// The registration response and the exchange wait on the same channel: the
	// observation routes its first delivery there and later ones to notify.
	events := make(chan serverResponse, 1)
	obs := &ServerObservation{
		srv: s, sess: sess, req: req, token: string(token), addr: addr,
		notify: notify, first: events, done: make(chan struct{}),
	}
	out := &serverRequest{peer: peerIdentity(sess), token: obs.token, mid: msg.MessageID, events: events, obs: obs}
	if !s.registerServerRequest(out) {
		return nil, ClientResponse{}, ErrTooManyServerRequests
	}
	tCtx, cancel := context.WithTimeout(ctx, s.params.exchangeLifetime)
	defer cancel()
	first, err := s.exchangeServerRequest(tCtx, sess, msg, out)
	if err != nil {
		s.unregisterServerRequest(out.token)
		return nil, ClientResponse{}, err
	}
	if first.Code.Class() != 2 {
		s.unregisterServerRequest(out.token)
		return nil, ClientResponse{Code: first.Code, Options: first.Options, Payload: first.Payload}, fmt.Errorf("%w: %s", ErrObserveRejected, first.Code)
	}
	if _, ok := first.Observe(); !ok {
		s.unregisterServerRequest(out.token)
		return nil, ClientResponse{Code: first.Code, Options: first.Options, Payload: first.Payload}, ErrObserveRejected
	}
	return obs, ClientResponse{Code: first.Code, Options: first.Options, Payload: first.Payload}, nil
}
