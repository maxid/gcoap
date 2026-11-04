package gcoap

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

// ErrNoSession is returned when a server-initiated request names an address no
// live session belongs to.
var ErrNoSession = errors.New("gcoap: no session for that address")

// ErrTooManyServerRequests is returned when the server already has the maximum
// number of server-initiated requests in flight.
var ErrTooManyServerRequests = errors.New("gcoap: too many server-initiated requests in flight")

// maxServerRequests bounds the requests a server keeps in flight at one time. It
// matches the deduplication budget: a peer that can hold 1024 inbound exchanges
// must not be able to hold more than that on the way out.
const maxServerRequests = 1024

// serverRequest is one request the server sent over an existing session. Inbound
// datagrams are matched to it by token, and the acknowledgement of its own message
// id by peer and id.
type serverRequest struct {
	peer  string
	token string
	mid   uint16

	// events receives the first response of the exchange (or a reset); a
	// concurrent reader consumes exactly one value.
	events chan serverResponse
}

type serverResponse struct {
	msg coap.Message
	err error
	// acked reports an empty acknowledgement: the peer will answer separately, so
	// the request stops retransmitting but keeps waiting.
	acked bool
}

// sessionByAddr returns the session whose peer address or id is addr.
func (s *Server) sessionByAddr(addr string) *gnety.Session {
	s.obsMu.Lock()
	defer s.obsMu.Unlock()
	for _, sess := range s.sessions {
		if sess == nil {
			continue
		}
		if sess.ID == addr {
			return sess
		}
		if sess.RemoteAddr != nil && sess.RemoteAddr.String() == addr {
			return sess
		}
	}
	return nil
}

// registerServerRequest adds one outbound request to the routing table. It reports
// false when the bound is already reached.
func (s *Server) registerServerRequest(req *serverRequest) bool {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	if s.requests == nil {
		s.requests = make(map[string]*serverRequest)
	}
	if len(s.requests) >= maxServerRequests {
		return false
	}
	s.requests[req.token] = req
	return true
}

func (s *Server) unregisterServerRequest(token string) {
	s.reqMu.Lock()
	delete(s.requests, token)
	s.reqMu.Unlock()
}

// serverRequestByToken returns the pending request a response belongs to.
func (s *Server) serverRequestByToken(peer, token string) *serverRequest {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	req := s.requests[token]
	if req == nil || req.peer != peer {
		return nil
	}
	return req
}

// deliverServerResponse hands one inbound datagram to the server request that owns
// it. It reports whether a request claimed the datagram.
func (s *Server) deliverServerResponse(sess *gnety.Session, m coap.Message) bool {
	req := s.serverRequestByToken(peerIdentity(sess), string(m.Token))
	if req == nil {
		return false
	}
	select {
	case req.events <- serverResponse{msg: m}:
	default: // a duplicate response is already queued; the first one wins
	}
	return true
}

// deliverServerAck records an empty acknowledgement of a server-initiated
// request, so the exchange stops retransmitting and waits for the separate
// response.
func (s *Server) deliverServerAck(sess *gnety.Session, m coap.Message) bool {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	for _, req := range s.requests {
		if req.peer != peerIdentity(sess) || req.mid != m.MessageID {
			continue
		}
		select {
		case req.events <- serverResponse{acked: true}:
		default:
		}
		return true
	}
	return false
}

// deliverServerReset ends a server request whose message the peer rejected.
func (s *Server) deliverServerReset(sess *gnety.Session, m coap.Message) bool {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	for _, req := range s.requests {
		if req.peer != peerIdentity(sess) || req.mid != m.MessageID {
			continue
		}
		select {
		case req.events <- serverResponse{err: ErrReset}:
		default:
		}
		return true
	}
	return false
}

// DoTo sends one CoAP request over the session a peer already has with this
// server and returns its response. It is what a server-initiated operation needs:
// the request leaves from the server's own socket, which is the address a real
// client accepts requests from, and the response is correlated by token.
//
// A block-wise response is assembled; a request body larger than one block is
// rejected with ErrRequestTooLarge, because Block1 from the server side is not
// implemented yet.
func (s *Server) DoTo(ctx context.Context, addr string, req ClientRequest) (ClientResponse, error) {
	if len(req.Payload) > DefaultClientBlockSize {
		return ClientResponse{}, ErrRequestTooLarge
	}
	sess := s.sessionByAddr(addr)
	if sess == nil {
		return ClientResponse{}, fmt.Errorf("%w: %s", ErrNoSession, addr)
	}
	token, err := newServerToken()
	if err != nil {
		return ClientResponse{}, err
	}
	msg, err := s.buildServerMessage(req, token, nil)
	if err != nil {
		return ClientResponse{}, err
	}
	out := &serverRequest{peer: peerIdentity(sess), token: string(token), mid: msg.MessageID, events: make(chan serverResponse, 1)}
	if !s.registerServerRequest(out) {
		return ClientResponse{}, ErrTooManyServerRequests
	}
	defer s.unregisterServerRequest(out.token)

	resp, err := s.exchangeServerRequest(ctx, sess, msg, out)
	if err != nil {
		return ClientResponse{}, err
	}
	return s.collectServerResponse(ctx, sess, req, token, resp)
}

// exchangeServerRequest sends one message and follows the RFC 7252
// retransmission schedule until a response, a reset, the exchange lifetime or the
// context ends.
func (s *Server) exchangeServerRequest(ctx context.Context, sess *gnety.Session, msg coap.Message, out *serverRequest) (coap.Message, error) {
	raw, err := msg.Marshal()
	if err != nil {
		return coap.Message{}, err
	}
	schedule := s.params.newRetransmissionSchedule()
	sent := false
	for {
		if err := ctx.Err(); err != nil {
			return coap.Message{}, err
		}
		if sent {
			s.recordRetransmit(RetransmitRequest)
		}
		if err := sess.Send(raw); err != nil {
			return coap.Message{}, err
		}
		sent = true
		wait, retransmit := schedule.next()
		if wait <= 0 {
			return coap.Message{}, context.DeadlineExceeded
		}
		timer := s.params.after(wait)
		select {
		case <-ctx.Done():
			return coap.Message{}, ctx.Err()
		case <-s.baseContext().Done():
			return coap.Message{}, ErrExchangeClosed
		case <-timer:
			if !retransmit {
				return coap.Message{}, context.DeadlineExceeded
			}
		case ev := <-out.events:
			if ev.acked {
				// The peer owns the exchange: no more retransmissions, just wait
				// for the separate response.
				return s.awaitServerResponse(ctx, sess, out, schedule.remaining())
			}
			return s.settledServerResponse(sess, ev)
		}
	}
}

// awaitServerResponse waits for the separate response after an empty
// acknowledgement.
func (s *Server) awaitServerResponse(ctx context.Context, sess *gnety.Session, out *serverRequest, wait time.Duration) (coap.Message, error) {
	if wait <= 0 {
		return coap.Message{}, context.DeadlineExceeded
	}
	timer := s.params.after(wait)
	select {
	case <-ctx.Done():
		return coap.Message{}, ctx.Err()
	case <-s.baseContext().Done():
		return coap.Message{}, ErrExchangeClosed
	case <-timer:
		return coap.Message{}, context.DeadlineExceeded
	case ev := <-out.events:
		return s.settledServerResponse(sess, ev)
	}
}

// settledServerResponse turns one event into the exchange outcome and
// acknowledges a separate confirmable response.
func (s *Server) settledServerResponse(sess *gnety.Session, ev serverResponse) (coap.Message, error) {
	if ev.err != nil {
		return coap.Message{}, ev.err
	}
	if ev.msg.Type == coap.Confirmable {
		ack := coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: ev.msg.MessageID}
		_ = sess.Send(ack)
	}
	return ev.msg, nil
}

// collectServerResponse follows Block2 response blocks until the body is complete.
func (s *Server) collectServerResponse(ctx context.Context, sess *gnety.Session, req ClientRequest, token []byte, resp coap.Message) (ClientResponse, error) {
	out := ClientResponse{Code: resp.Code, Options: resp.Options, Payload: append([]byte(nil), resp.Payload...)}

	block, ok := resp.Block2()
	more := ok && block.More
	if !more {
		return out, nil
	}
	if len(out.Payload) > DefaultMaxResponseSize {
		return ClientResponse{}, fmt.Errorf("%w: %d of %d bytes", ErrResponseTooLarge, len(out.Payload), DefaultMaxResponseSize)
	}
	for more {
		block.Num++
		msg, err := s.buildServerMessage(req, token, &block)
		if err != nil {
			return ClientResponse{}, err
		}
		next := &serverRequest{peer: peerIdentity(sess), token: string(token), mid: msg.MessageID, events: make(chan serverResponse, 1)}
		if !s.registerServerRequest(next) {
			return ClientResponse{}, ErrTooManyServerRequests
		}
		partial, err := s.exchangeServerRequest(ctx, sess, msg, next)
		s.unregisterServerRequest(next.token)
		if err != nil {
			return ClientResponse{}, err
		}
		if partial.Code != resp.Code {
			return ClientResponse{}, fmt.Errorf("%w: block %d answered %s", ErrBlockSequence, block.Num, partial.Code)
		}
		if len(out.Payload)+len(partial.Payload) > DefaultMaxResponseSize {
			return ClientResponse{}, fmt.Errorf("%w: more than %d bytes", ErrResponseTooLarge, DefaultMaxResponseSize)
		}
		out.Payload = append(out.Payload, partial.Payload...)
		block, ok = partial.Block2()
		more = ok && block.More
	}
	return out, nil
}

// buildServerMessage assembles one request datagram for a session.
func (s *Server) buildServerMessage(req ClientRequest, token []byte, block2 *coap.Block) (coap.Message, error) {
	m := coap.Message{
		Type:      coap.Confirmable,
		Code:      req.Code,
		MessageID: uint16(s.nextID.Add(1)),
		Token:     append([]byte(nil), token...),
		Options:   append([]coap.Option(nil), req.Options...),
		Payload:   req.Payload,
	}
	if block2 != nil {
		m.Payload = nil
	}
	segments := m.URIPath()
	if req.Path != "" && req.Path != "/" {
		segments = append(segments, strings.Split(strings.Trim(req.Path, "/"), "/")...)
	}
	if len(segments) > 0 {
		if err := m.SetURIPath(segments...); err != nil {
			return coap.Message{}, err
		}
	}
	query := m.URIQuery()
	for key, values := range req.Query {
		for _, v := range values {
			query = append(query, key+"="+v)
		}
	}
	if len(query) > 0 {
		if err := m.SetURIQuery(query...); err != nil {
			return coap.Message{}, err
		}
	}
	if block2 != nil {
		if err := m.SetBlock2(*block2); err != nil {
			return coap.Message{}, err
		}
	}
	return m, nil
}

// newServerToken returns a fresh request token.
func newServerToken() ([]byte, error) {
	token := make([]byte, 4)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	return token, nil
}
