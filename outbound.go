package gcoap

import (
	"context"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

// This file holds the server-side exchange registry for outbound confirmable
// messages: Observe notifications (RFC 7641) and separate responses (RFC 7252
// section 5.2). Both use the exchangeParams retransmission schedule from
// exchange.go; this file only correlates the ACK/RST the peer sends back.

// outboundResult is how an outbound confirmable exchange ended.
type outboundResult uint8

const (
	outboundAcked      outboundResult = iota // empty ACK from the peer
	outboundReset                            // RST from the peer
	outboundTimeout                          // retransmission limit exhausted
	outboundCancelled                        // server stop or observation cancel
	outboundSendFailed                       // the datagram could not be written
)

// outboundKey identifies one outbound confirmable message. Message ids are only
// unique per peer, so the peer identity is part of the key, as in dedup.
type outboundKey struct {
	peer      string
	messageID uint16
}

// outboundExchange is one registered confirmable message awaiting an ACK/RST.
// result is buffered so the read loop never blocks delivering the answer.
type outboundExchange struct {
	result chan outboundResult
}

// registerOutbound reserves key. ok is false when another exchange already uses
// the same peer + message id, which can only happen after the 16-bit id space
// wrapped while the old exchange is still alive.
func (s *Server) registerOutbound(key outboundKey) (ex *outboundExchange, ok bool) {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	if s.outbound == nil {
		s.outbound = make(map[outboundKey]*outboundExchange)
	}
	if _, exists := s.outbound[key]; exists {
		return nil, false
	}
	ex = &outboundExchange{result: make(chan outboundResult, 1)}
	s.outbound[key] = ex
	return ex, true
}

func (s *Server) unregisterOutbound(key outboundKey, ex *outboundExchange) {
	s.outMu.Lock()
	if s.outbound[key] == ex {
		delete(s.outbound, key)
	}
	s.outMu.Unlock()
}

// dispatchControl routes an ACK or RST to the outbound exchange that owns its
// message id. An ACK/RST never reaches the request path, so an empty
// acknowledgement of a notification cannot be mistaken for a request handler.
func (s *Server) dispatchControl(sess *gnety.Session, m coap.Message) {
	if sess == nil {
		return
	}
	if m.Type != coap.Acknowledgement && m.Type != coap.Reset {
		return
	}
	key := outboundKey{peer: peerIdentity(sess), messageID: m.MessageID}
	s.outMu.Lock()
	ex := s.outbound[key]
	s.outMu.Unlock()
	if ex == nil {
		return
	}
	result := outboundAcked
	if m.Type == coap.Reset {
		result = outboundReset
	}
	select {
	case ex.result <- result:
	default: // a duplicate ACK/RST is already queued; the first one wins
	}
}

// outboundPending reports how many confirmable exchanges are registered. It is
// only used by tests to assert that an exchange stopped.
func (s *Server) outboundPending() int {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	return len(s.outbound)
}

// sendConfirmable transmits msg and retransmits it with the RFC 7252 schedule
// until the peer ACKs or resets it, the exchange lifetime ends or ctx is
// cancelled. Every transmission is the identical datagram (same message id and
// token), as RFC 7252 section 4.2 requires.
func (s *Server) sendConfirmable(ctx context.Context, sess *gnety.Session, msg coap.Message) outboundResult {
	if sess == nil {
		return outboundSendFailed
	}
	raw, err := msg.Marshal()
	if err != nil {
		return outboundSendFailed
	}
	key := outboundKey{peer: peerIdentity(sess), messageID: msg.MessageID}
	ex, ok := s.registerOutbound(key)
	if !ok {
		return outboundSendFailed
	}
	defer s.unregisterOutbound(key, ex)

	schedule := s.params.newRetransmissionSchedule()
	for {
		if ctx.Err() != nil {
			return outboundCancelled
		}
		if err := sess.Send(raw); err != nil {
			return outboundSendFailed
		}
		wait, retransmit := schedule.next()
		if wait <= 0 {
			select {
			case result := <-ex.result:
				return result
			default:
				return outboundTimeout
			}
		}
		timer := s.params.after(wait)
		select {
		case <-ctx.Done():
			return outboundCancelled
		case result := <-ex.result:
			return result
		case <-timer:
			if !retransmit {
				// The final response window ended. An ACK may still have
				// arrived just before the timer; prefer it over a timeout.
				select {
				case result := <-ex.result:
					return result
				default:
					return outboundTimeout
				}
			}
		}
	}
}
