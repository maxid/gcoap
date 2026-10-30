package gcoap

import (
	"context"
	"errors"
	"net/url"
	"sync"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

// ErrResponseAlreadySent is returned by a Responder when the request exchange
// already produced a response (for example because the handler returned one or
// because the responder was called twice).
var ErrResponseAlreadySent = errors.New("gcoap: response already sent")

// ErrExchangeClosed is returned when a separate response can no longer be sent
// because the server stopped or the peer session closed.
var ErrExchangeClosed = errors.New("gcoap: exchange closed")

// Responder sends the response of a request whose handler asked to answer
// later. It is safe to call from another goroutine and at most one call wins.
// For a confirmable request the retransmission runs in the background, so the
// responder can be called from the read loop without blocking it; the returned
// error only reports an exchange that is already answered or closed.
type Responder func(Response) error

// requestExchange is one inbound request. It owns the decision between a
// piggybacked response and a separate response, the empty-ACK timer and the
// handler context lifetime. Fields are guarded by mu unless noted otherwise.
type requestExchange struct {
	srv         *Server
	sess        *gnety.Session
	msg         coap.Message // immutable after planRequest (payload may grow)
	key         exchangeKey
	confirmable bool
	plan        requestPlan // set before the handler runs; immutable afterwards

	mu          sync.Mutex
	ackSent     bool
	finished    bool
	deferred    bool
	timerCancel context.CancelFunc
	handlerDone func()
}

// requestPlan is the result of the request processing that happens before the
// handler: Block1 reassembly, route lookup and Observe registration.
type requestPlan struct {
	path            string
	query           url.Values
	finalBlock1     *coap.Option
	early           *Response
	handler         Handler
	observeRegister bool
}

// answerRequest runs one request to completion. With SeparateResponses the
// caller starts it on its own goroutine, so the transport read loop stays free
// while a slow handler works.
func (s *Server) answerRequest(st *requestExchange) {
	st.plan = s.planRequest(st)
	if st.plan.early != nil {
		st.deliver(*st.plan.early)
		return
	}
	resp := Response{Code: coap.CodeNotFound}
	if st.plan.handler != nil {
		if st.confirmable {
			st.startAckTimer()
		}
		ctx, done := s.handlerContext(st.sess)
		st.mu.Lock()
		st.handlerDone = done
		st.mu.Unlock()
		resp = st.plan.handler(ctx, Request{
			Message: st.msg, Path: st.plan.path, Query: st.plan.query,
			Payload: append([]byte(nil), st.msg.Payload...), Session: st.sess,
			exchange: st,
		})
	}
	st.handlerReturned(resp)
}

// planRequest performs the pre-handler work that used to live in respond:
// Block1 reassembly, route matching and Observe (de)registration.
func (s *Server) planRequest(st *requestExchange) requestPlan {
	m := st.msg
	path, query := parsePath(m)
	plan := requestPlan{path: path, query: query}
	if m.Code == coap.CodePUT {
		if b, ok, err := requestBlock(m, coap.OptionBlock1); err != nil {
			plan.early = &Response{Code: coap.CodeBadRequest}
			return plan
		} else if ok {
			full, final, err := s.processBlock1(st.sess, m, b)
			if err != nil {
				plan.early = &Response{Code: coap.CodeBadRequest}
				return plan
			}
			option, err := blockOption(coap.OptionBlock1, final)
			if err != nil {
				plan.early = &Response{Code: coap.CodeBadRequest}
				return plan
			}
			if b.More {
				plan.early = &Response{Code: coap.CodeContinue, Options: []coap.Option{option}}
				return plan
			}
			st.msg.Payload = full
			plan.finalBlock1 = &option
		}
	}
	h := s.match(m.Code, path)
	if h != nil && m.Code == coap.CodeGET {
		if obs, hasObs := m.Observe(); hasObs {
			if obs == coap.ObserveDeregister {
				s.cancelObserver(st.sess, m.Token, path)
			} else {
				s.addObserver(st.sess, m.Token, path)
				plan.observeRegister = true
			}
		}
	}
	plan.handler = h
	return plan
}

// handlerReturned handles the value a handler returned. A handler that used
// Request.RespondLater may either return a zero Response and answer from
// another goroutine, or return the response itself; both end up in a separate
// response because the empty ACK is already on the wire.
func (st *requestExchange) handlerReturned(resp Response) {
	st.mu.Lock()
	if st.finished {
		done := st.handlerDone
		st.handlerDone = nil
		st.mu.Unlock()
		if done != nil {
			done()
		}
		return
	}
	if st.deferred {
		if resp.Code != 0 || len(resp.Options) > 0 || len(resp.Payload) > 0 {
			st.mu.Unlock()
			_ = st.respondDeferred(resp)
			return
		}
		// The handler is still working from another goroutine. Keep the
		// handler context alive until the responder sends the answer.
		st.mu.Unlock()
		return
	}
	st.mu.Unlock()
	st.deliver(resp)
}

// deferResponse marks the exchange as a separate response, sends the empty ACK
// immediately and returns the responder for the final message.
func (st *requestExchange) deferResponse() Responder {
	st.mu.Lock()
	if st.finished {
		st.mu.Unlock()
		return func(Response) error { return ErrResponseAlreadySent }
	}
	st.deferred = true
	st.mu.Unlock()
	st.sendEmptyAck()
	return st.respondDeferred
}

// respondDeferred hands off the actual response as a separate CON message with
// its own message id (or sends a NON message for a NON request). The CON
// retransmission runs in the background.
func (st *requestExchange) respondDeferred(resp Response) error {
	st.mu.Lock()
	if st.finished {
		st.mu.Unlock()
		return ErrResponseAlreadySent
	}
	st.finished = true
	st.deferred = true
	done := st.handlerDone
	st.handlerDone = nil
	st.stopTimerLocked()
	st.mu.Unlock()
	if done != nil {
		done()
	}
	if !st.confirmable {
		return st.sendSeparate(resp)
	}
	if st.sess.Closed() || st.srv.baseContext().Err() != nil {
		return ErrExchangeClosed
	}
	// A confirmable separate response waits for its ACK. Run that exchange in
	// the background so a handler that answers inline cannot block the read loop
	// that must receive the ACK.
	go func() { _ = st.sendSeparate(resp) }()
	return nil
}

// deliver completes an exchange whose handler returned normally: a piggybacked
// ACK when no empty ACK was sent, otherwise a separate CON response.
func (st *requestExchange) deliver(resp Response) {
	out := st.srv.finalizeResponse(st.msg, st.plan, resp)
	st.mu.Lock()
	if st.finished {
		st.mu.Unlock()
		return
	}
	st.finished = true
	ackSent := st.ackSent
	done := st.handlerDone
	st.handlerDone = nil
	st.stopTimerLocked()
	st.mu.Unlock()
	if done != nil {
		done()
	}
	if ackSent && st.confirmable {
		// The response owns its own retransmission exchange. Keep it off the
		// read loop, which must stay free to receive the client's ACK.
		go func() { _ = st.sendSeparate(out) }()
		return
	}
	outMsg := responseMessage(st.srv.nextID.Add, st.msg, out)
	raw, err := outMsg.Marshal()
	if err == nil {
		err = st.sess.Send(raw)
	}
	if st.confirmable {
		if err != nil {
			st.srv.dedup.forget(st.key)
		} else {
			st.srv.dedup.complete(st.key, raw)
		}
	}
}

// sendSeparate writes a response after the request's empty ACK. A confirmable
// request gets a CON response (retransmitted until ACK/RST); a NON request gets
// a single NON response with a fresh message id.
func (st *requestExchange) sendSeparate(resp Response) error {
	out := st.srv.finalizeResponse(st.msg, st.plan, resp)
	typ := coap.Confirmable
	if !st.confirmable {
		typ = coap.NonConfirmable
	}
	msg := coap.Message{
		Type: typ, Code: out.Code, MessageID: uint16(st.srv.nextID.Add(1)),
		Token: append([]byte(nil), st.msg.Token...), Options: out.Options, Payload: out.Payload,
	}
	if typ == coap.NonConfirmable {
		if st.sess.Closed() {
			return ErrExchangeClosed
		}
		return st.sess.Send(msg)
	}
	switch st.srv.sendConfirmable(st.srv.baseContext(), st.sess, msg) {
	case outboundAcked, outboundTimeout:
		return nil
	case outboundReset:
		return ErrReset
	case outboundCancelled, outboundSendFailed:
		return ErrExchangeClosed
	default:
		return nil
	}
}

// startAckTimer sends an empty ACK if the handler has not answered within
// ACK_TIMEOUT, turning the eventual handler result into a separate exchange.
func (st *requestExchange) startAckTimer() {
	ctx, cancel := context.WithCancel(st.srv.baseContext())
	st.mu.Lock()
	if st.finished || st.ackSent || st.deferred {
		st.mu.Unlock()
		cancel()
		return
	}
	st.timerCancel = cancel
	st.mu.Unlock()
	go func() {
		timer := st.srv.params.after(st.srv.params.ackTimeout)
		select {
		case <-ctx.Done():
		case <-timer:
			st.sendEmptyAck()
		}
	}()
}

// sendEmptyAck writes the RFC 7252 empty ACK and records it in the dedup cache
// as the interim answer for duplicate requests.
func (st *requestExchange) sendEmptyAck() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.finished || st.ackSent || !st.confirmable {
		return false
	}
	ack := coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: st.msg.MessageID}
	raw, err := ack.Marshal()
	if err != nil {
		return false
	}
	// Record the replay bytes before the write, so a duplicate arriving while
	// the write is in progress is answered with the same empty ACK.
	st.ackSent = true
	st.srv.dedup.acknowledge(st.key, raw)
	st.stopTimerLocked()
	if err := st.sess.Send(raw); err != nil {
		st.ackSent = false
		st.srv.dedup.acknowledge(st.key, nil)
		return false
	}
	return true
}

func (st *requestExchange) stopTimerLocked() {
	if st.timerCancel != nil {
		st.timerCancel()
		st.timerCancel = nil
	}
}

// finalizeResponse applies the post-handler rules shared by piggybacked and
// separate responses: default code, Block2 slicing, the final Block1 option
// and the Observe option of the initial registration response.
func (s *Server) finalizeResponse(m coap.Message, plan requestPlan, resp Response) Response {
	if resp.Code == 0 {
		resp.Code = coap.CodeContent
	}
	if m.Code == coap.CodeGET {
		if b, ok, err := requestBlock(m, coap.OptionBlock2); ok {
			var chunk []byte
			var option coap.Option
			if err == nil {
				var outBlock coap.Block
				chunk, outBlock, err = sliceBlock(resp.Payload, b)
				if err == nil {
					option, err = blockOption(coap.OptionBlock2, outBlock)
				}
			}
			if err != nil {
				resp.Code = coap.CodeBadRequest
				resp.Payload = nil
			} else {
				resp.Payload = chunk
				resp.Options = append(resp.Options, option)
			}
		}
	}
	if plan.finalBlock1 != nil {
		resp.Options = append(resp.Options, *plan.finalBlock1)
	}
	if m.Code == coap.CodeGET && plan.observeRegister {
		resp.Options = append(resp.Options, observeOption(coap.ObserveRegister))
	}
	return resp
}
