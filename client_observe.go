package gcoap

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/maxid/gnety/codec/coap"
)

// ErrObserveRejected is returned when a server answers an Observe registration
// with an error or without the Observe option, i.e. it does not support the
// observation (RFC 7641 section 3.1).
var ErrObserveRejected = errors.New("gcoap: observe registration rejected")

// Observation is one client-side CoAP observation (RFC 7641). It is created by
// Client.Observe and remains registered until it is cancelled, its context is
// cancelled, the client is closed or the server ends it with an error
// notification. Notifications are delivered in order to the callback passed to
// Client.Observe.
type Observation struct {
	cli    *Client
	token  []byte
	req    ClientRequest
	notify func(ClientResponse)

	done      chan struct{} // closed when the observation ends
	stopped   chan struct{} // closed by stopLocal, releases the context watcher
	watchDone chan struct{} // closed when the context watcher has exited
	stopOnce  sync.Once

	mu          sync.Mutex
	seq         coap.Observe
	established bool
	ended       bool
}

// Observe registers a client-side observation: it sends a GET with Observe=0 and
// returns the registration response together with the handle that receives the
// following notifications. RFC 7641 section 3.2 makes the registration response
// the first notification, so it is returned here rather than passed to the
// callback; later notifications are handed to notify. When the server rejects the
// registration, the response is returned together with ErrObserveRejected and no
// Observation is created.
//
// notify runs serially on the client's receive goroutine, in the order the
// notifications arrive. It must not block and must not call back into the
// client (a blocking call would stall every exchange of the client). Hand the
// notification to a goroutine or a buffered channel if the consumer can be slow.
//
// The observation lives as long as ctx: cancelling ctx stops delivery and sends a
// best-effort non-confirmable deregistration. Observation.Cancel deregisters
// explicitly and waits for the server. A per-observation sequence number is
// tracked (RFC 7641 section 3.4), so a stale notification is acknowledged if it
// is confirmable but not delivered. Neither the registration response nor a
// notification is followed with Block2; block-wise bodies are assembled by Do
// only.
func (c *Client) Observe(ctx context.Context, req ClientRequest, notify func(ClientResponse)) (*Observation, ClientResponse, error) {
	if notify == nil {
		return nil, ClientResponse{}, errors.New("gcoap: Observe requires a notification callback")
	}
	if req.Code != coap.CodeGET {
		return nil, ClientResponse{}, errors.New("gcoap: Observe requires a GET request")
	}
	if len(req.Payload) > 0 {
		return nil, ClientResponse{}, errors.New("gcoap: Observe does not support a request body")
	}
	if ctx == nil {
		return nil, ClientResponse{}, errors.New("gcoap: Observe requires a context")
	}
	if err := ctx.Err(); err != nil {
		return nil, ClientResponse{}, err
	}
	token := c.newToken()
	register := coap.ObserveRegister
	msg, err := c.buildMessage(req, token, messageOptions{payload: []byte{}, observe: &register})
	if err != nil {
		return nil, ClientResponse{}, err
	}
	o := &Observation{
		cli:   c,
		token: append([]byte(nil), token...),
		req: ClientRequest{
			Code: req.Code, Path: req.Path, Query: req.Query,
			Options: append([]coap.Option(nil), req.Options...),
		},
		notify:    notify,
		done:      make(chan struct{}),
		stopped:   make(chan struct{}),
		watchDone: make(chan struct{}),
	}
	// Reserve the token before the request goes out, so a concurrent request
	// cannot pick it while the registration response is awaited.
	c.exMu.Lock()
	if c.baseCtx.Err() != nil {
		c.exMu.Unlock()
		return nil, ClientResponse{}, ErrClientClosed
	}
	c.observes[string(token)] = o
	c.exMu.Unlock()
	go o.watch(ctx)

	resp, err := c.exchange(ctx, &msg, false)
	if err != nil {
		o.stopLocal(false)
		return nil, ClientResponse{}, err
	}
	if resp.Code.Class() != 2 {
		o.stopLocal(false)
		return nil, clientResponseOf(resp), fmt.Errorf("%w: %s", ErrObserveRejected, resp.Code)
	}
	seq, ok := resp.Observe()
	if !ok {
		o.stopLocal(false)
		return nil, clientResponseOf(resp), ErrObserveRejected
	}
	o.mu.Lock()
	o.seq = seq
	o.established = true
	o.mu.Unlock()
	return o, clientResponseOf(resp), nil
}

// Done returns a channel that is closed when the observation ends, whether by
// Cancel, by its context, by Client.Close or because the server sent an error
// notification.
func (o *Observation) Done() <-chan struct{} {
	if o == nil {
		return nil
	}
	return o.done
}

// Cancel stops the observation and deregisters it from the server with a
// confirmable GET Observe=1 request, waiting for the server's answer within ctx.
// Delivery stops before Cancel returns; the registration is freed even when the
// deregistration request fails, so the returned error reports only that the
// server may still believe the observation is alive.
//
// Cancel is idempotent: a second call, or a call after the observation ended on
// its own, returns nil without sending anything. It must not be called from the
// notification callback.
func (o *Observation) Cancel(ctx context.Context) error {
	if o == nil || o.cli == nil {
		return nil
	}
	if !o.stopLocal(false) {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return o.sendDeregister(ctx, true)
}

// watch releases the observation when its context is cancelled. It is the only
// goroutine an observation owns; stopLocal closes stopped so it exits on every
// other path.
func (o *Observation) watch(ctx context.Context) {
	go func() {
		defer close(o.watchDone)
		select {
		case <-ctx.Done():
			// A non-confirmable deregistration leaves no exchange behind, so
			// cancelling a context never blocks.
			o.stopLocal(true)
		case <-o.stopped:
		}
	}()
}

// stopLocal ends the observation locally and reports whether this call was the
// one that ended it. It closes Done, removes the registration so no further
// notification is delivered, and optionally tells the server with a single
// non-confirmable deregistration.
func (o *Observation) stopLocal(sendDeregister bool) bool {
	first := false
	o.stopOnce.Do(func() {
		first = true
		close(o.stopped)
	})
	if !first {
		return false
	}
	o.cli.removeObservation(o)
	close(o.done)
	if sendDeregister {
		_ = o.sendDeregister(context.Background(), false)
	}
	return true
}

// removeObservation drops the registration from the client and marks the
// observation as ended. Marking ended under mu waits for a callback that is
// already running, so no notification is delivered after this returns.
func (c *Client) removeObservation(o *Observation) {
	c.exMu.Lock()
	if c.observes[string(o.token)] == o {
		delete(c.observes, string(o.token))
	}
	c.exMu.Unlock()
	o.mu.Lock()
	o.ended = true
	o.mu.Unlock()
}

// stopAllObservations ends every observation of a closing client and waits for
// their watcher goroutines. It does not send a deregistration, because the
// transport is going away.
func (c *Client) stopAllObservations() {
	c.exMu.Lock()
	list := make([]*Observation, 0, len(c.observes))
	for _, o := range c.observes {
		list = append(list, o)
	}
	c.observes = make(map[string]*Observation)
	c.exMu.Unlock()
	for _, o := range list {
		o.stopLocal(false)
	}
	for _, o := range list {
		<-o.watchDone
	}
}

// sendDeregister sends the GET Observe=1 request of the observation. A
// confirmable request runs as a normal exchange; a non-confirmable one is a
// single datagram with no state to clean up.
func (o *Observation) sendDeregister(ctx context.Context, confirmable bool) error {
	deregister := coap.ObserveDeregister
	msg, err := o.cli.buildMessage(o.req, o.token, messageOptions{
		payload: []byte{}, observe: &deregister,
	})
	if err != nil {
		return err
	}
	if !confirmable {
		msg.Type = coap.NonConfirmable
		return o.cli.conn.Send(msg)
	}
	_, err = o.cli.exchange(ctx, &msg, true)
	return err
}

// deliver applies the RFC 7641 rules to one notification: a stale sequence
// number is dropped, an error response ends the observation, and a fresh
// notification is handed to the callback. The callback runs while mu is held, so
// Cancel (which takes mu) cannot return while a delivery is still in progress.
func (o *Observation) deliver(m coap.Message) {
	if !m.Code.IsResponse() {
		return
	}
	o.mu.Lock()
	if !o.established || o.ended {
		o.mu.Unlock()
		return
	}
	final := m.Code.Class() == 4 || m.Code.Class() == 5
	deliver := final
	if !final {
		if seq, ok := m.Observe(); ok && observeFresh(o.seq, seq) {
			o.seq = seq
			deliver = true
		}
	}
	if deliver {
		o.notify(clientResponseOf(m))
	}
	o.mu.Unlock()
	if final {
		// A server that cannot continue the observation answers with an error
		// notification (RFC 7641 section 3.2); it still made it to the callback,
		// but there is nothing left to observe.
		o.stopLocal(false)
	}
}

// observeFresh reports whether next is a newer notification than prev under the
// RFC 7641 section 3.4 rule: the 24-bit sequence numbers are compared with RFC
// 1982 serial number arithmetic, so a value is newer when it lies ahead of the
// stored one by less than half the value space. Moving from the largest value
// back to zero is therefore fresh, and an old value that reappears after the
// window moved on is stale.
func observeFresh(prev, next coap.Observe) bool {
	diff := (uint32(next) - uint32(prev)) & 0xFFFFFF
	return diff != 0 && diff < 1<<23
}

// clientResponseOf converts a decoded message to the client response type.
func clientResponseOf(m coap.Message) ClientResponse {
	return ClientResponse{Code: m.Code, Options: m.Options, Payload: m.Payload}
}
