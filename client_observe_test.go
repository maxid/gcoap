package gcoap

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// peerDatagram is one datagram a scripted peer received, with the client address
// to answer.
type peerDatagram struct {
	msg  coap.Message
	addr *net.UDPAddr
}

// scriptPeer is a bare UDP socket standing in for a CoAP server that the test
// drives one datagram at a time. It never answers on its own, so a test can
// assert the exact order of the client's requests and notifications.
type scriptPeer struct {
	conn   *net.UDPConn
	recv   chan peerDatagram
	closed chan struct{}
	wg     sync.WaitGroup
}

func newScriptPeer(t *testing.T) *scriptPeer {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	p := &scriptPeer{conn: conn, recv: make(chan peerDatagram, 32), closed: make(chan struct{})}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		buf := make([]byte, 2048)
		for {
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			msg, err := coap.Parse(append([]byte(nil), buf[:n]...))
			if err != nil {
				continue
			}
			select {
			case p.recv <- peerDatagram{msg: msg, addr: addr}:
			case <-p.closed:
				return
			}
		}
	}()
	t.Cleanup(func() {
		close(p.closed)
		_ = conn.Close()
		p.wg.Wait()
	})
	return p
}

func (p *scriptPeer) Addr() string { return p.conn.LocalAddr().String() }

// next returns the next datagram the client sent.
func (p *scriptPeer) next(t *testing.T) peerDatagram {
	t.Helper()
	select {
	case d := <-p.recv:
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a client datagram")
		return peerDatagram{}
	}
}

func (p *scriptPeer) send(t *testing.T, d peerDatagram, resp coap.Message) {
	t.Helper()
	raw, err := resp.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.conn.WriteToUDP(raw, d.addr); err != nil {
		t.Fatal(err)
	}
}

// reply builds a piggybacked acknowledgement for req.
func reply(req coap.Message, code coap.Code, payload string) coap.Message {
	return coap.Message{
		Type: coap.Acknowledgement, Code: code,
		MessageID: req.MessageID, Token: append([]byte(nil), req.Token...),
		Payload: []byte(payload),
	}
}

// notification builds an Observe notification for token.
func notification(token []byte, mid uint16, typ coap.Type, seq coap.Observe, payload string) coap.Message {
	m := coap.Message{
		Type: typ, Code: coap.CodeContent, MessageID: mid,
		Token: append([]byte(nil), token...), Payload: []byte(payload),
	}
	if err := m.SetObserve(seq); err != nil {
		panic(err)
	}
	return m
}

// observeClient dials a client with millisecond exchange timings.
func observeClient(t *testing.T, peer *scriptPeer) *Client {
	t.Helper()
	cli, err := DialConfig(context.Background(), ClientConfig{
		Addr: peer.Addr(),
		// A generous ACK timeout keeps a retransmission out of the datagram
		// sequence the test asserts, while a failing read still ends quickly.
		AckTimeout: 200 * time.Millisecond, AckRandomFactor: 1,
		MaxRetransmit: 2, ExchangeLifetime: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// deliveryLog records the notifications the client handed to its callback.
type deliveryLog struct {
	mu   sync.Mutex
	seen []ClientResponse
}

func (l *deliveryLog) add(resp ClientResponse) {
	l.mu.Lock()
	l.seen = append(l.seen, resp)
	l.mu.Unlock()
}

func (l *deliveryLog) entries() []ClientResponse {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ClientResponse(nil), l.seen...)
}

// registerObservation registers an observation against the peer: it answers the
// registration request with Observe=seq and returns the observation together
// with the registration datagram (which carries the client address to send
// further messages to).
func registerObservation(t *testing.T, cli *Client, peer *scriptPeer, log *deliveryLog, seq coap.Observe) (*Observation, peerDatagram) {
	t.Helper()
	type result struct {
		obs  *Observation
		resp ClientResponse
		err  error
	}
	results := make(chan result, 1)
	go func() {
		obs, resp, err := cli.Observe(context.Background(),
			ClientRequest{Code: coap.CodeGET, Path: "/state"},
			func(resp ClientResponse) { log.add(resp) })
		results <- result{obs: obs, resp: resp, err: err}
	}()
	reg := peer.next(t)
	if reg.msg.Code != coap.CodeGET {
		t.Fatalf("registration code = %v, want GET", reg.msg.Code)
	}
	if value, ok := hasObserve(reg.msg); !ok || value != uint32(coap.ObserveRegister) {
		t.Fatalf("registration observe = %d (present %v), want 0", value, ok)
	}
	initial := reply(reg.msg, coap.CodeContent, "v0")
	if err := initial.SetObserve(seq); err != nil {
		t.Fatal(err)
	}
	peer.send(t, reg, initial)
	select {
	case r := <-results:
		if r.err != nil {
			t.Fatalf("Observe: %v", r.err)
		}
		if string(r.resp.Payload) != "v0" {
			t.Fatalf("registration response payload = %q, want v0", r.resp.Payload)
		}
		return r.obs, reg
	case <-time.After(2 * time.Second):
		t.Fatal("Observe did not return")
		return nil, peerDatagram{}
	}
}

// TestClientObserveNotifications registers, receives a non-confirmable and a
// confirmable notification, rejects a stale one, acknowledges the confirmable
// ones and deregisters.
func TestClientObserveNotifications(t *testing.T) {
	peer := newScriptPeer(t)
	cli := observeClient(t, peer)
	log := &deliveryLog{}

	// Observe blocks until the registration response arrives, so it runs on its
	// own goroutine while the test answers the request.
	type result struct {
		obs *Observation
		err error
	}
	results := make(chan result, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		obs, _, err := cli.Observe(ctx, ClientRequest{Code: coap.CodeGET, Path: "/state"},
			func(resp ClientResponse) { log.add(resp) })
		results <- result{obs: obs, err: err}
	}()
	reg := peer.next(t)
	if value, ok := hasObserve(reg.msg); !ok || value != uint32(coap.ObserveRegister) {
		t.Fatalf("registration observe = %d (present %v), want 0", value, ok)
	}
	initial := reply(reg.msg, coap.CodeContent, "v0")
	if err := initial.SetObserve(5); err != nil {
		t.Fatal(err)
	}
	peer.send(t, reg, initial)
	r := <-results
	if r.err != nil {
		t.Fatalf("Observe: %v", r.err)
	}
	obs := r.obs
	if len(log.entries()) != 0 {
		t.Fatalf("callback ran for the registration response: %+v", log.entries())
	}
	token := append([]byte(nil), reg.msg.Token...)

	// A non-confirmable notification is delivered without an acknowledgement.
	peer.send(t, reg, notification(token, 100, coap.NonConfirmable, 6, "n1"))
	// A confirmable notification is acknowledged and delivered.
	peer.send(t, reg, notification(token, 101, coap.Confirmable, 7, "n2"))
	ack := peer.next(t)
	if ack.msg.Type != coap.Acknowledgement || ack.msg.Code != coap.CodeEmpty || ack.msg.MessageID != 101 {
		t.Fatalf("ack for notification = %v %v mid %d, want empty ACK for 101",
			ack.msg.Type, ack.msg.Code, ack.msg.MessageID)
	}
	// A stale confirmable notification is acknowledged but not delivered; the
	// acknowledgement proves the client processed it.
	peer.send(t, reg, notification(token, 102, coap.Confirmable, 6, "stale"))
	staleAck := peer.next(t)
	if staleAck.msg.Type != coap.Acknowledgement || staleAck.msg.Code != coap.CodeEmpty || staleAck.msg.MessageID != 102 {
		t.Fatalf("ack for stale notification = %v %v mid %d, want empty ACK for 102",
			staleAck.msg.Type, staleAck.msg.Code, staleAck.msg.MessageID)
	}

	// Cancel deregisters with the observation token and stops delivery.
	cancelled := make(chan error, 1)
	go func() { cancelled <- obs.Cancel(context.Background()) }()
	deregister := peer.next(t)
	if deregister.msg.Code != coap.CodeGET {
		t.Fatalf("deregistration code = %v, want GET", deregister.msg.Code)
	}
	if value, ok := hasObserve(deregister.msg); !ok || value != uint32(coap.ObserveDeregister) {
		t.Fatalf("deregistration observe = %d (present %v), want 1", value, ok)
	}
	if string(deregister.msg.Token) != string(token) {
		t.Fatalf("deregistration token = %x, want %x", deregister.msg.Token, token)
	}
	peer.send(t, deregister, reply(deregister.msg, coap.CodeContent, "bye"))
	select {
	case err := <-cancelled:
		if err != nil {
			t.Fatalf("Cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel did not return")
	}
	select {
	case <-obs.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done was not closed after Cancel")
	}
	select {
	case <-obs.watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the observation context watcher did not exit")
	}

	// The stale notification was processed before the deregistration response
	// (one reader, in order), so the callback saw n1 and n2 only.
	got := log.entries()
	if len(got) != 2 || string(got[0].Payload) != "n1" || string(got[1].Payload) != "n2" {
		t.Fatalf("delivered %+v, want n1 then n2", got)
	}
	cli.exMu.Lock()
	observing, pending := len(cli.observes), len(cli.requests)
	cli.exMu.Unlock()
	if observing != 0 || pending != 0 {
		t.Fatalf("client state after Cancel: %d observations, %d pending exchanges", observing, pending)
	}

	// A notification that arrives after the deregistration is dropped. A
	// following request makes the order deterministic: the response arrives
	// after the stray notification was already read.
	peer.send(t, reg, notification(token, 103, coap.NonConfirmable, 8, "late"))
	done := make(chan error, 1)
	go func() {
		_, err := cli.Do(context.Background(), ClientRequest{Code: coap.CodeGET, Path: "/ping"})
		done <- err
	}()
	ping := peer.next(t)
	peer.send(t, ping, reply(ping.msg, coap.CodeContent, "pong"))
	if err := <-done; err != nil {
		t.Fatalf("Do after Cancel: %v", err)
	}
	if got := log.entries(); len(got) != 2 {
		t.Fatalf("delivered %d notifications after deregistration, want 2", len(got))
	}
}

// TestClientObserveContextCancelFreesState cancels the observation context and
// checks that the registration, the pending exchange and the watcher goroutine
// are all gone. It runs enough cycles that a leak or a data race shows up under
// -race.
func TestClientObserveContextCancelFreesState(t *testing.T) {
	peer := newScriptPeer(t)
	cli := observeClient(t, peer)

	for i := 0; i < 25; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		type result struct {
			obs *Observation
			err error
		}
		results := make(chan result, 1)
		go func() {
			obs, _, err := cli.Observe(ctx, ClientRequest{Code: coap.CodeGET, Path: "/state"},
				func(ClientResponse) {})
			results <- result{obs: obs, err: err}
		}()
		reg := peer.next(t)
		initial := reply(reg.msg, coap.CodeContent, "v0")
		if err := initial.SetObserve(coap.Observe(i + 1)); err != nil {
			t.Fatal(err)
		}
		peer.send(t, reg, initial)
		r := <-results
		if r.err != nil {
			t.Fatalf("cycle %d: Observe: %v", i, r.err)
		}
		cancel()

		// Cancelling the context deregisters with a single NON message.
		deregister := peer.next(t)
		if value, ok := hasObserve(deregister.msg); !ok || value != uint32(coap.ObserveDeregister) {
			t.Fatalf("cycle %d: deregistration observe = %d (present %v), want 1", i, value, ok)
		}
		if deregister.msg.Type != coap.NonConfirmable {
			t.Fatalf("cycle %d: deregistration type = %v, want NON", i, deregister.msg.Type)
		}
		if string(deregister.msg.Token) != string(reg.msg.Token) {
			t.Fatalf("cycle %d: deregistration token = %x, want %x", i, deregister.msg.Token, reg.msg.Token)
		}
		select {
		case <-r.obs.watchDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("cycle %d: the observation context watcher did not exit", i)
		}
		cli.exMu.Lock()
		observing, pending, pendingIDs := len(cli.observes), len(cli.requests), len(cli.requestsByID)
		cli.exMu.Unlock()
		if observing != 0 || pending != 0 || pendingIDs != 0 {
			t.Fatalf("cycle %d: client state = %d observations, %d pending exchanges, %d message ids",
				i, observing, pending, pendingIDs)
		}
	}
}

// TestClientObserveErrorNotificationEndsObservation covers a server that ends an
// observation with an error notification: it is delivered once, acknowledged and
// the registration disappears.
func TestClientObserveErrorNotificationEndsObservation(t *testing.T) {
	peer := newScriptPeer(t)
	cli := observeClient(t, peer)
	log := &deliveryLog{}
	obs, reg := registerObservation(t, cli, peer, log, 5)

	gone := coap.Message{
		Type: coap.Confirmable, Code: coap.CodeNotFound, MessageID: 200,
		Token: append([]byte(nil), obs.token...), Payload: []byte("gone"),
	}
	peer.send(t, reg, gone)
	ack := peer.next(t)
	if ack.msg.Type != coap.Acknowledgement || ack.msg.Code != coap.CodeEmpty || ack.msg.MessageID != 200 {
		t.Fatalf("ack = %v %v mid %d, want empty ACK for 200", ack.msg.Type, ack.msg.Code, ack.msg.MessageID)
	}
	select {
	case <-obs.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done was not closed after the error notification")
	}
	select {
	case <-obs.watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the observation context watcher did not exit")
	}
	got := log.entries()
	if len(got) != 1 || got[0].Code != coap.CodeNotFound {
		t.Fatalf("delivered %+v, want one 4.04", got)
	}
	cli.exMu.Lock()
	observing := len(cli.observes)
	cli.exMu.Unlock()
	if observing != 0 {
		t.Fatalf("client still has %d observations", observing)
	}
}

// TestClientObserveCloseEndsObservations checks that closing the client frees
// every observation and its watcher goroutine.
func TestClientObserveCloseEndsObservations(t *testing.T) {
	peer := newScriptPeer(t)
	cli := observeClient(t, peer)
	log := &deliveryLog{}
	obs, _ := registerObservation(t, cli, peer, log, 5)

	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-obs.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done was not closed by Close")
	}
	select {
	case <-obs.watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the observation context watcher did not exit on Close")
	}
	cli.exMu.Lock()
	observing := len(cli.observes)
	cli.exMu.Unlock()
	if observing != 0 {
		t.Fatalf("client still has %d observations after Close", observing)
	}
	// Closing twice is harmless.
	if err := cli.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestClientObserveCloseDuringNotifications races Close with a stream of
// notifications: the reader, the callback and the closing client must not leak
// or race.
func TestClientObserveCloseDuringNotifications(t *testing.T) {
	peer := newScriptPeer(t)
	cli := observeClient(t, peer)
	log := &deliveryLog{}
	obs, reg := registerObservation(t, cli, peer, log, 1)

	for i := 0; i < 20; i++ {
		peer.send(t, reg, notification(obs.token, uint16(300+i), coap.NonConfirmable, coap.Observe(i+2), "n"))
	}
	closed := make(chan error, 1)
	go func() { closed <- cli.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return")
	}
	select {
	case <-obs.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done was not closed by Close")
	}
	select {
	case <-obs.watchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the observation context watcher did not exit")
	}
	cli.exMu.Lock()
	observing := len(cli.observes)
	cli.exMu.Unlock()
	if observing != 0 {
		t.Fatalf("client still has %d observations after Close", observing)
	}
}

// TestObserveFresh pins the RFC 7641 section 3.4 sequence rule, including the
// move from the largest value back to zero.
func TestObserveFresh(t *testing.T) {
	cases := []struct {
		prev, next coap.Observe
		fresh      bool
	}{
		{0, 1, true},
		{1, 0, false},
		{5, 5, false},
		{5, 4, false},
		{5, 6, true},
		{1<<24 - 1, 0, true}, // wrap around
		{0, 1<<24 - 1, false},
		{1, 1<<23 - 1, true},
		{1, 1 << 23, true},
		{0, 1 << 23, false}, // exactly half the space: not decidable, treated as stale
		{1 << 23, 0, false},
		{1 << 23, 1, false},
		{1<<23 - 1, 1, false},
	}
	for _, c := range cases {
		if got := observeFresh(c.prev, c.next); got != c.fresh {
			t.Fatalf("observeFresh(%d, %d) = %v, want %v", c.prev, c.next, got, c.fresh)
		}
	}
}
