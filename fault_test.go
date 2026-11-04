package gcoap

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// The weak-network matrix. Every scenario applies one fault to one feature and
// asserts the invariant that fault must not break: a duplicate request does not
// run its handler twice, a lost acknowledgement is retransmitted byte for byte,
// an out-of-order block is rejected without corrupting the assembler.
//
// Time is manual. Exchange timers run on the fakeClock from clock_test.go, so
// "the ACK was lost, wait for the retransmission" is a clock advance instead of a
// sleep; only the negative assertion (`silent`) waits in real time, as a guard.
//
// The table in fault_matrix_test.go is the coverage record: a fault × feature
// pair with no entry is a visible gap.

// faultServer is a server whose exchange timers run on a manual clock.
type faultServer struct {
	*Server
	clock *fakeClock
}

// newFaultServer starts a server whose timers are manual. Jitter is disabled
// (rand is always 0), so a retransmission interval is exactly AckTimeout << n.
func newFaultServer(t *testing.T, cfg Config, routes func(*Server)) *faultServer {
	t.Helper()
	clock := newFakeClock()
	cfg.Addr = "127.0.0.1:0"
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.params.now = clock.Now
	srv.params.after = clock.After
	srv.params.rand = func() float64 { return 0 }
	srv.dedup.now = clock.Now
	if routes != nil {
		routes(srv)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	return &faultServer{Server: srv, clock: clock}
}

// faultClient is a raw UDP peer: it writes exactly the datagrams a scenario
// chooses, including duplicates, resets and out-of-order blocks.
type faultClient struct {
	conn *net.UDPConn
}

func newFaultClient(t *testing.T, srv *Server) *faultClient {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &faultClient{conn: conn}
}

// send marshals and sends one message, returning the bytes on the wire so a
// scenario can resend them verbatim.
func (c *faultClient) send(t *testing.T, msg coap.Message) []byte {
	t.Helper()
	raw, err := msg.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return c.sendRaw(t, raw)
}

func (c *faultClient) sendRaw(t *testing.T, raw []byte) []byte {
	t.Helper()
	if err := c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.conn.Write(raw); err != nil {
		t.Fatalf("write: %v", err)
	}
	return raw
}

// recv reads one datagram and returns both the message and its bytes.
func (c *faultClient) recv(t *testing.T) (coap.Message, []byte) {
	t.Helper()
	buf := make([]byte, 1500)
	if err := c.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	n, err := c.conn.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	raw := append([]byte(nil), buf[:n]...)
	msg, err := coap.Parse(raw)
	if err != nil {
		t.Fatalf("parse % x: %v", raw, err)
	}
	return msg, raw
}

// silent asserts the server sent nothing. Call it only once the exchange under
// test has ended, so nothing is scheduled and the guard wait cannot race a
// legitimate datagram.
func (c *faultClient) silent(t *testing.T) {
	t.Helper()
	buf := make([]byte, 1500)
	_ = c.conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if n, err := c.conn.Read(buf); err == nil {
		t.Fatalf("unexpected datagram % x", buf[:n])
	}
}

// faultRequest builds a confirmable request with a one-byte token.
func faultRequest(mid uint16, method coap.Code, path string) coap.Message {
	return coap.Message{
		Type: coap.Confirmable, Code: method, MessageID: mid, Token: []byte{byte(mid)},
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte(trimSlash(path))}},
	}
}

// faultObserve builds an observation registration (register=true, Observe=0) or
// deregistration (Observe=1) request.
func faultObserve(mid uint16, path string, register bool) coap.Message {
	msg := faultRequest(mid, coap.CodeGET, path)
	if register {
		msg.Options = append(msg.Options, coap.Option{Number: coap.OptionObserve})
		return msg
	}
	msg.Options = append(msg.Options, observeOption(coap.ObserveDeregister))
	return msg
}

// faultBlock1 builds one Block1 chunk with 1024-byte blocks and a one-byte token.
func faultBlock1(mid uint16, token byte, path string, num uint32, more bool, payload []byte) coap.Message {
	block, err := coap.Block{Num: num, More: more, SizeExp: 6}.Encode()
	if err != nil {
		panic(err) // a fixed, valid block cannot fail to encode
	}
	return coap.Message{
		Type: coap.Confirmable, Code: coap.CodePOST, MessageID: mid, Token: []byte{token},
		Payload: payload,
		Options: []coap.Option{
			{Number: coap.OptionBlock1, Value: block},
			{Number: coap.OptionURIPath, Value: []byte(trimSlash(path))},
		},
	}
}

func trimSlash(path string) string {
	if len(path) > 0 && path[0] == '/' {
		return path[1:]
	}
	return path
}

// --- server side: the peer sends the faults -----------------------------------

// duplicate/request: once a response is cached, a repeated confirmable request is
// answered from the cache, byte for byte, without running the handler again.
func testDuplicateRequest(t *testing.T) {
	var hits atomic.Int32
	f := newFaultServer(t, Config{AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2}, func(s *Server) {
		s.Handle(coap.CodeGET, "/echo", func(context.Context, Request) Response {
			hits.Add(1)
			return Response{Code: coap.CodeContent, Payload: []byte("v")}
		})
	})
	cli := newFaultClient(t, f.Server)
	raw := cli.send(t, faultRequest(0x0101, coap.CodeGET, "/echo"))

	first, firstRaw := cli.recv(t)
	if first.Type != coap.Acknowledgement || first.Code != coap.CodeContent || first.MessageID != 0x0101 {
		t.Fatalf("first response = %s %s mid %d, want ACK 2.05 mid 0x0101", first.Type, first.Code, first.MessageID)
	}

	cli.sendRaw(t, raw)
	_, secondRaw := cli.recv(t)
	if !bytes.Equal(firstRaw, secondRaw) {
		t.Fatalf("duplicate replay differs:\nfirst  % x\nsecond % x", firstRaw, secondRaw)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("handler ran %d times for a duplicated request, want 1", got)
	}
}

// duplicate/non: a non-confirmable request is never deduplicated — nothing
// acknowledges it, so a repeat is a new request by definition.
func testNonConfirmableNotDeduplicated(t *testing.T) {
	var hits atomic.Int32
	f := newFaultServer(t, Config{}, func(s *Server) {
		s.Handle(coap.CodeGET, "/echo", func(context.Context, Request) Response {
			hits.Add(1)
			return Response{Code: coap.CodeContent, Payload: []byte("v")}
		})
	})
	cli := newFaultClient(t, f.Server)
	req := faultRequest(0x0102, coap.CodeGET, "/echo")
	req.Type = coap.NonConfirmable
	raw := cli.send(t, req)
	_, firstRaw := cli.recv(t)
	cli.sendRaw(t, raw)
	_, secondRaw := cli.recv(t)
	if bytes.Equal(firstRaw, secondRaw) {
		t.Fatal("a NON request was answered from the deduplication cache")
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("handler ran %d times for two NON requests, want 2", got)
	}
}

// separateServer returns a server whose /slow handler blocks until the scenario
// releases it, so it answers with a separate response, plus the handler counter
// and the two channels.
func separateServer(t *testing.T, cfg Config) (*faultServer, *atomic.Int32, chan struct{}, chan struct{}) {
	t.Helper()
	var hits atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	f := newFaultServer(t, cfg, func(s *Server) {
		s.Handle(coap.CodeGET, "/slow", func(context.Context, Request) Response {
			hits.Add(1)
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return Response{Code: coap.CodeContent, Payload: []byte("slow")}
		})
	})
	return f, &hits, started, release
}

// startSeparate triggers a separate response and returns the empty ACK, the
// separate response and both raw datagrams.
func startSeparate(t *testing.T, f *faultServer, cli *faultClient, started chan struct{}, release chan struct{}) (coap.Message, coap.Message, []byte) {
	t.Helper()
	req := faultRequest(0x0201, coap.CodeGET, "/slow")
	cli.send(t, req)
	<-started

	f.clock.AdvanceNext(t) // the ACK timer
	ack, _ := cli.recv(t)
	if ack.Type != coap.Acknowledgement || ack.Code != coap.CodeEmpty || ack.MessageID != req.MessageID {
		t.Fatalf("slow handler produced %s %s, want an empty ACK for mid %d", ack.Type, ack.Code, req.MessageID)
	}
	close(release)
	resp, respRaw := cli.recv(t)
	if resp.Type != coap.Confirmable || resp.Code != coap.CodeContent {
		t.Fatalf("separate response = %s %s, want CON 2.05", resp.Type, resp.Code)
	}
	if resp.MessageID == req.MessageID {
		t.Fatal("the separate response reused the request message id")
	}
	return req, resp, respRaw
}

// loss/separate: the ACK of the separate response is lost, so the server
// retransmits the identical datagram, and a duplicate of the original request is
// answered from the deduplication cache while the handler is still running.
func testSeparateResponseAckLost(t *testing.T) {
	f, hits, started, release := separateServer(t, Config{
		SeparateResponses: true, AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2,
	})
	cli := newFaultClient(t, f.Server)
	req, resp, respRaw := startSeparate(t, f, cli, started, release)

	f.clock.AdvanceNext(t) // the separate-response retransmission timer
	retry, retryRaw := cli.recv(t)
	if retry.MessageID != resp.MessageID {
		t.Fatalf("retransmission mid = %d, want %d", retry.MessageID, resp.MessageID)
	}
	if !bytes.Equal(respRaw, retryRaw) {
		t.Fatalf("retransmission is not byte identical:\nfirst  % x\nretry  % x", respRaw, retryRaw)
	}

	// A duplicate of the original request while the exchange is open is answered
	// with the cached empty ACK, not by running the handler again.
	cli.send(t, req)
	duplicate, _ := cli.recv(t)
	if duplicate.MessageID != req.MessageID || duplicate.Type != coap.Acknowledgement {
		t.Fatalf("duplicate request answered with %s %s, want ACK for mid %d", duplicate.Type, duplicate.Code, req.MessageID)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}

	// The ACK ends the exchange and nothing follows.
	cli.send(t, coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: resp.MessageID})
	waitFor(t, "the separate-response exchange to end", func() bool { return f.outboundPending() == 0 })
	cli.silent(t)
}

// delay/separate: an ACK that arrives only after two retransmissions still ends
// the exchange, and the retransmission intervals double.
func testSeparateResponseLateAck(t *testing.T) {
	f, _, started, release := separateServer(t, Config{
		SeparateResponses: true, AckTimeout: 100 * time.Millisecond, MaxRetransmit: 3,
	})
	cli := newFaultClient(t, f.Server)
	_, resp, firstRaw := startSeparate(t, f, cli, started, release)

	first := f.clock.AdvanceNext(t)
	_, secondRaw := cli.recv(t)
	if !bytes.Equal(firstRaw, secondRaw) {
		t.Fatal("the first retransmission is not byte identical")
	}
	second := f.clock.AdvanceNext(t)
	_, thirdRaw := cli.recv(t)
	if !bytes.Equal(firstRaw, thirdRaw) {
		t.Fatal("the second retransmission is not byte identical")
	}
	if got, want := second.Sub(first), 2*100*time.Millisecond; got != want {
		t.Fatalf("second interval = %v, want %v (ACK_TIMEOUT doubled)", got, want)
	}

	cli.send(t, coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: resp.MessageID})
	waitFor(t, "the late ACK to end the exchange", func() bool { return f.outboundPending() == 0 })
	cli.silent(t)
}

// reset/separate: a reset ends the exchange; the server stops retransmitting.
func testSeparateResponseReset(t *testing.T) {
	f, _, started, release := separateServer(t, Config{
		SeparateResponses: true, AckTimeout: 100 * time.Millisecond, MaxRetransmit: 3,
	})
	cli := newFaultClient(t, f.Server)
	_, resp, _ := startSeparate(t, f, cli, started, release)

	cli.send(t, coap.Message{Type: coap.Reset, Code: coap.CodeEmpty, MessageID: resp.MessageID})
	waitFor(t, "the reset to end the exchange", func() bool { return f.outboundPending() == 0 })
	cli.silent(t)
}

// observeServer starts a server with one observed resource and returns it.
func observeServer(t *testing.T, cfg Config) *faultServer {
	t.Helper()
	return newFaultServer(t, cfg, func(s *Server) {
		s.Handle(coap.CodeGET, "/state", func(context.Context, Request) Response {
			return Response{Code: coap.CodeContent, Payload: []byte("v0")}
		})
	})
}

// registerObserver registers the observation of one raw peer and returns the
// client along with the registration response.
func registerObserver(t *testing.T, f *faultServer) (*faultClient, coap.Message) {
	t.Helper()
	cli := newFaultClient(t, f.Server)
	cli.send(t, faultObserve(0x0301, "/state", true))
	resp, _ := cli.recv(t)
	if _, ok := hasObserve(resp); !ok || resp.Code != coap.CodeContent {
		t.Fatalf("registration response = %s (observe present %v), want 2.05 with an Observe option", resp.Code, ok)
	}
	return cli, resp
}

// loss/observe: a confirmable notification whose ACK is lost is retransmitted
// byte for byte, and the observation survives it.
func testNotificationAckLost(t *testing.T) {
	f := observeServer(t, Config{ConfirmableNotifications: true, AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	cli, _ := registerObserver(t, f)

	if sent := f.Notify("/state", Response{Code: coap.CodeContent, Payload: []byte("v1")}); sent != 1 {
		t.Fatalf("Notify sent = %d, want 1", sent)
	}
	note, noteRaw := cli.recv(t)
	if note.Type != coap.Confirmable || string(note.Payload) != "v1" {
		t.Fatalf("notification = %s %q, want CON v1", note.Type, note.Payload)
	}

	f.clock.AdvanceNext(t) // the notification retransmission timer
	retry, retryRaw := cli.recv(t)
	if retry.MessageID != note.MessageID || !bytes.Equal(noteRaw, retryRaw) {
		t.Fatalf("notification retransmission differs:\nfirst % x\nretry % x", noteRaw, retryRaw)
	}

	cli.send(t, coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: note.MessageID})
	waitFor(t, "the notification exchange to end", func() bool { return f.outboundPending() == 0 })

	// The observation still works after the lost ACK.
	if sent := f.Notify("/state", Response{Code: coap.CodeContent, Payload: []byte("v2")}); sent != 1 {
		t.Fatalf("Notify after the ACK = %d, want 1", sent)
	}
	next, _ := cli.recv(t)
	if string(next.Payload) != "v2" {
		t.Fatalf("notification after the lost ACK = %q, want v2", next.Payload)
	}
}

// reset/observe: a reset in answer to a notification cancels the observation, as
// RFC 7641 section 4.5 requires.
func testNotificationResetCancels(t *testing.T) {
	f := observeServer(t, Config{ConfirmableNotifications: true, AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	cli, _ := registerObserver(t, f)

	if sent := f.Notify("/state", Response{Code: coap.CodeContent, Payload: []byte("v1")}); sent != 1 {
		t.Fatalf("Notify sent = %d, want 1", sent)
	}
	note, _ := cli.recv(t)
	cli.send(t, coap.Message{Type: coap.Reset, Code: coap.CodeEmpty, MessageID: note.MessageID})

	waitFor(t, "the reset to remove the observer", func() bool { return observerTotal(f.Server) == 0 })
	if sent := f.Notify("/state", Response{Code: coap.CodeContent, Payload: []byte("v2")}); sent != 0 {
		t.Fatalf("Notify after the reset sent = %d, want 0", sent)
	}
}

// delay/observe: while one confirmable notification is unacknowledged, newer
// states coalesce — the observer sees the newest one, never a queue.
func testNotificationCoalescing(t *testing.T) {
	f := observeServer(t, Config{ConfirmableNotifications: true, AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	cli, _ := registerObserver(t, f)

	f.Notify("/state", Response{Code: coap.CodeContent, Payload: []byte("v1")})
	first, _ := cli.recv(t)
	f.Notify("/state", Response{Code: coap.CodeContent, Payload: []byte("v2")})
	f.Notify("/state", Response{Code: coap.CodeContent, Payload: []byte("v3")})

	cli.send(t, coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: first.MessageID})
	next, _ := cli.recv(t)
	if got := string(next.Payload); got != "v3" {
		t.Fatalf("coalesced notification = %q, want the newest state v3", got)
	}
}

// reorder/block1: a chunk that arrives out of order is rejected with 4.08, and
// the assembler does not keep a half body from it.
func testBlock1Reordered(t *testing.T) {
	var hits atomic.Int32
	f := newFaultServer(t, Config{}, func(s *Server) {
		s.Handle(coap.CodePOST, "/upload", func(_ context.Context, req Request) Response {
			hits.Add(1)
			return Response{Code: coap.CodeChanged, Payload: req.Payload}
		})
	})
	cli := newFaultClient(t, f.Server)
	chunk := bytes.Repeat([]byte{'a'}, 1024)

	// Chunk 1 without chunk 0: nothing to append to.
	if got := cli.sendAndRecv(t, faultBlock1(0x0401, 1, "/upload", 1, false, []byte("late"))); got.Code != coap.CodeRequestEntityIncomplete {
		t.Fatalf("out-of-order first block = %s, want 4.08", got.Code)
	}
	// A fresh block 0 starts the upload even after that rejection.
	if got := cli.sendAndRecv(t, faultBlock1(0x0402, 1, "/upload", 0, true, chunk)); got.Code != coap.CodeContinue {
		t.Fatalf("block 0 after the rejection = %s, want 2.31", got.Code)
	}
	// A gap inside the upload drops the state; block 2 does not append to the
	// body and leaves nothing behind.
	if got := cli.sendAndRecv(t, faultBlock1(0x0403, 1, "/upload", 2, false, []byte("done"))); got.Code != coap.CodeRequestEntityIncomplete {
		t.Fatalf("gap block = %s, want 4.08", got.Code)
	}
	// The state is gone, so the missing block 1 is not a completion either.
	if got := cli.sendAndRecv(t, faultBlock1(0x0404, 1, "/upload", 1, false, []byte("done"))); got.Code != coap.CodeRequestEntityIncomplete {
		t.Fatalf("block 1 after the gap = %s, want 4.08", got.Code)
	}

	// Restarting from block 0 produces the full body exactly once.
	if got := cli.sendAndRecv(t, faultBlock1(0x0405, 1, "/upload", 0, true, chunk)); got.Code != coap.CodeContinue {
		t.Fatalf("restart block 0 = %s, want 2.31", got.Code)
	}
	got := cli.sendAndRecv(t, faultBlock1(0x0406, 1, "/upload", 1, false, []byte("done")))
	if got.Code != coap.CodeChanged {
		t.Fatalf("final block = %s, want 2.04", got.Code)
	}
	if want := append(bytes.Repeat([]byte{'a'}, 1024), []byte("done")...); !bytes.Equal(got.Payload, want) {
		t.Fatalf("assembled body is %d bytes, want %d", len(got.Payload), len(want))
	}
	if hits.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", hits.Load())
	}
}

// duplicate/block1: a repeated final block with a new message id replays the
// cached final response instead of running the handler again.
func testBlock1DuplicateFinal(t *testing.T) {
	var hits atomic.Int32
	f := newFaultServer(t, Config{}, func(s *Server) {
		s.Handle(coap.CodePOST, "/upload", func(_ context.Context, req Request) Response {
			hits.Add(1)
			return Response{Code: coap.CodeChanged, Payload: req.Payload}
		})
	})
	cli := newFaultClient(t, f.Server)
	chunk := bytes.Repeat([]byte{'b'}, 1024)
	cli.send(t, faultBlock1(0x0501, 1, "/upload", 0, true, chunk))
	if ack, _ := cli.recv(t); ack.Code != coap.CodeContinue {
		t.Fatalf("block 0 = %s, want 2.31", ack.Code)
	}
	cli.send(t, faultBlock1(0x0502, 1, "/upload", 1, false, []byte("done")))
	final, _ := cli.recv(t)
	if final.Code != coap.CodeChanged {
		t.Fatalf("final block = %s, want 2.04", final.Code)
	}

	// The same final block, new message id: a client whose ACK was lost.
	cli.send(t, faultBlock1(0x0503, 1, "/upload", 1, false, []byte("done")))
	replay, _ := cli.recv(t)
	if replay.Code != final.Code || !bytes.Equal(replay.Payload, final.Payload) {
		t.Fatalf("replayed final response = %s %d bytes, want %s %d bytes", replay.Code, len(replay.Payload), final.Code, len(final.Payload))
	}
	if hits.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", hits.Load())
	}
}

// reconnect/request: deduplication is keyed by the transport peer identity, so a
// reconnecting peer that reuses a message id gets a new exchange, not a replay.
func testReconnectNotDeduplicated(t *testing.T) {
	var hits atomic.Int32
	f := newFaultServer(t, Config{}, func(s *Server) {
		s.Handle(coap.CodeGET, "/echo", func(context.Context, Request) Response {
			hits.Add(1)
			return Response{Code: coap.CodeContent, Payload: []byte("v")}
		})
	})
	first := newFaultClient(t, f.Server)
	req := faultRequest(0x0601, coap.CodeGET, "/echo")
	raw := first.send(t, req)
	if resp, _ := first.recv(t); resp.Code != coap.CodeContent {
		t.Fatalf("first peer response = %s, want 2.05", resp.Code)
	}

	// A new socket is a new peer: the same bytes must run the handler again.
	second := newFaultClient(t, f.Server)
	second.sendRaw(t, raw)
	if resp, _ := second.recv(t); resp.Code != coap.CodeContent {
		t.Fatalf("reconnected peer response = %s, want 2.05", resp.Code)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("handler ran %d times across two peers, want 2", got)
	}
}

// sendAndRecv sends one message and returns the response.
func (c *faultClient) sendAndRecv(t *testing.T, msg coap.Message) coap.Message {
	t.Helper()
	c.send(t, msg)
	resp, _ := c.recv(t)
	return resp
}
