package gcoap

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// The client half of the fault matrix. It reuses the scripted peer from
// client_observe_test.go — a bare UDP socket that answers only when the test says
// so — and puts the client's exchange timers on the manual clock, which makes
// "the response was lost, wait for the retransmission" a clock advance instead of
// a sleep.

// faultClientPair returns a scripted peer, a client whose exchange timers run on
// a manual clock, and that clock.
func faultClientPair(t *testing.T) (*scriptPeer, *Client, *fakeClock) {
	t.Helper()
	peer := newScriptPeer(t)
	cli := observeClient(t, peer)
	clock := newFakeClock()
	cli.params.now = clock.Now
	cli.params.after = clock.After
	cli.params.rand = func() float64 { return 0 }
	return peer, cli, clock
}

// peerNone asserts the client sent nothing more.
func peerNone(t *testing.T, peer *scriptPeer) {
	t.Helper()
	select {
	case d := <-peer.recv:
		t.Fatalf("unexpected client datagram: %s %s mid %d", d.msg.Type, d.msg.Code, d.msg.MessageID)
	case <-time.After(100 * time.Millisecond):
	}
}

// clientPending reports how many request exchanges the client still holds: a
// completed or failed Do must leave none.
func clientPending(cli *Client) int {
	cli.exMu.Lock()
	defer cli.exMu.Unlock()
	return len(cli.requests)
}

type doResult struct {
	resp ClientResponse
	err  error
}

// doAsync runs one request in the background and returns its result channel.
func doAsync(cli *Client, req ClientRequest) <-chan doResult {
	out := make(chan doResult, 1)
	go func() {
		resp, err := cli.Do(context.Background(), req)
		out <- doResult{resp: resp, err: err}
	}()
	return out
}

func awaitDo(t *testing.T, out <-chan doResult) doResult {
	t.Helper()
	select {
	case r := <-out:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("the client did not finish")
		return doResult{}
	}
}

// sameWire reports whether two parsed datagrams encode to the same bytes, which
// is the property a retransmission must have.
func sameWire(t *testing.T, first, second coap.Message) bool {
	t.Helper()
	a, err := first.Marshal()
	if err != nil {
		t.Fatalf("marshal first: %v", err)
	}
	b, err := second.Marshal()
	if err != nil {
		t.Fatalf("marshal second: %v", err)
	}
	return bytes.Equal(a, b)
}

// loss/request: the first request is dropped, so the client retransmits the
// identical datagram (same message id, same token) and the answer to that
// retransmission completes the call.
func testClientResponseLost(t *testing.T) {
	peer, cli, clock := faultClientPair(t)
	out := doAsync(cli, ClientRequest{Code: coap.CodeGET, Path: "/x"})

	first := peer.next(t)
	clock.AdvanceNext(t) // the retransmission timer
	second := peer.next(t)
	if !sameWire(t, first.msg, second.msg) {
		t.Fatalf("retransmission differs:\nfirst  %+v\nsecond %+v", first.msg, second.msg)
	}
	if second.msg.Type != coap.Confirmable || second.msg.MessageID != first.msg.MessageID {
		t.Fatalf("retransmission = %s mid %d, want CON mid %d", second.msg.Type, second.msg.MessageID, first.msg.MessageID)
	}

	peer.send(t, second, reply(second.msg, coap.CodeContent, "v"))
	r := awaitDo(t, out)
	if r.err != nil || string(r.resp.Payload) != "v" {
		t.Fatalf("Do after the retransmission = %v %q, want no error and v", r.err, r.resp.Payload)
	}
	waitFor(t, "the exchange to be released", func() bool { return clientPending(cli) == 0 })
}

// delay/request duplicated: a response that arrives twice completes the call
// once and leaves no exchange state behind.
func testClientDuplicateResponse(t *testing.T) {
	peer, cli, _ := faultClientPair(t)
	out := doAsync(cli, ClientRequest{Code: coap.CodeGET, Path: "/x"})

	req := peer.next(t)
	answer := reply(req.msg, coap.CodeContent, "v")
	peer.send(t, req, answer)
	peer.send(t, req, answer) // the same response again, e.g. a late retransmission

	r := awaitDo(t, out)
	if r.err != nil || string(r.resp.Payload) != "v" {
		t.Fatalf("Do = %v %q, want no error and v", r.err, r.resp.Payload)
	}
	waitFor(t, "the exchange to be released", func() bool { return clientPending(cli) == 0 })

	// The client's exchange table is still usable after the duplicate.
	second := doAsync(cli, ClientRequest{Code: coap.CodeGET, Path: "/y"})
	req2 := peer.next(t)
	peer.send(t, req2, reply(req2.msg, coap.CodeContent, "w"))
	if r := awaitDo(t, second); r.err != nil || string(r.resp.Payload) != "w" {
		t.Fatalf("second Do = %v %q, want no error and w", r.err, r.resp.Payload)
	}
}

// reset/request: a reset for the request ends the exchange with ErrReset and no
// retransmission.
func testClientReset(t *testing.T) {
	peer, cli, _ := faultClientPair(t)
	out := doAsync(cli, ClientRequest{Code: coap.CodeGET, Path: "/x"})

	req := peer.next(t)
	peer.send(t, req, coap.Message{Type: coap.Reset, Code: coap.CodeEmpty, MessageID: req.msg.MessageID})
	r := awaitDo(t, out)
	if !errors.Is(r.err, ErrReset) {
		t.Fatalf("Do after a reset = %v, want ErrReset", r.err)
	}
	peerNone(t, peer)
	waitFor(t, "the exchange to be released", func() bool { return clientPending(cli) == 0 })
}

// delay/request: an empty ACK stops the client's retransmissions, and the
// separate response that follows is acknowledged by the client and returned.
func testClientEmptyAckThenSeparate(t *testing.T) {
	peer, cli, _ := faultClientPair(t)
	out := doAsync(cli, ClientRequest{Code: coap.CodeGET, Path: "/slow"})

	req := peer.next(t)
	peer.send(t, req, coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: req.msg.MessageID})
	peerNone(t, peer) // the empty ACK cancelled the request retransmissions

	separate := coap.Message{
		Type: coap.Confirmable, Code: coap.CodeContent, MessageID: 0x7f7f,
		Token: append([]byte(nil), req.msg.Token...), Payload: []byte("slow"),
	}
	peer.send(t, req, separate)
	r := awaitDo(t, out)
	if r.err != nil || string(r.resp.Payload) != "slow" {
		t.Fatalf("Do with a separate response = %v %q, want no error and slow", r.err, r.resp.Payload)
	}
	ack := peer.next(t)
	if ack.msg.Type != coap.Acknowledgement || ack.msg.MessageID != separate.MessageID {
		t.Fatalf("client answered the separate response with %s mid %d, want ACK mid %d", ack.msg.Type, ack.msg.MessageID, separate.MessageID)
	}
}

// reorder/observe: notifications that arrive out of order are filtered by the
// 24-bit sequence rule — the stale one is not delivered, but a confirmable stale
// one is still acknowledged.
func testClientStaleNotification(t *testing.T) {
	peer, cli, _ := faultClientPair(t)
	log := &deliveryLog{}
	// The observation ends with the client in the fixture's cleanup; the peer
	// never answers a deregistration, so cancelling it here would only wait.
	_, reg := registerObservation(t, cli, peer, log, 5)

	peer.send(t, reg, notification(reg.msg.Token, 0x7001, coap.NonConfirmable, 6, "n6"))
	waitFor(t, "the fresh notification", func() bool { return len(log.entries()) == 1 })

	peer.send(t, reg, notification(reg.msg.Token, 0x7002, coap.Confirmable, 3, "n3"))
	ack := peer.next(t)
	if ack.msg.Type != coap.Acknowledgement || ack.msg.MessageID != 0x7002 {
		t.Fatalf("stale notification answered with %s mid %d, want ACK mid 0x7002", ack.msg.Type, ack.msg.MessageID)
	}

	// Nothing more is delivered: the stale sequence is dropped.
	time.Sleep(100 * time.Millisecond)
	if got := len(log.entries()); got != 1 {
		t.Fatalf("delivered notifications = %d after a stale one, want 1", got)
	}
}
