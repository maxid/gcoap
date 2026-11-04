package gcoap

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// warmSession makes the peer send one request, so the server holds a session for
// its address — the precondition of every server-initiated request.
func warmSession(t *testing.T, srv *Server, peer *testPeer) {
	t.Helper()
	req := coap.Message{
		Type: coap.Confirmable, Code: coap.CodeGET, MessageID: 0x7001, Token: []byte("warm"),
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("warm")}},
	}
	raw, err := req.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	addr, err := net.ResolveUDPAddr("udp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.conn.WriteToUDP(raw, addr); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a session for the peer", func() bool { return srv.sessionByAddr(peer.Addr()) != nil })
}

// faultTestServer starts a server with a route for the session warm-up.
func faultTestServer(t *testing.T, cfg Config) *faultServer {
	t.Helper()
	return newFaultServer(t, cfg, func(s *Server) {
		s.Handle(coap.CodeGET, "/warm", func(context.Context, Request) Response {
			return Response{Code: coap.CodeContent, Payload: []byte("ok")}
		})
	})
}

// TestServerRequestPiggyback sends a request over the peer's session and reads the
// response from the piggybacked acknowledgement.
func TestServerRequestPiggyback(t *testing.T) {
	f := faultTestServer(t, Config{AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		return []coap.Message{{
			Type: coap.Acknowledgement, Code: coap.CodeContent,
			MessageID: req.MessageID, Token: append([]byte(nil), req.Token...), Payload: []byte("21.5"),
		}}
	})
	warmSession(t, f.Server, peer)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := f.DoTo(ctx, peer.Addr(), ClientRequest{Code: coap.CodeGET, Path: "/3303/0/5700"})
	if err != nil {
		t.Fatalf("DoTo: %v", err)
	}
	if resp.Code != coap.CodeContent || string(resp.Payload) != "21.5" {
		t.Fatalf("response = %s %q, want 2.05 21.5", resp.Code, resp.Payload)
	}
}

// TestServerRequestSeparateResponse answers with an empty acknowledgement first and
// a separate confirmable response afterwards; the server must acknowledge that
// response and return it.
func TestServerRequestSeparateResponse(t *testing.T) {
	f := faultTestServer(t, Config{AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	var acked atomic.Int32
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		if req.Type == coap.Acknowledgement || req.Type == coap.Reset {
			acked.Store(int32(req.MessageID))
			return nil
		}
		return []coap.Message{
			{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: req.MessageID},
			{
				Type: coap.Confirmable, Code: coap.CodeContent, MessageID: 0x4242,
				Token: append([]byte(nil), req.Token...), Payload: []byte("separate"),
			},
		}
	})
	warmSession(t, f.Server, peer)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := f.DoTo(ctx, peer.Addr(), ClientRequest{Code: coap.CodeGET, Path: "/3303/0/5700"})
	if err != nil {
		t.Fatalf("DoTo: %v", err)
	}
	if string(resp.Payload) != "separate" {
		t.Fatalf("response payload = %q, want separate", resp.Payload)
	}
	waitFor(t, "the separate response to be acknowledged", func() bool { return acked.Load() == 0x4242 })
}

// TestServerRequestRetransmitsUntilAnswered drops the first transmission and lets
// the manual clock fire the retransmission, which is byte-identical.
func TestServerRequestRetransmitsUntilAnswered(t *testing.T) {
	f := faultTestServer(t, Config{AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	var seen atomic.Int32
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		if string(req.Token) == "warm" {
			return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID, Token: req.Token, Payload: []byte("ok")}}
		}
		if seen.Add(1) == 1 {
			return nil // drop the first transmission
		}
		return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID, Token: req.Token, Payload: []byte("late")}}
	})
	warmSession(t, f.Server, peer)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type result struct {
		resp ClientResponse
		err  error
	}
	out := make(chan result, 1)
	go func() {
		resp, err := f.DoTo(ctx, peer.Addr(), ClientRequest{Code: coap.CodeGET, Path: "/3303/0/5700"})
		out <- result{resp, err}
	}()
	waitFor(t, "the first transmission", func() bool { return seen.Load() >= 1 })
	before := peer.received.Load()
	f.clock.AdvanceNext(t) // the retransmission timer
	waitFor(t, "the retransmission", func() bool { return peer.received.Load() > before })

	select {
	case r := <-out:
		if r.err != nil {
			t.Fatalf("DoTo: %v", r.err)
		}
		if string(r.resp.Payload) != "late" {
			t.Fatalf("response payload = %q, want late", r.resp.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DoTo did not return")
	}
}

// TestServerRequestReset reports a peer's reset as ErrReset.
func TestServerRequestReset(t *testing.T) {
	f := faultTestServer(t, Config{AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		if string(req.Token) == "warm" {
			return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID, Token: req.Token}}
		}
		return []coap.Message{{Type: coap.Reset, Code: coap.CodeEmpty, MessageID: req.MessageID}}
	})
	warmSession(t, f.Server, peer)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := f.DoTo(ctx, peer.Addr(), ClientRequest{Code: coap.CodeGET, Path: "/3303/0/5700"})
	if !errors.Is(err, ErrReset) {
		t.Fatalf("DoTo = %v, want ErrReset", err)
	}
}

// TestServerRequestNeedsSession refuses an address no session belongs to.
func TestServerRequestNeedsSession(t *testing.T) {
	f := faultTestServer(t, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := f.DoTo(ctx, "127.0.0.1:1", ClientRequest{Code: coap.CodeGET, Path: "/x"})
	if !errors.Is(err, ErrNoSession) {
		t.Fatalf("DoTo = %v, want ErrNoSession", err)
	}
}

// TestServerRequestAssemblesBlock2 follows a sliced response until the last block.
func TestServerRequestAssemblesBlock2(t *testing.T) {
	f := faultTestServer(t, Config{AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		if string(req.Token) == "warm" {
			return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID, Token: req.Token}}
		}
		block, has := req.Block2()
		if !has {
			value, _ := coap.Block{Num: 0, More: true, SizeExp: 0}.Encode() // 16-byte blocks
			return []coap.Message{{
				Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID,
				Token: append([]byte(nil), req.Token...), Options: []coap.Option{{Number: coap.OptionBlock2, Value: value}},
				Payload: []byte("0123456789abcdef"),
			}}
		}
		value, _ := coap.Block{Num: block.Num, More: block.Num < 1, SizeExp: 0}.Encode()
		payload := []byte("FEDCBA9876543210")
		return []coap.Message{{
			Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID,
			Token: append([]byte(nil), req.Token...), Options: []coap.Option{{Number: coap.OptionBlock2, Value: value}},
			Payload: payload,
		}}
	})
	warmSession(t, f.Server, peer)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := f.DoTo(ctx, peer.Addr(), ClientRequest{Code: coap.CodeGET, Path: "/3303/0"})
	if err != nil {
		t.Fatalf("DoTo: %v", err)
	}
	if got, want := string(resp.Payload), "0123456789abcdefFEDCBA9876543210"; got != want {
		t.Fatalf("assembled payload = %q, want %q", got, want)
	}
}

// TestServerRequestRejectsLargeBody refuses a request body Block1 would be needed
// for: the server side does not implement it yet.
func TestServerRequestRejectsLargeBody(t *testing.T) {
	f := faultTestServer(t, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := f.DoTo(ctx, "127.0.0.1:1", ClientRequest{
		Code: coap.CodePUT, Path: "/3303/0/5700", Payload: make([]byte, DefaultClientBlockSize+1),
	})
	if !errors.Is(err, ErrRequestTooLarge) {
		t.Fatalf("DoTo = %v, want ErrRequestTooLarge", err)
	}
}
