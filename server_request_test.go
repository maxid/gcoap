package gcoap

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
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

// block1Option encodes a Block1 option value for a 2.31 answer.
func block1Option(block coap.Block) coap.Option {
	value, err := block.Encode()
	if err != nil {
		panic(err)
	}
	return coap.Option{Number: coap.OptionBlock1, Value: value}
}

// TestServerRequestUploadsBlock1 sends a body larger than one block: every
// non-final block waits for 2.31 Continue, and the final one carries the response.
func TestServerRequestUploadsBlock1(t *testing.T) {
	f := faultTestServer(t, Config{AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	var mu sync.Mutex
	var seen []coap.Message
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		if string(req.Token) == "warm" {
			return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID, Token: req.Token}}
		}
		mu.Lock()
		seen = append(seen, req)
		mu.Unlock()
		if block, ok := req.Block1(); ok && block.More {
			return []coap.Message{{
				Type: coap.Acknowledgement, Code: coap.CodeContinue, MessageID: req.MessageID,
				Token: append([]byte(nil), req.Token...), Options: []coap.Option{block1Option(block)},
			}}
		}
		return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeChanged, MessageID: req.MessageID, Token: append([]byte(nil), req.Token...)}}
	})
	warmSession(t, f.Server, peer)

	body := append(bytes.Repeat([]byte{'x'}, 2048), []byte("tail")...) // three blocks: 1024, 1024, 4
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := f.DoTo(ctx, peer.Addr(), ClientRequest{Code: coap.CodePUT, Path: "/3303/0/5700", Payload: body})
	if err != nil {
		t.Fatalf("DoTo: %v", err)
	}
	if resp.Code != coap.CodeChanged {
		t.Fatalf("response = %s, want 2.04", resp.Code)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("blocks sent = %d, want 3", len(seen))
	}
	var assembled []byte
	for i, msg := range seen {
		block, ok := msg.Block1()
		if !ok {
			t.Fatalf("block %d carries no Block1 option", i)
		}
		if block.Num != uint32(i) {
			t.Fatalf("block %d carries number %d", i, block.Num)
		}
		if size1, ok := msg.Size1(); !ok || int(size1) != len(body) {
			t.Fatalf("block %d Size1 = %d (present %v), want %d", i, size1, ok, len(body))
		}
		assembled = append(assembled, msg.Payload...)
	}
	if !bytes.Equal(assembled, body) {
		t.Fatalf("peer received %d bytes, want %d", len(assembled), len(body))
	}
}

// TestServerRequestUploadsBlock1Downgraded adopts a smaller block size the peer
// asks for after block 0, recomputing the block number without skipping a byte.
func TestServerRequestUploadsBlock1Downgraded(t *testing.T) {
	f := faultTestServer(t, Config{AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	var mu sync.Mutex
	var seen []coap.Message
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		if string(req.Token) == "warm" {
			return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID, Token: req.Token}}
		}
		mu.Lock()
		seen = append(seen, req)
		mu.Unlock()
		block, ok := req.Block1()
		if ok && block.More {
			// Ask for 256-byte blocks from the second block on.
			return []coap.Message{{
				Type: coap.Acknowledgement, Code: coap.CodeContinue, MessageID: req.MessageID,
				Token: append([]byte(nil), req.Token...), Options: []coap.Option{block1Option(coap.Block{Num: block.Num, SizeExp: 4})},
			}}
		}
		return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeChanged, MessageID: req.MessageID, Token: append([]byte(nil), req.Token...)}}
	})
	warmSession(t, f.Server, peer)

	body := bytes.Repeat([]byte{'y'}, 2048) // 1024 + 4*256
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := f.DoTo(ctx, peer.Addr(), ClientRequest{Code: coap.CodePUT, Path: "/3303/0/5700", Payload: body}); err != nil {
		t.Fatalf("DoTo: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 5 {
		t.Fatalf("blocks sent = %d, want 5", len(seen))
	}
	if first, _ := seen[0].Block1(); first.SizeExp != defaultUploadExp {
		t.Fatalf("block 0 size exponent = %d, want %d", first.SizeExp, defaultUploadExp)
	}
	for i, msg := range seen[1:] {
		block, _ := msg.Block1()
		if block.SizeExp != 4 || len(msg.Payload) != 256 {
			t.Fatalf("block %d: size exponent %d, %d bytes, want 4 and 256", i+1, block.SizeExp, len(msg.Payload))
		}
		if want := uint32(4 + i); block.Num != want {
			t.Fatalf("block %d carries number %d, want %d (block numbers count in the smaller size)", i+1, block.Num, want)
		}
	}
}
