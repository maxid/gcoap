package gcoap

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// block1Request builds one Block1 chunk of a PUT to /upload.
func block1Request(t *testing.T, mid uint16, token []byte, path string, payload []byte, num uint32, more bool) coap.Message {
	t.Helper()
	req := coap.Message{
		Type: coap.Confirmable, Code: coap.CodePUT, MessageID: mid, Token: token,
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte(path)}},
		Payload: payload,
	}
	if err := req.SetBlock1(coap.Block{Num: num, More: more, SizeExp: 0}); err != nil {
		t.Fatal(err)
	}
	return req
}

// block1Server starts a server whose /upload handler counts its calls and keeps
// the last body.
func block1Server(t *testing.T, tune func(*Config)) (*Server, *atomic.Int64, <-chan []byte) {
	t.Helper()
	cfg := Config{Addr: "127.0.0.1:0"}
	if tune != nil {
		tune(&cfg)
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	stored := make(chan []byte, 4)
	srv.Handle(coap.CodePUT, "/upload", func(_ context.Context, req Request) Response {
		calls.Add(1)
		stored <- append([]byte(nil), req.Payload...)
		return Response{Code: coap.CodeChanged}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	return srv, &calls, stored
}

func TestBlock1LimitsAndState(t *testing.T) {
	payload := []byte("0123456789abcdef0123456789abcdef") // 32 bytes, two 16-byte blocks

	t.Run("duplicate block is not appended twice", func(t *testing.T) {
		srv, calls, stored := block1Server(t, nil)
		conn := dialServer(t, srv)
		token := []byte{1}
		first := block1Request(t, 1, token, "upload", payload[:16], 0, true)
		if resp := roundTrip(t, conn, first); resp.Code != coap.CodeContinue {
			t.Fatalf("first = %v, want 2.31", resp.Code)
		}
		// The same block with a new message id must be acknowledged again.
		repeat := block1Request(t, 2, token, "upload", payload[:16], 0, true)
		if resp := roundTrip(t, conn, repeat); resp.Code != coap.CodeContinue {
			t.Fatalf("repeat = %v, want 2.31", resp.Code)
		}
		second := block1Request(t, 3, token, "upload", payload[16:], 1, false)
		if resp := roundTrip(t, conn, second); resp.Code != coap.CodeChanged {
			t.Fatalf("second = %v, want 2.04", resp.Code)
		}
		if calls.Load() != 1 {
			t.Fatalf("handler ran %d times, want 1", calls.Load())
		}
		select {
		case got := <-stored:
			if !bytes.Equal(got, payload) {
				t.Fatalf("stored %q, want %q", got, payload)
			}
		case <-time.After(time.Second):
			t.Fatal("handler stored nothing")
		}
	})

	t.Run("duplicate final block replays the response", func(t *testing.T) {
		srv, calls, _ := block1Server(t, nil)
		conn := dialServer(t, srv)
		token := []byte{2}
		if resp := roundTrip(t, conn, block1Request(t, 1, token, "upload", payload[:16], 0, true)); resp.Code != coap.CodeContinue {
			t.Fatalf("first = %v", resp.Code)
		}
		final := block1Request(t, 2, token, "upload", payload[16:], 1, false)
		if resp := roundTrip(t, conn, final); resp.Code != coap.CodeChanged {
			t.Fatalf("final = %v", resp.Code)
		}
		// A retransmission that reaches the block layer with a fresh message id
		// (the dedup cache only covers the same id) is answered from the cache.
		again := block1Request(t, 3, token, "upload", payload[16:], 1, false)
		if resp := roundTrip(t, conn, again); resp.Code != coap.CodeChanged {
			t.Fatalf("replayed final = %v, want 2.04", resp.Code)
		}
		if calls.Load() != 1 {
			t.Fatalf("handler ran %d times, want 1", calls.Load())
		}
	})

	t.Run("declared body above the limit", func(t *testing.T) {
		srv, _, _ := block1Server(t, func(cfg *Config) { cfg.MaxBlockPayload = 16 })
		conn := dialServer(t, srv)
		req := block1Request(t, 1, []byte{3}, "upload", payload[:16], 0, true)
		req.SetSize1(uint32(len(payload)))
		if resp := roundTrip(t, conn, req); resp.Code != coap.CodeRequestEntityTooLarge {
			t.Fatalf("declared body = %v, want 4.13", resp.Code)
		}
	})

	t.Run("accumulated body above the limit", func(t *testing.T) {
		srv, _, _ := block1Server(t, func(cfg *Config) { cfg.MaxBlockPayload = 24 })
		conn := dialServer(t, srv)
		token := []byte{4}
		if resp := roundTrip(t, conn, block1Request(t, 1, token, "upload", payload[:16], 0, true)); resp.Code != coap.CodeContinue {
			t.Fatalf("first = %v", resp.Code)
		}
		if resp := roundTrip(t, conn, block1Request(t, 2, token, "upload", payload[16:], 1, false)); resp.Code != coap.CodeRequestEntityTooLarge {
			t.Fatalf("second = %v, want 4.13", resp.Code)
		}
	})

	t.Run("path change mid-upload", func(t *testing.T) {
		srv, _, _ := block1Server(t, nil)
		conn := dialServer(t, srv)
		token := []byte{5}
		if resp := roundTrip(t, conn, block1Request(t, 1, token, "upload", payload[:16], 0, true)); resp.Code != coap.CodeContinue {
			t.Fatalf("first = %v", resp.Code)
		}
		if resp := roundTrip(t, conn, block1Request(t, 2, token, "other", payload[16:], 1, false)); resp.Code != coap.CodeRequestEntityIncomplete {
			t.Fatalf("changed path = %v, want 4.08", resp.Code)
		}
	})

	t.Run("gap in the sequence", func(t *testing.T) {
		srv, _, _ := block1Server(t, nil)
		conn := dialServer(t, srv)
		if resp := roundTrip(t, conn, block1Request(t, 1, []byte{6}, "upload", payload[16:], 1, false)); resp.Code != coap.CodeRequestEntityIncomplete {
			t.Fatalf("first block 1 = %v, want 4.08", resp.Code)
		}
	})

	t.Run("changed duplicate payload", func(t *testing.T) {
		srv, _, _ := block1Server(t, nil)
		conn := dialServer(t, srv)
		three := append(append([]byte(nil), payload...), payload...)
		token := []byte{7}
		if resp := roundTrip(t, conn, block1Request(t, 1, token, "upload", three[0:16], 0, true)); resp.Code != coap.CodeContinue {
			t.Fatalf("block 0 = %v", resp.Code)
		}
		if resp := roundTrip(t, conn, block1Request(t, 2, token, "upload", three[16:32], 1, true)); resp.Code != coap.CodeContinue {
			t.Fatalf("block 1 = %v", resp.Code)
		}
		changed := append([]byte(nil), three[16:32]...)
		changed[0] = 'X'
		if resp := roundTrip(t, conn, block1Request(t, 3, token, "upload", changed, 1, true)); resp.Code != coap.CodeRequestEntityIncomplete {
			t.Fatalf("changed duplicate = %v, want 4.08", resp.Code)
		}
	})

	t.Run("too many uploads", func(t *testing.T) {
		srv, _, _ := block1Server(t, func(cfg *Config) { cfg.MaxBlockUploads = 1 })
		conn := dialServer(t, srv)
		if resp := roundTrip(t, conn, block1Request(t, 1, []byte{8}, "upload", payload[:16], 0, true)); resp.Code != coap.CodeContinue {
			t.Fatalf("first upload = %v", resp.Code)
		}
		if resp := roundTrip(t, conn, block1Request(t, 2, []byte{9}, "upload", payload[:16], 0, true)); resp.Code != coap.CodeServiceUnavailable {
			t.Fatalf("second upload = %v, want 5.03", resp.Code)
		}
	})

	t.Run("idle upload expires", func(t *testing.T) {
		srv, _, _ := block1Server(t, func(cfg *Config) {
			cfg.BlockIdleTimeout = 20 * time.Millisecond
		})
		conn := dialServer(t, srv)
		token := []byte{10}
		if resp := roundTrip(t, conn, block1Request(t, 1, token, "upload", payload[:16], 0, true)); resp.Code != coap.CodeContinue {
			t.Fatalf("first = %v", resp.Code)
		}
		time.Sleep(60 * time.Millisecond)
		if resp := roundTrip(t, conn, block1Request(t, 2, token, "upload", payload[16:], 1, false)); resp.Code != coap.CodeRequestEntityIncomplete {
			t.Fatalf("after idle = %v, want 4.08", resp.Code)
		}
	})
}

func TestBlock1ConfigValidation(t *testing.T) {
	cases := map[string]Config{
		"negative observers":    {MaxObservers: -1},
		"negative uploads":      {MaxBlockUploads: -1},
		"negative payload":      {MaxBlockPayload: -1},
		"negative idle timeout": {BlockIdleTimeout: -time.Second},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if srv, err := New(cfg); err == nil {
				_ = srv.Stop(context.Background())
				t.Fatal("New succeeded")
			}
		})
	}
	if _, err := DialConfig(context.Background(), ClientConfig{Addr: "127.0.0.1:1", MaxResponseSize: -1}); err == nil {
		t.Fatal("ClientConfig accepted a negative MaxResponseSize")
	}
}

func TestMaxObservers(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0", MaxObservers: 1})
	if err != nil {
		t.Fatal(err)
	}
	srv.Handle(coap.CodeGET, "/obs", func(context.Context, Request) Response {
		return Response{Code: coap.CodeContent, Payload: []byte("v")}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()
	conn := dialServer(t, srv)

	observe := func(mid uint16, token []byte) coap.Message {
		req := coap.Message{
			Type: coap.Confirmable, Code: coap.CodeGET, MessageID: mid, Token: token,
			Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("obs")}},
		}
		if err := req.SetObserve(coap.ObserveRegister); err != nil {
			t.Fatal(err)
		}
		return roundTrip(t, conn, req)
	}
	if resp := observe(1, []byte{1}); resp.Code != coap.CodeContent {
		t.Fatalf("first registration = %v", resp.Code)
	}
	if resp := observe(2, []byte{2}); resp.Code != coap.CodeServiceUnavailable {
		t.Fatalf("second registration = %v, want 5.03", resp.Code)
	}
}

// metricsRecorder captures every hook for assertions.
type metricsRecorder struct {
	exchanges   atomic.Int64
	dedups      atomic.Int64
	retransmits atomic.Int64
	observes    atomic.Int64
	blocks      atomic.Int64
}

func (m *metricsRecorder) Exchange(coap.Code, string, time.Duration) { m.exchanges.Add(1) }
func (m *metricsRecorder) Dedup()                                    { m.dedups.Add(1) }
func (m *metricsRecorder) Retransmit(string)                         { m.retransmits.Add(1) }
func (m *metricsRecorder) Observe(string)                            { m.observes.Add(1) }
func (m *metricsRecorder) Block(string, string)                      { m.blocks.Add(1) }

func TestServerMetrics(t *testing.T) {
	recorder := &metricsRecorder{}
	srv, err := New(Config{Addr: "127.0.0.1:0", Metrics: recorder})
	if err != nil {
		t.Fatal(err)
	}
	srv.Handle(coap.CodeGET, "/m", func(context.Context, Request) Response {
		return Response{Code: coap.CodeContent, Payload: []byte("ok")}
	})
	srv.Handle(coap.CodePUT, "/m", func(_ context.Context, req Request) Response {
		return Response{Code: coap.CodeChanged, Payload: req.Payload}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()
	conn := dialServer(t, srv)

	req := coap.Message{Type: coap.Confirmable, Code: coap.CodeGET, MessageID: 1, Token: []byte{1},
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("m")}}}
	if resp := roundTrip(t, conn, req); resp.Code != coap.CodeContent {
		t.Fatalf("GET = %v", resp.Code)
	}
	// The same confirmable request again is answered from the dedup cache.
	if resp := roundTrip(t, conn, req); resp.Code != coap.CodeContent {
		t.Fatalf("duplicate GET = %v", resp.Code)
	}
	payload := []byte("0123456789abcdef0123456789abcdef")
	if resp := roundTrip(t, conn, block1Request(t, 2, []byte{2}, "m", payload[:16], 0, true)); resp.Code != coap.CodeContinue {
		t.Fatalf("block 0 = %v", resp.Code)
	}
	if resp := roundTrip(t, conn, block1Request(t, 3, []byte{2}, "m", payload[16:], 1, false)); resp.Code != coap.CodeChanged {
		t.Fatalf("block 1 = %v", resp.Code)
	}
	if recorder.exchanges.Load() < 3 {
		t.Fatalf("exchanges = %d, want at least 3", recorder.exchanges.Load())
	}
	if recorder.dedups.Load() != 1 {
		t.Fatalf("dedup hits = %d, want 1", recorder.dedups.Load())
	}
	if recorder.blocks.Load() < 2 {
		t.Fatalf("block events = %d, want at least 2", recorder.blocks.Load())
	}
}

func TestClientMetricsAndResponseLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 64)
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	srv.Handle(coap.CodeGET, "/big", func(context.Context, Request) Response {
		return Response{Code: coap.CodeContent, Payload: payload}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	recorder := &metricsRecorder{}
	cli, err := DialConfig(context.Background(), ClientConfig{
		Addr: srv.Addr(), BlockSize: 16, MaxResponseSize: 32, Metrics: recorder,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = cli.Do(ctx, ClientRequest{Code: coap.CodeGET, Path: "/big", Options: []coap.Option{block2Option(t, 0)}})
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("Do = %v, want ErrResponseTooLarge", err)
	}
	if recorder.exchanges.Load() != 1 {
		t.Fatalf("exchanges = %d, want 1", recorder.exchanges.Load())
	}
}
