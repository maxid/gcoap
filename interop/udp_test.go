package interop

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/maxid/gcoap"
	"github.com/maxid/gnety/codec/coap"
	plgdMessage "github.com/plgd-dev/go-coap/v3/message"
	plgdCodes "github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/mux"
	plgdNet "github.com/plgd-dev/go-coap/v3/net"
	"github.com/plgd-dev/go-coap/v3/net/blockwise"
	"github.com/plgd-dev/go-coap/v3/options"
	"github.com/plgd-dev/go-coap/v3/udp"
)

const largeSize = 3 * 1024

func largeBody(seed byte) []byte {
	out := make([]byte, largeSize)
	for i := range out {
		out[i] = seed + byte(i%26)
	}
	return out
}

// startPlgdUDPServer starts the third-party server on an ephemeral loopback
// port and returns its address.
func startPlgdUDPServer(t *testing.T, router mux.Handler) string {
	t.Helper()
	ln, err := plgdNet.NewListenUDP("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := udp.NewServer(
		options.WithMux(router),
		options.WithBlockwise(true, blockwise.SZX1024, 5*time.Second),
	)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	})
	return ln.LocalAddr().String()
}

// writeObserve answers a GET Observe=0 with an Observe option, which RFC 7641
// requires on the registration response.
func writeObserve(t *testing.T, w mux.ResponseWriter, r *mux.Message, text string) {
	t.Helper()
	m := w.Conn().AcquireMessage(r.Context())
	defer w.Conn().ReleaseMessage(m)
	m.SetCode(plgdCodes.Content)
	m.SetToken(r.Token())
	m.SetBody(bytes.NewReader([]byte(text)))
	m.SetContentFormat(plgdMessage.TextPlain)
	m.SetObserve(0)
	if err := w.Conn().WriteMessage(m); err != nil {
		t.Errorf("write observe response: %v", err)
	}
}

// TestOurClientToPlgdServer exercises this library's client against an
// independent CoAP server: a plain GET, a Block1 upload, a Block2 download and
// an observation.
func TestOurClientToPlgdServer(t *testing.T) {
	uploaded := make(chan []byte, 4)
	observers := make(chan struct{}, 1)
	router := mux.NewRouter()
	_ = router.Handle("/hello", mux.HandlerFunc(func(w mux.ResponseWriter, _ *mux.Message) {
		_ = w.SetResponse(plgdCodes.Content, plgdMessage.TextPlain, bytes.NewReader([]byte("hello")))
	}))
	_ = router.Handle("/upload", mux.HandlerFunc(func(w mux.ResponseWriter, r *mux.Message) {
		body, err := io.ReadAll(r.Body())
		if err != nil {
			_ = w.SetResponse(plgdCodes.BadRequest, plgdMessage.TextPlain, nil)
			return
		}
		select {
		case uploaded <- body:
		default:
		}
		_ = w.SetResponse(plgdCodes.Changed, plgdMessage.TextPlain, bytes.NewReader([]byte("ok")))
	}))
	body := largeBody('a')
	_ = router.Handle("/big", mux.HandlerFunc(func(w mux.ResponseWriter, _ *mux.Message) {
		_ = w.SetResponse(plgdCodes.Content, plgdMessage.TextPlain, bytes.NewReader(body))
	}))
	_ = router.Handle("/obs", mux.HandlerFunc(func(w mux.ResponseWriter, r *mux.Message) {
		if obs, err := r.Options().Observe(); err == nil && obs == 0 {
			select {
			case observers <- struct{}{}:
			default:
			}
			writeObserve(t, w, r, "v0")
			return
		}
		_ = w.SetResponse(plgdCodes.Content, plgdMessage.TextPlain, bytes.NewReader([]byte("v0")))
	}))
	addr := startPlgdUDPServer(t, router)

	cli, err := gcoap.Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = cli.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := cli.Do(ctx, gcoap.ClientRequest{Code: coap.CodeGET, Path: "/hello"})
	if err != nil || string(resp.Payload) != "hello" {
		t.Fatalf("GET = %q, %v", resp.Payload, err)
	}

	payload := largeBody('A')
	resp, err = cli.Do(ctx, gcoap.ClientRequest{Code: coap.CodePOST, Path: "/upload", Payload: payload})
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	if resp.Code.Class() != 2 {
		t.Fatalf("POST code = %v", resp.Code)
	}
	select {
	case got := <-uploaded:
		if !bytes.Equal(got, payload) {
			t.Fatalf("third-party server received %d bytes, want %d", len(got), len(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("third-party server never received the uploaded body")
	}

	// Ask for Block2 explicitly so the server has to slice the response.
	block2, err := coap.Block{Num: 0, SizeExp: 2}.Encode() // 64-byte blocks
	if err != nil {
		t.Fatal(err)
	}
	resp, err = cli.Do(ctx, gcoap.ClientRequest{
		Code: coap.CodeGET, Path: "/big",
		Options: []coap.Option{{Number: coap.OptionBlock2, Value: block2}},
	})
	if err != nil {
		t.Fatalf("Block2 GET: %v", err)
	}
	if !bytes.Equal(resp.Payload, body) {
		t.Fatalf("Block2 GET returned %d bytes, want %d", len(resp.Payload), len(body))
	}

	notifications := make(chan string, 8)
	obs, _, err := cli.Observe(ctx, gcoap.ClientRequest{Code: coap.CodeGET, Path: "/obs"}, func(r gcoap.ClientResponse) {
		select {
		case notifications <- string(r.Payload):
		default:
		}
	})
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	defer func() {
		cancelCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = obs.Cancel(cancelCtx)
	}()
	select {
	case <-observers:
	case <-time.After(3 * time.Second):
		t.Fatal("third-party server never saw the observe registration")
	}
}

// TestPlgdClientToOurServer exercises an independent CoAP client against this
// library's server, including Block1, Block2 and Observe.
func TestPlgdClientToOurServer(t *testing.T) {
	srv, err := gcoap.New(gcoap.Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	body := largeBody('b')
	uploaded := make(chan []byte, 4)
	observed := make(chan struct{}, 1)
	srv.Handle(coap.CodeGET, "/hello", func(context.Context, gcoap.Request) gcoap.Response {
		return gcoap.Response{Code: coap.CodeContent, Payload: []byte("hello")}
	})
	srv.Handle(coap.CodePOST, "/upload", func(_ context.Context, req gcoap.Request) gcoap.Response {
		select {
		case uploaded <- append([]byte(nil), req.Payload...):
		default:
		}
		return gcoap.Response{Code: coap.CodeChanged, Payload: []byte("ok")}
	})
	srv.Handle(coap.CodeGET, "/big", func(context.Context, gcoap.Request) gcoap.Response {
		return gcoap.Response{Code: coap.CodeContent, Payload: body}
	})
	srv.Handle(coap.CodeGET, "/obs", func(context.Context, gcoap.Request) gcoap.Response {
		select {
		case observed <- struct{}{}:
		default:
		}
		return gcoap.Response{Code: coap.CodeContent, Payload: []byte("v0")}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	co, err := udp.Dial(srv.Addr(), options.WithBlockwise(true, blockwise.SZX1024, 5*time.Second))
	if err != nil {
		t.Fatalf("third-party dial: %v", err)
	}
	defer func() { _ = co.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := co.Get(ctx, "/hello")
	if err != nil {
		t.Fatalf("third-party GET: %v", err)
	}
	hello, err := resp.ReadBody()
	if err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if text := strings.TrimSpace(string(hello)); text != "hello" {
		t.Fatalf("third-party GET = %q", text)
	}

	payload := largeBody('B')
	resp, err = co.Post(ctx, "/upload", plgdMessage.TextPlain, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("third-party POST: %v", err)
	}
	if resp.Code() != plgdCodes.Changed {
		t.Fatalf("third-party POST code = %v", resp.Code())
	}
	select {
	case got := <-uploaded:
		if !bytes.Equal(got, payload) {
			t.Fatalf("our server received %d bytes, want %d", len(got), len(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("our server never received the uploaded body")
	}

	resp, err = co.Get(ctx, "/big")
	if err != nil {
		t.Fatalf("third-party Block2 GET: %v", err)
	}
	if got := resp.Body(); got != nil {
		data, readErr := io.ReadAll(got)
		if readErr != nil {
			t.Fatalf("read body: %v", readErr)
		}
		if !bytes.Equal(data, body) {
			t.Fatalf("third-party received %d bytes, want %d", len(data), len(body))
		}
	} else {
		t.Fatal("third-party Block2 GET returned no body")
	}

	notifications := make(chan string, 8)
	obs, err := co.Observe(ctx, "/obs", func(m *pool.Message) {
		data, readErr := m.ReadBody()
		if readErr != nil {
			return
		}
		select {
		case notifications <- string(data):
		default:
		}
	})
	if err != nil {
		t.Fatalf("third-party Observe: %v", err)
	}
	defer func() {
		cancelCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = obs.Cancel(cancelCtx)
	}()
	select {
	case <-observed:
	case <-time.After(3 * time.Second):
		t.Fatal("our server never saw the observe registration")
	}
	if sent := srv.Notify("/obs", gcoap.Response{Code: coap.CodeContent, Payload: []byte("v1")}); sent != 1 {
		t.Fatalf("Notify reached %d observers, want 1", sent)
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case got := <-notifications:
			// The registration response may be delivered as the first callback.
			if got == "v1" {
				return
			}
		case <-deadline:
			t.Fatal("third-party client never received the v1 notification")
		}
	}
}
