package gcoap

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// TestClientConcurrentDo pins the concurrency contract: one Client serves many
// simultaneous requests, each response is routed to its own caller by token, and
// no response is delivered to the wrong request.
func TestClientConcurrentDo(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	srv.HandlePrefix(coap.CodeGET, "/echo", func(_ context.Context, req Request) Response {
		id := req.Query.Get("id")
		n, _ := strconv.Atoi(id)
		// Vary the latency so responses arrive out of order.
		time.Sleep(time.Duration(n%7) * time.Millisecond)
		return Response{Code: coap.CodeContent, Payload: []byte(id)}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	cli, err := Dial(context.Background(), srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	const callers = 32
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			want := strconv.Itoa(i)
			resp, err := cli.Do(ctx, ClientRequest{
				Code:  coap.CodeGET,
				Path:  "/echo",
				Query: url.Values{"id": {want}},
			})
			if err != nil {
				errs <- fmt.Errorf("caller %s: %w", want, err)
				return
			}
			if got := string(resp.Payload); got != want {
				errs <- fmt.Errorf("caller %s received %q", want, got)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestClientCancellationIsIsolated checks that cancelling one request does not
// disturb the others sharing the client.
func TestClientCancellationIsIsolated(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	srv.HandlePrefix(coap.CodeGET, "/slow", func(ctx context.Context, req Request) Response {
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
		return Response{Code: coap.CodeContent, Payload: []byte(req.Query.Get("id"))}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	cli, err := Dial(context.Background(), srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	cancelCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = cli.Do(cancelCtx, ClientRequest{Code: coap.CodeGET, Path: "/slow", Query: url.Values{"id": {"cancelled"}}})
	if err == nil {
		t.Fatal("the cancelled call succeeded")
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("cancellation took %s", elapsed)
	}

	ctx, cancelAll := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAll()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			want := "ok-" + strconv.Itoa(i)
			resp, err := cli.Do(ctx, ClientRequest{Code: coap.CodeGET, Path: "/slow", Query: url.Values{"id": {want}}})
			if err != nil {
				t.Errorf("caller %s: %v", want, err)
				return
			}
			if string(resp.Payload) != want {
				t.Errorf("caller %s received %q", want, resp.Payload)
			}
		}(i)
	}
	wg.Wait()
}

// TestClientConcurrentBlock1Uploads runs several block-wise uploads at once and
// checks that every body arrives intact on its own exchange.
func TestClientConcurrentBlock1Uploads(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	received := make(map[string]string)
	srv.Handle(coap.CodePUT, "/upload", func(_ context.Context, req Request) Response {
		mu.Lock()
		received[string(req.Message.Token)] = string(req.Payload)
		mu.Unlock()
		return Response{Code: coap.CodeChanged}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	cli, err := DialConfig(context.Background(), ClientConfig{Addr: srv.Addr(), BlockSize: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()

	const callers = 8
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	body := func(i int) string {
		s := strconv.Itoa(i) + ":"
		for len(s) < 64 {
			s += "abcdefgh"
		}
		return s[:64]
	}
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(body(i))
			if _, err := cli.Do(ctx, ClientRequest{Code: coap.CodePUT, Path: "/upload", Payload: payload}); err != nil {
				t.Errorf("upload %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(received) != callers {
		t.Fatalf("server stored %d bodies, want %d", len(received), callers)
	}
	seen := make(map[string]bool, callers)
	for _, text := range received {
		seen[text] = true
	}
	for i := 0; i < callers; i++ {
		if !seen[body(i)] {
			t.Errorf("body %d did not arrive intact", i)
		}
	}
}
