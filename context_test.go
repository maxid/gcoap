package gcoap

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

func TestHandlerContextCancelledWhenExchangeCompletes(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	srv.Handle(coap.CodePOST, "/charge", func(ctx context.Context, _ Request) Response {
		if ctx == context.Background() {
			t.Error("handler got context.Background()")
		}
		go func() {
			<-ctx.Done()
			finished <- ctx.Err()
		}()
		return Response{Code: coap.CodeChanged}
	})
	startServer(t, srv)
	peer := dialPeer(t, srv)

	sendRequest(t, peer, coap.Message{
		Type: coap.Confirmable, Code: coap.CodePOST, MessageID: 21, Token: []byte{5},
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("charge")}},
	})

	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler context was not cancelled when the exchange completed")
	}
}

func TestHandlerContextCancelledOnStop(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	finished := make(chan error, 1)
	var once sync.Once
	srv.Handle(coap.CodeGET, "/slow", func(ctx context.Context, _ Request) Response {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		finished <- ctx.Err()
		return Response{Code: coap.CodeServiceUnavailable}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = srv.Stop(context.Background())
		}
	})
	peer := dialPeer(t, srv)
	raw, err := (coap.Message{
		Type: coap.Confirmable, Code: coap.CodeGET, MessageID: 31, Token: []byte{6},
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("slow")}},
	}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Write(raw); err != nil {
		t.Fatal(err)
	}

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	stopped = true

	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler context was not cancelled when the server stopped")
	}
}
