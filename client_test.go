package gcoap

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

func TestClientGet(t *testing.T) {
	srv := startTestServer(t)
	cli, err := Dial(context.Background(), srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := cli.Do(ctx, ClientRequest{
		Code: coap.CodeGET, Path: "/hello", Query: url.Values{"name": {"client"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Code != coap.CodeContent || string(resp.Payload) != "hello client" {
		t.Fatalf("resp = %v %q", resp.Code, resp.Payload)
	}
}

func TestClientBlock2(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 40)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	srv.Handle(coap.CodeGET, "/big", func(context.Context, Request) Response {
		return Response{Code: coap.CodeContent, Payload: payload}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop(context.Background())
	cli, err := Dial(context.Background(), srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	// Force 16-byte blocks via a Block2 request option.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Do(ctx, ClientRequest{
		Code: coap.CodeGET, Path: "/big",
		Options: []coap.Option{{Number: coap.OptionBlock2, Value: encodeBlock(block{SZX: 0})}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Payload) != string(payload) {
		t.Fatalf("payload = %d bytes, want %d", len(resp.Payload), len(payload))
	}
}
