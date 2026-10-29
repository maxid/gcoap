package gcoap

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

func startTestServer(t *testing.T) *Server {
	t.Helper()
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	srv.Handle(coap.CodeGET, "/hello", func(_ context.Context, req Request) Response {
		return Response{Code: coap.CodeContent, Payload: []byte("hello " + req.Query.Get("name"))}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	return srv
}

func exchange(t *testing.T, srv *Server, req coap.Message) coap.Message {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	raw, err := req.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1500)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := coap.Parse(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestGET(t *testing.T) {
	srv := startTestServer(t)
	req := coap.Message{
		Type: coap.Confirmable, Code: coap.CodeGET, MessageID: 7,
		Token: []byte{1, 2},
		Options: []coap.Option{
			{Number: coap.OptionURIPath, Value: []byte("hello")},
			{Number: coap.OptionURIQuery, Value: []byte("name=coap")},
		},
	}
	resp := exchange(t, srv, req)
	if resp.Type != coap.Acknowledgement || resp.Code != coap.CodeContent {
		t.Fatalf("response type/code = %v/%v", resp.Type, resp.Code)
	}
	if resp.MessageID != req.MessageID || string(resp.Token) != string(req.Token) {
		t.Fatalf("mid/token mismatch: %d %v", resp.MessageID, resp.Token)
	}
	if string(resp.Payload) != "hello coap" {
		t.Fatalf("payload = %q", resp.Payload)
	}
}

func TestNotFound(t *testing.T) {
	srv := startTestServer(t)
	req := coap.Message{Type: coap.Confirmable, Code: coap.CodeGET, MessageID: 8}
	resp := exchange(t, srv, req)
	if resp.Code != coap.CodeNotFound {
		t.Fatalf("code = %v", resp.Code)
	}
}

func TestNonConfirmable(t *testing.T) {
	srv := startTestServer(t)
	req := coap.Message{Type: coap.NonConfirmable, Code: coap.CodeGET, MessageID: 9,
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("hello")}}}
	resp := exchange(t, srv, req)
	if resp.Type != coap.NonConfirmable || resp.Code != coap.CodeContent {
		t.Fatalf("response = %v/%v", resp.Type, resp.Code)
	}
}

func TestPrefixRoute(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	srv.HandlePrefix(coap.CodePOST, "/rd", func(_ context.Context, req Request) Response {
		return Response{Code: coap.CodeCreated, Payload: []byte(req.Path)}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop(context.Background())
	req := coap.Message{Type: coap.Confirmable, Code: coap.CodePOST, MessageID: 1,
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("rd")}, {Number: coap.OptionURIPath, Value: []byte("abc")}}}
	resp := exchange(t, srv, req)
	if resp.Code != coap.CodeCreated || string(resp.Payload) != "/rd/abc" {
		t.Fatalf("resp = %v %q", resp.Code, resp.Payload)
	}
}
