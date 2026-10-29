package gcoap

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

func dialServer(t *testing.T, srv *Server) *net.UDPConn {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func roundTrip(t *testing.T, conn *net.UDPConn, req coap.Message) coap.Message {
	t.Helper()
	raw, err := req.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
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

func TestBlock2Read(t *testing.T) {
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
	conn := dialServer(t, srv)

	var got []byte
	num := uint32(0)
	for {
		req := coap.Message{
			Type: coap.Confirmable, Code: coap.CodeGET, MessageID: uint16(100 + num),
			Token: []byte{1}, Options: []coap.Option{
				{Number: coap.OptionURIPath, Value: []byte("big")},
				{Number: coap.OptionBlock2, Value: encodeBlock(block{Num: num, SZX: 0})},
			},
		}
		resp := roundTrip(t, conn, req)
		if resp.Code != coap.CodeContent {
			t.Fatalf("code = %v", resp.Code)
		}
		b, ok, err := parseBlock(resp, coap.OptionBlock2)
		if err != nil || !ok {
			t.Fatalf("block2 = %+v ok=%v err=%v", b, ok, err)
		}
		if b.Num != num {
			t.Fatalf("num = %d want %d", b.Num, num)
		}
		got = append(got, resp.Payload...)
		if !b.More {
			break
		}
		num++
	}
	if string(got) != string(payload) {
		t.Fatalf("assembled %d bytes, want %d", len(got), len(payload))
	}
}

func TestBlock1Write(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	storedCh := make(chan []byte, 1)
	srv.Handle(coap.CodePUT, "/upload", func(_ context.Context, req Request) Response {
		storedCh <- append([]byte(nil), req.Payload...)
		return Response{Code: coap.CodeChanged}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop(context.Background())
	conn := dialServer(t, srv)
	payload := []byte("0123456789abcdef0123456789abcdef") // 32 bytes, two 16-byte blocks

	first := coap.Message{
		Type: coap.Confirmable, Code: coap.CodePUT, MessageID: 1, Token: []byte{9},
		Options: []coap.Option{
			{Number: coap.OptionURIPath, Value: []byte("upload")},
			{Number: coap.OptionBlock1, Value: encodeBlock(block{Num: 0, More: true, SZX: 0})},
		},
		Payload: payload[:16],
	}
	resp := roundTrip(t, conn, first)
	if resp.Code != coap.CodeContinue {
		t.Fatalf("first code = %v, want 2.31", resp.Code)
	}
	second := coap.Message{
		Type: coap.Confirmable, Code: coap.CodePUT, MessageID: 2, Token: []byte{9},
		Options: []coap.Option{
			{Number: coap.OptionURIPath, Value: []byte("upload")},
			{Number: coap.OptionBlock1, Value: encodeBlock(block{Num: 1, More: false, SZX: 0})},
		},
		Payload: payload[16:],
	}
	resp = roundTrip(t, conn, second)
	if resp.Code != coap.CodeChanged {
		t.Fatalf("second code = %v", resp.Code)
	}
	select {
	case stored := <-storedCh:
		if string(stored) != string(payload) {
			t.Fatalf("stored %q", stored)
		}
	case <-time.After(time.Second):
		t.Fatal("handler not called")
	}
}
