package gcoap

import (
	"bytes"
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

// block2Option builds a Block2 request option asking for one 16-byte block.
func block2Option(t *testing.T, num uint32) coap.Option {
	t.Helper()
	value, err := coap.Block{Num: num, SizeExp: 0}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return coap.Option{Number: coap.OptionBlock2, Value: value}
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
				block2Option(t, num),
			},
		}
		resp := roundTrip(t, conn, req)
		if resp.Code != coap.CodeContent {
			t.Fatalf("code = %v", resp.Code)
		}
		b, ok := resp.Block2()
		if !ok {
			t.Fatalf("block2 missing from %+v", resp.Options)
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
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("upload")}},
		Payload: payload[:16],
	}
	if err := first.SetBlock1(coap.Block{Num: 0, More: true}); err != nil {
		t.Fatal(err)
	}
	resp := roundTrip(t, conn, first)
	if resp.Code != coap.CodeContinue {
		t.Fatalf("first code = %v, want 2.31", resp.Code)
	}
	second := coap.Message{
		Type: coap.Confirmable, Code: coap.CodePUT, MessageID: 2, Token: []byte{9},
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("upload")}},
		Payload: payload[16:],
	}
	if err := second.SetBlock1(coap.Block{Num: 1}); err != nil {
		t.Fatal(err)
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

// TestBlockOptionCompactForm pins the wire form gcoap emits for block options:
// RFC 7959 allows the value as a uint of 0 to 3 bytes and gcoap keeps the
// shortest form, so a small block number stays on one byte.
func TestBlockOptionCompactForm(t *testing.T) {
	cases := []struct {
		block coap.Block
		want  []byte
	}{
		{coap.Block{}, []byte{0x00}},
		{coap.Block{More: true}, []byte{0x08}},
		{coap.Block{Num: 15, SizeExp: 6}, []byte{0xf6}},
		{coap.Block{Num: 16}, []byte{0x01, 0x00}},
		{coap.Block{Num: 1 << 12, More: true}, []byte{0x01, 0x00, 0x08}},
		{coap.Block{Num: 0x0fffff, SizeExp: 6, More: true}, []byte{0xff, 0xff, 0xfe}},
	}
	for _, c := range cases {
		got, err := blockValue(c.block)
		if err != nil {
			t.Fatalf("blockValue(%+v): %v", c.block, err)
		}
		if !bytes.Equal(got, c.want) {
			t.Fatalf("blockValue(%+v) = %x, want %x", c.block, got, c.want)
		}
	}
	if _, err := blockValue(coap.Block{SizeExp: 7}); err == nil {
		t.Fatal("blockValue accepted the reserved size exponent 7")
	}
	if _, err := blockValue(coap.Block{Num: 1 << 20}); err == nil {
		t.Fatal("blockValue accepted a block number above 20 bits")
	}
}
