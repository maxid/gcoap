package gcoap

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

func hasObserve(m coap.Message) (uint32, bool) {
	for _, opt := range m.Options {
		if opt.Number != coap.OptionObserve {
			continue
		}
		var v uint32
		for _, b := range opt.Value {
			v = v<<8 | uint32(b)
		}
		return v, true
	}
	return 0, false
}

func TestObserveNotifyCancel(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	srv.Handle(coap.CodeGET, "/state", func(_ context.Context, _ Request) Response {
		return Response{Code: coap.CodeContent, Payload: []byte("v0")}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop(context.Background())

	addr, _ := net.ResolveUDPAddr("udp", srv.Addr())
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	token := []byte{0x01, 0x02}
	req := coap.Message{
		Type: coap.Confirmable, Code: coap.CodeGET, MessageID: 1, Token: token,
		Options: []coap.Option{
			{Number: coap.OptionObserve, Value: nil},
			{Number: coap.OptionURIPath, Value: []byte("state")},
		},
	}
	raw, _ := req.Marshal()
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1500)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := coap.Parse(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := hasObserve(initial); !ok {
		t.Fatal("initial response missing Observe option")
	}

	if sent := srv.Notify("/state", Response{Code: coap.CodeContent, Payload: []byte("v1")}); sent != 1 {
		t.Fatalf("notify sent = %d", sent)
	}
	n, err = conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	notif, err := coap.Parse(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	if notif.Type != coap.NonConfirmable || string(notif.Payload) != "v1" {
		t.Fatalf("notification = %v %q", notif.Type, notif.Payload)
	}
	if string(notif.Token) != string(token) {
		t.Fatalf("token = %v", notif.Token)
	}
	if seq, ok := hasObserve(notif); !ok || seq == 0 {
		t.Fatalf("notification observe = %d ok=%v", seq, ok)
	}

	// Cancel with Observe=1.
	cancel := coap.Message{
		Type: coap.Confirmable, Code: coap.CodeGET, MessageID: 2, Token: token,
		Options: []coap.Option{
			{Number: coap.OptionObserve, Value: []byte{1}},
			{Number: coap.OptionURIPath, Value: []byte("state")},
		},
	}
	raw, _ = cancel.Marshal()
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(buf); err != nil {
		t.Fatal(err)
	}
	if sent := srv.Notify("/state", Response{Payload: []byte("v2")}); sent != 0 {
		t.Fatalf("notify after cancel = %d", sent)
	}
}
