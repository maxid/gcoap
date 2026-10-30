package gcoap

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// notificationServer starts a server whose notification exchange runs on the
// supplied manual clock. The caller gets a UDP peer already registered as an
// observer of /state.
func notificationServer(t *testing.T, clock *fakeClock, cfg Config) (*Server, *net.UDPConn, coap.Message) {
	t.Helper()
	cfg.Addr = "127.0.0.1:0"
	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = 100 * time.Millisecond
	}
	if cfg.AckRandomFactor == 0 {
		cfg.AckRandomFactor = 1
	}
	if cfg.ExchangeLifetime == 0 {
		cfg.ExchangeLifetime = 10 * time.Second
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv.params.now = clock.Now
	srv.params.after = clock.After
	srv.dedup.now = clock.Now
	srv.Handle(coap.CodeGET, "/state", func(context.Context, Request) Response {
		return Response{Code: coap.CodeContent, Payload: []byte("v0")}
	})
	startServer(t, srv)
	peer := dialPeer(t, srv)

	token := []byte{0x5a, 0x01}
	req := coap.Message{
		Type: coap.Confirmable, Code: coap.CodeGET, MessageID: 1, Token: token,
		Options: []coap.Option{
			{Number: coap.OptionObserve, Value: nil},
			{Number: coap.OptionURIPath, Value: []byte("state")},
		},
	}
	raw := sendRequest(t, peer, req)
	initial, err := coap.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := hasObserve(initial); !ok {
		t.Fatal("initial response missing Observe option")
	}
	return srv, peer, initial
}

func readDatagram(t *testing.T, conn *net.UDPConn) []byte {
	t.Helper()
	buf := make([]byte, 1500)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), buf[:n]...)
}

func TestConfirmableNotificationRetransmitsUntilAck(t *testing.T) {
	clock := newFakeClock()
	srv, peer, _ := notificationServer(t, clock, Config{
		ConfirmableNotifications: true, MaxRetransmit: 3,
	})

	if sent := srv.Notify("/state", Response{Payload: []byte("v1")}); sent != 1 {
		t.Fatalf("Notify sent = %d, want 1", sent)
	}
	firstRaw := readDatagram(t, peer)
	first, err := coap.Parse(firstRaw)
	if err != nil {
		t.Fatal(err)
	}
	if first.Type != coap.Confirmable || string(first.Payload) != "v1" {
		t.Fatalf("notification = %v %q, want CON v1", first.Type, first.Payload)
	}
	if seq, ok := hasObserve(first); !ok || seq == 0 {
		t.Fatalf("notification observe = %d ok=%v", seq, ok)
	}

	// The retransmission is driven entirely by the injected clock. Wait for
	// its timer (the cancelled registration timer from the initial GET is still
	// in the fake clock).
	waitFor(t, "notification retransmission timer", func() bool {
		return clock.PendingTimers() >= 2
	})
	clock.AdvanceNext(t)
	secondRaw := readDatagram(t, peer)
	second, err := coap.Parse(secondRaw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstRaw, secondRaw) {
		t.Fatalf("retransmission is not byte identical:\nfirst  %x\nsecond %x", firstRaw, secondRaw)
	}
	if second.MessageID != first.MessageID {
		t.Fatalf("retransmission mid = %d, want %d", second.MessageID, first.MessageID)
	}

	ack := coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: first.MessageID}
	ackRaw, _ := ack.Marshal()
	if _, err := peer.Write(ackRaw); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "notification exchange to stop", func() bool {
		srv.obsMu.Lock()
		defer srv.obsMu.Unlock()
		list := srv.observers["/state"]
		return len(list) == 1 && !list[0].inflight && list[0].pending == nil
	})
	if got := srv.outboundPending(); got != 0 {
		t.Fatalf("outbound exchanges = %d after ACK, want 0", got)
	}

	// The observation is still alive and a later notification starts a fresh
	// exchange with a new message id.
	if sent := srv.Notify("/state", Response{Payload: []byte("v2")}); sent != 1 {
		t.Fatalf("Notify after ACK sent = %d, want 1", sent)
	}
	next := readDatagram(t, peer)
	nextMsg, err := coap.Parse(next)
	if err != nil {
		t.Fatal(err)
	}
	if nextMsg.MessageID == first.MessageID {
		t.Fatalf("new notification reused message id %d", nextMsg.MessageID)
	}
	if string(nextMsg.Payload) != "v2" {
		t.Fatalf("new notification payload = %q", nextMsg.Payload)
	}
}

func TestConfirmableNotificationResetDropsObservation(t *testing.T) {
	clock := newFakeClock()
	srv, peer, _ := notificationServer(t, clock, Config{
		ConfirmableNotifications: true, MaxRetransmit: 3,
	})

	if sent := srv.Notify("/state", Response{Payload: []byte("v1")}); sent != 1 {
		t.Fatalf("Notify sent = %d, want 1", sent)
	}
	notif, err := coap.Parse(readDatagram(t, peer))
	if err != nil {
		t.Fatal(err)
	}
	reset := coap.Message{Type: coap.Reset, Code: coap.CodeEmpty, MessageID: notif.MessageID}
	raw, _ := reset.Marshal()
	if _, err := peer.Write(raw); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "observation to be removed on RST", func() bool {
		srv.obsMu.Lock()
		defer srv.obsMu.Unlock()
		return len(srv.observers["/state"]) == 0
	})
	if got := srv.outboundPending(); got != 0 {
		t.Fatalf("outbound exchanges = %d after RST, want 0", got)
	}
	if sent := srv.Notify("/state", Response{Payload: []byte("v2")}); sent != 0 {
		t.Fatalf("Notify after RST sent = %d, want 0", sent)
	}
}

func TestConfirmableNotificationGivesUpAtLimit(t *testing.T) {
	clock := newFakeClock()
	srv, peer, _ := notificationServer(t, clock, Config{
		ConfirmableNotifications: true, AckTimeout: 10 * time.Millisecond,
		MaxRetransmit: 2, ExchangeLifetime: 10 * time.Second,
	})

	if sent := srv.Notify("/state", Response{Payload: []byte("v1")}); sent != 1 {
		t.Fatalf("Notify sent = %d, want 1", sent)
	}
	first := readDatagram(t, peer)
	waitFor(t, "notification retransmission timer", func() bool {
		return clock.PendingTimers() >= 2
	})
	// MAX_RETRANSMIT 2 means the original plus two retransmissions.
	for i := 0; i < 2; i++ {
		clock.AdvanceNext(t)
		next := readDatagram(t, peer)
		if !bytes.Equal(first, next) {
			t.Fatalf("retransmission %d differs from the first transmission", i+1)
		}
	}
	// The final response window expires without an ACK; RFC 7641 removes the
	// observation after the last retransmission times out.
	clock.AdvanceNext(t)
	waitFor(t, "observation to be removed after the retransmission limit", func() bool {
		srv.obsMu.Lock()
		defer srv.obsMu.Unlock()
		return len(srv.observers["/state"]) == 0
	})
	if got := srv.outboundPending(); got != 0 {
		t.Fatalf("outbound exchanges = %d after giving up, want 0", got)
	}
	if sent := srv.Notify("/state", Response{Payload: []byte("v2")}); sent != 0 {
		t.Fatalf("Notify after giving up sent = %d, want 0", sent)
	}
}

func TestConfirmableNotificationInFlightCapCoalesces(t *testing.T) {
	clock := newFakeClock()
	srv, peer, _ := notificationServer(t, clock, Config{
		ConfirmableNotifications: true, MaxRetransmit: 3,
	})

	if sent := srv.Notify("/state", Response{Payload: []byte("v1")}); sent != 1 {
		t.Fatalf("Notify sent = %d, want 1", sent)
	}
	first, err := coap.Parse(readDatagram(t, peer))
	if err != nil {
		t.Fatal(err)
	}

	// Flood notifications while the first one is unacknowledged. The cap keeps
	// one in-flight message plus one coalesced latest message, no matter how
	// many Notify calls arrive.
	for i := 0; i < 50; i++ {
		if sent := srv.Notify("/state", Response{Payload: []byte("latest")}); sent != 0 {
			t.Fatalf("queued Notify sent = %d, want 0", sent)
		}
	}
	srv.obsMu.Lock()
	if len(srv.observers["/state"]) != 1 {
		srv.obsMu.Unlock()
		t.Fatal("observer list changed")
	}
	o := srv.observers["/state"][0]
	if !o.inflight || o.pending == nil {
		srv.obsMu.Unlock()
		t.Fatalf("notification state = inflight %v pending %v, want one of each", o.inflight, o.pending != nil)
	}
	if string(o.pending.Payload) != "latest" {
		srv.obsMu.Unlock()
		t.Fatalf("coalesced payload = %q, want latest", o.pending.Payload)
	}
	srv.obsMu.Unlock()

	// Acknowledging the outstanding notification releases the coalesced one
	// without another Notify call.
	ack := coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: first.MessageID}
	raw, _ := ack.Marshal()
	if _, err := peer.Write(raw); err != nil {
		t.Fatal(err)
	}
	second, err := coap.Parse(readDatagram(t, peer))
	if err != nil {
		t.Fatal(err)
	}
	if second.Type != coap.Confirmable || string(second.Payload) != "latest" {
		t.Fatalf("coalesced notification = %v %q", second.Type, second.Payload)
	}
	if second.MessageID == first.MessageID {
		t.Fatal("coalesced notification reused the outstanding message id")
	}
	if seq, ok := hasObserve(second); !ok || seq <= 1 {
		t.Fatalf("coalesced observe = %d ok=%v, want the newest sequence", seq, ok)
	}

	ack = coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: second.MessageID}
	raw, _ = ack.Marshal()
	if _, err := peer.Write(raw); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "coalesced notification to finish", func() bool {
		srv.obsMu.Lock()
		defer srv.obsMu.Unlock()
		list := srv.observers["/state"]
		return len(list) == 1 && !list[0].inflight && list[0].pending == nil
	})
}
