package gcoap

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// observePeer answers the warm-up, the registration with Observe=2 and then every
// message the test tells it to send.
func TestServerObserveTo(t *testing.T) {
	f := faultTestServer(t, Config{AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	var registrations atomic.Int32
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		switch string(req.Token) {
		case "warm":
			return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID, Token: req.Token}}
		}
		if _, has := req.Observe(); has {
			registrations.Add(1)
			return []coap.Message{{
				Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID,
				Token:   append([]byte(nil), req.Token...),
				Options: []coap.Option{{Number: coap.OptionObserve, Value: []byte{0, 0, 2}}},
				Payload: []byte("21.5"),
			}}
		}
		return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID, Token: req.Token}}
	})
	warmSession(t, f.Server, peer)

	notes := make(chan string, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	obs, initial, err := f.ObserveTo(ctx, peer.Addr(), ClientRequest{Code: coap.CodeGET, Path: "/3303/0/5700"}, func(resp ClientResponse) {
		select {
		case notes <- string(resp.Payload):
		default:
		}
	})
	if err != nil {
		t.Fatalf("ObserveTo: %v", err)
	}
	if string(initial.Payload) != "21.5" {
		t.Fatalf("initial = %q, want 21.5", initial.Payload)
	}
	if got := registrations.Load(); got != 1 {
		t.Fatalf("registrations = %d, want 1", got)
	}

	// A notification from the peer reaches the callback.
	peer.sendTo(t, f.Addr(), coap.Message{
		Type: coap.NonConfirmable, Code: coap.CodeContent, MessageID: 0x5001,
		Token: []byte(obs.token), Options: []coap.Option{{Number: coap.OptionObserve, Value: []byte{0, 0, 3}}},
		Payload: []byte("22.0"),
	})
	select {
	case got := <-notes:
		if got != "22.0" {
			t.Fatalf("notification = %q, want 22.0", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no notification arrived")
	}

	if err := obs.Cancel(ctx); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	select {
	case <-obs.Done():
	case <-time.After(time.Second):
		t.Fatal("Done was not closed after Cancel")
	}
	if err := obs.Cancel(ctx); err != nil {
		t.Fatalf("second Cancel: %v", err)
	}
}

// TestServerObserveToRejected reports a non-2.xx registration answer and leaves
// nothing registered.
func TestServerObserveToRejected(t *testing.T) {
	f := faultTestServer(t, Config{AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		if string(req.Token) == "warm" {
			return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID, Token: req.Token}}
		}
		return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeNotFound, MessageID: req.MessageID, Token: req.Token}}
	})
	warmSession(t, f.Server, peer)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	obs, _, err := f.ObserveTo(ctx, peer.Addr(), ClientRequest{Code: coap.CodeGET, Path: "/3303/0/5700"}, func(ClientResponse) {})
	if !errors.Is(err, ErrObserveRejected) {
		t.Fatalf("ObserveTo = %v, want ErrObserveRejected", err)
	}
	if obs != nil {
		t.Fatal("a rejected registration returned an observation")
	}
	if pending := f.serverRequestCount(); pending != 0 {
		t.Fatalf("pending requests = %d after a rejection, want 0", pending)
	}
}

// TestServerObserveToEndsOnErrorNotification ends an observation when the peer
// sends a final error notification.
func TestServerObserveToEndsOnErrorNotification(t *testing.T) {
	f := faultTestServer(t, Config{AckTimeout: 100 * time.Millisecond, MaxRetransmit: 2})
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		if string(req.Token) == "warm" {
			return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID, Token: req.Token}}
		}
		return []coap.Message{{
			Type: coap.Acknowledgement, Code: coap.CodeContent, MessageID: req.MessageID,
			Token: append([]byte(nil), req.Token...), Options: []coap.Option{{Number: coap.OptionObserve, Value: []byte{0, 0, 1}}},
		}}
	})
	warmSession(t, f.Server, peer)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	obs, _, err := f.ObserveTo(ctx, peer.Addr(), ClientRequest{Code: coap.CodeGET, Path: "/3303/0/5700"}, func(ClientResponse) {})
	if err != nil {
		t.Fatalf("ObserveTo: %v", err)
	}
	peer.sendTo(t, f.Addr(), coap.Message{
		Type: coap.NonConfirmable, Code: coap.CodeNotFound, MessageID: 0x5002, Token: []byte(obs.token),
	})
	select {
	case <-obs.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("an error notification did not end the observation")
	}
}
