package gcoap

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

func TestSeparateResponseAfterAckTimeout(t *testing.T) {
	clock := newFakeClock()
	srv, err := New(Config{
		Addr: "127.0.0.1:0", SeparateResponses: true,
		AckTimeout: time.Second, AckRandomFactor: 1, MaxRetransmit: 2,
		ExchangeLifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.params.now = clock.Now
	srv.params.after = clock.After
	srv.dedup.now = clock.Now

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	srv.Handle(coap.CodeGET, "/slow", func(ctx context.Context, req Request) Response {
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return Response{
			Code:    coap.CodeContent,
			Options: []coap.Option{{Number: coap.OptionContentFormat, Value: []byte{0}}},
			Payload: []byte("late-response"),
		}
	})
	startServer(t, srv)
	peer := dialPeer(t, srv)

	req := coap.Message{
		Type: coap.Confirmable, Code: coap.CodeGET, MessageID: 77, Token: []byte{0xde, 0xad},
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("slow")}},
	}
	reqRaw, _ := req.Marshal()
	start := clock.Now()
	if _, err := peer.Write(reqRaw); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	// Advance the injected clock exactly ACK_TIMEOUT: the server must answer
	// with an empty ACK while the handler is still running.
	clock.AdvanceNext(t)
	if elapsed := clock.Now().Sub(start); elapsed != time.Second {
		t.Fatalf("empty ACK fired after %v, want ACK_TIMEOUT 1s", elapsed)
	}
	ackRaw := readDatagram(t, peer)
	ack, err := coap.Parse(ackRaw)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != coap.Acknowledgement || ack.Code != coap.CodeEmpty {
		t.Fatalf("ack = %v/%v, want empty ACK", ack.Type, ack.Code)
	}
	if ack.MessageID != req.MessageID {
		t.Fatalf("ack mid = %d, want %d", ack.MessageID, req.MessageID)
	}
	if len(ack.Token) != 0 {
		t.Fatalf("empty ACK carries token %x", ack.Token)
	}

	// A duplicate request while the response is pending gets the identical
	// empty ACK replayed; the handler is not run a second time.
	if _, err := peer.Write(reqRaw); err != nil {
		t.Fatal(err)
	}
	replay := readDatagram(t, peer)
	if !bytes.Equal(ackRaw, replay) {
		t.Fatalf("duplicate request replay differs:\nack    %x\nreplay %x", ackRaw, replay)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times while pending, want 1", got)
	}

	// The real response is a new confirmable message with the request token.
	close(release)
	responseRaw := readDatagram(t, peer)
	response, err := coap.Parse(responseRaw)
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != coap.Confirmable || response.Code != coap.CodeContent {
		t.Fatalf("separate response = %v/%v, want CON 2.05", response.Type, response.Code)
	}
	if response.MessageID == req.MessageID {
		t.Fatalf("separate response reused request mid %d", response.MessageID)
	}
	if !bytes.Equal(response.Token, req.Token) {
		t.Fatalf("separate response token = %x, want %x", response.Token, req.Token)
	}
	if string(response.Payload) != "late-response" {
		t.Fatalf("separate response payload = %q", response.Payload)
	}
	if len(response.Options) != 1 || response.Options[0].Number != coap.OptionContentFormat {
		t.Fatalf("separate response options = %+v", response.Options)
	}

	// No ACK from the client: the response is retransmitted byte for byte on
	// the exchange schedule.
	clock.AdvanceNext(t)
	retransmission := readDatagram(t, peer)
	if !bytes.Equal(responseRaw, retransmission) {
		t.Fatalf("separate response retransmission differs:\nfirst %x\nagain %x", responseRaw, retransmission)
	}

	// Acknowledge the separate response so the server stops retransmitting.
	responseAck := coap.Message{
		Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: response.MessageID,
	}
	ackRawForResponse, _ := responseAck.Marshal()
	if _, err := peer.Write(ackRawForResponse); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "separate response exchange to stop", func() bool {
		return srv.outboundPending() == 0
	})

	// A duplicate after the response was delivered still replays the empty ACK
	// and never turns it into the cached final answer.
	if _, err := peer.Write(reqRaw); err != nil {
		t.Fatal(err)
	}
	lateReplay := readDatagram(t, peer)
	if !bytes.Equal(ackRaw, lateReplay) {
		t.Fatalf("late duplicate replay differs:\nack    %x\nreplay %x", ackRaw, lateReplay)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times after the duplicate, want 1", got)
	}
}

func TestClientReceivesSeparateResponseAndAcksIt(t *testing.T) {
	clock := newFakeClock()
	srv, err := New(Config{
		Addr:       "127.0.0.1:0",
		AckTimeout: time.Second, AckRandomFactor: 1, MaxRetransmit: 2,
		ExchangeLifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.params.now = clock.Now
	srv.params.after = clock.After
	srv.dedup.now = clock.Now

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	srv.Handle(coap.CodeGET, "/slow", func(context.Context, Request) Response {
		entered <- struct{}{}
		<-release
		return Response{Code: coap.CodeContent, Payload: []byte("client-separate")}
	})
	startServer(t, srv)

	cli, err := DialConfig(context.Background(), ClientConfig{
		Addr: srv.Addr(), AckTimeout: time.Second, AckRandomFactor: 1,
		MaxRetransmit: 1, ExchangeLifetime: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type result struct {
		resp ClientResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := cli.Do(ctx, ClientRequest{Code: coap.CodeGET, Path: "/slow"})
		done <- result{resp: resp, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	clock.AdvanceNext(t) // empty ACK to the client
	close(release)
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.resp.Code != coap.CodeContent || string(got.resp.Payload) != "client-separate" {
			t.Fatalf("client response = %v %q", got.resp.Code, got.resp.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client did not receive the separate response")
	}
	// The client ACKed the separate CON response, so no server exchange stays
	// open waiting for it.
	waitFor(t, "client ACK of the separate response", func() bool {
		return srv.outboundPending() == 0
	})
}

func TestRespondLaterHandler(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	responded := make(chan error, 1)
	var calls atomic.Int32
	srv.Handle(coap.CodeGET, "/later", func(ctx context.Context, req Request) Response {
		calls.Add(1)
		respond := req.RespondLater()
		go func() {
			<-release
			responded <- respond(Response{Code: coap.CodeContent, Payload: []byte("later")})
		}()
		return Response{}
	})
	startServer(t, srv)
	peer := dialPeer(t, srv)

	req := coap.Message{
		Type: coap.Confirmable, Code: coap.CodeGET, MessageID: 90, Token: []byte{0x90},
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("later")}},
	}
	raw, _ := req.Marshal()
	if _, err := peer.Write(raw); err != nil {
		t.Fatal(err)
	}
	ack, err := coap.Parse(readDatagram(t, peer))
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != coap.Acknowledgement || ack.Code != coap.CodeEmpty || ack.MessageID != req.MessageID {
		t.Fatalf("RespondLater ACK = %v/%v/%d", ack.Type, ack.Code, ack.MessageID)
	}

	close(release)
	response, err := coap.Parse(readDatagram(t, peer))
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != coap.Confirmable || response.Code != coap.CodeContent ||
		string(response.Payload) != "later" || response.MessageID == req.MessageID {
		t.Fatalf("deferred response = %v/%v/%q mid=%d", response.Type, response.Code, response.Payload, response.MessageID)
	}
	ack = coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: response.MessageID}
	ackRaw, _ := ack.Marshal()
	if _, err := peer.Write(ackRaw); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-responded:
		if err != nil {
			t.Fatalf("respond: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("responder did not finish")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
}
