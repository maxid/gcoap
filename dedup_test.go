package gcoap

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

// startServer starts srv on its loopback address and stops it when the test
// ends.
func startServer(t *testing.T, srv *Server) {
	t.Helper()
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
}

// dialPeer opens one UDP socket that stands for one CoAP peer: every datagram
// written through it comes from the same address, so the server sees one
// session.
func dialPeer(t *testing.T, srv *Server) *net.UDPConn {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// sendRequest writes one request datagram and returns the raw response bytes, so
// two responses can be compared byte for byte.
func sendRequest(t *testing.T, conn *net.UDPConn, m coap.Message) []byte {
	t.Helper()
	raw, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1500)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), buf[:n]...)
}

// testClock is a manually advanced clock, so an exchange lifetime can be tested
// without sleeping.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock { return &testClock{now: time.Unix(1_700_000_000, 0)} }

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func TestConfirmableDuplicateRunsHandlerOnce(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	srv.Handle(coap.CodePOST, "/charge", func(context.Context, Request) Response {
		return Response{
			Code:    coap.CodeChanged,
			Payload: []byte(fmt.Sprintf("charge-%d", calls.Add(1))),
		}
	})
	startServer(t, srv)
	peer := dialPeer(t, srv)
	req := coap.Message{
		Type: coap.Confirmable, Code: coap.CodePOST, MessageID: 100, Token: []byte{0xa, 0xb},
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("charge")}},
	}

	first := sendRequest(t, peer, req)
	second := sendRequest(t, peer, req)

	if got := calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("duplicate response is not byte identical:\nfirst  %x\nsecond %x", first, second)
	}
	resp, err := coap.Parse(second)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Type != coap.Acknowledgement || resp.Code != coap.CodeChanged {
		t.Fatalf("response = %v/%v", resp.Type, resp.Code)
	}
	if resp.MessageID != req.MessageID || !bytes.Equal(resp.Token, req.Token) {
		t.Fatalf("replayed mid/token = %d/%x", resp.MessageID, resp.Token)
	}
	if string(resp.Payload) != "charge-1" {
		t.Fatalf("replayed payload = %q, want the first response", resp.Payload)
	}
	if got := srv.dedup.size(); got != 1 {
		t.Fatalf("dedup size = %d, want 1", got)
	}
}

func TestConfirmableDedupIsPerPeer(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	srv.Handle(coap.CodePOST, "/charge", func(context.Context, Request) Response {
		return Response{Code: coap.CodeChanged, Payload: []byte(fmt.Sprintf("charge-%d", calls.Add(1)))}
	})
	startServer(t, srv)
	// Two sockets are two peers from the server's point of view; they use the
	// same message id and token on purpose.
	firstPeer := dialPeer(t, srv)
	secondPeer := dialPeer(t, srv)
	req := coap.Message{
		Type: coap.Confirmable, Code: coap.CodePOST, MessageID: 42, Token: []byte{1},
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("charge")}},
	}

	firstResp := sendRequest(t, firstPeer, req)
	secondResp := sendRequest(t, secondPeer, req)

	if got := calls.Load(); got != 2 {
		t.Fatalf("handler ran %d times, want 2: a second peer must not hit another peer's dedup entry", got)
	}
	if bytes.Equal(firstResp, secondResp) {
		t.Fatal("second peer got the first peer's cached response")
	}
	if got := srv.dedup.size(); got != 2 {
		t.Fatalf("dedup size = %d, want one entry per peer", got)
	}
}

func TestConfirmableDuplicateAfterLifetimeRunsAgain(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	clock := newTestClock()
	// Install the fake clock before Start so the read loop only ever sees it.
	srv.dedup.now = clock.Now
	var calls atomic.Int32
	srv.Handle(coap.CodePOST, "/charge", func(context.Context, Request) Response {
		return Response{Code: coap.CodeChanged, Payload: []byte(fmt.Sprintf("charge-%d", calls.Add(1)))}
	})
	startServer(t, srv)
	peer := dialPeer(t, srv)
	req := coap.Message{
		Type: coap.Confirmable, Code: coap.CodePOST, MessageID: 7, Token: []byte{3},
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("charge")}},
	}

	first := sendRequest(t, peer, req)
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
	// Inside the exchange lifetime the duplicate is replayed.
	if replay := sendRequest(t, peer, req); !bytes.Equal(first, replay) {
		t.Fatal("duplicate inside the exchange lifetime was not replayed")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times after an in-lifetime duplicate, want 1", got)
	}

	clock.Advance(DefaultExchangeLifetime)
	expired := sendRequest(t, peer, req)
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler ran %d times after the exchange lifetime, want 2", got)
	}
	if bytes.Equal(first, expired) {
		t.Fatal("response after the exchange lifetime came from the cache")
	}
	if got := srv.dedup.size(); got != 1 {
		t.Fatalf("dedup size = %d, want the expired entry replaced", got)
	}
}

func TestDedupCacheIsBounded(t *testing.T) {
	const capacity = 4
	srv, err := New(Config{Addr: "127.0.0.1:0", MaxExchanges: capacity})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	srv.Handle(coap.CodePOST, "/charge", func(context.Context, Request) Response {
		return Response{Code: coap.CodeChanged, Payload: []byte(fmt.Sprintf("charge-%d", calls.Add(1)))}
	})
	startServer(t, srv)
	peer := dialPeer(t, srv)
	request := func(mid uint16) coap.Message {
		return coap.Message{
			Type: coap.Confirmable, Code: coap.CodePOST, MessageID: mid, Token: []byte{byte(mid)},
			Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("charge")}},
		}
	}

	original := make(map[uint16]string, 6)
	for mid := uint16(1); mid <= 6; mid++ {
		original[mid] = responsePayload(t, sendRequest(t, peer, request(mid)))
	}
	if got := srv.dedup.size(); got != capacity {
		t.Fatalf("dedup size = %d after 6 exchanges, want the cap %d", got, capacity)
	}

	// The oldest entry was evicted, so its message id runs the handler again.
	before := calls.Load()
	if payload := responsePayload(t, sendRequest(t, peer, request(1))); payload == original[1] {
		t.Fatalf("evicted message id was served from the cache: %q", payload)
	}
	if got := calls.Load(); got != before+1 {
		t.Fatalf("handler ran %d times for the evicted message id, want %d", got, before+1)
	}

	// The newest entry is still cached and must not run the handler again.
	before = calls.Load()
	if payload := responsePayload(t, sendRequest(t, peer, request(6))); payload != original[6] {
		t.Fatalf("cached response = %q, want the original %q", payload, original[6])
	}
	if got := calls.Load(); got != before {
		t.Fatalf("handler ran again for a cached message id: %d calls", got)
	}
	if got := srv.dedup.size(); got != capacity {
		t.Fatalf("dedup size = %d after eviction, want %d", got, capacity)
	}
}

func TestNonConfirmableIsNotDeduplicated(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	srv.Handle(coap.CodePOST, "/charge", func(context.Context, Request) Response {
		calls.Add(1)
		return Response{Code: coap.CodeChanged}
	})
	startServer(t, srv)
	peer := dialPeer(t, srv)
	req := coap.Message{
		Type: coap.NonConfirmable, Code: coap.CodePOST, MessageID: 11, Token: []byte{4},
		Options: []coap.Option{{Number: coap.OptionURIPath, Value: []byte("charge")}},
	}

	sendRequest(t, peer, req)
	sendRequest(t, peer, req)

	if got := calls.Load(); got != 2 {
		t.Fatalf("handler ran %d times for two NON requests, want 2", got)
	}
	if got := srv.dedup.size(); got != 0 {
		t.Fatalf("dedup size = %d, want NON requests to stay out of the cache", got)
	}
}

// responsePayload returns the payload of a response datagram.
func responsePayload(t *testing.T, raw []byte) string {
	t.Helper()
	resp, err := coap.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return string(resp.Payload)
}

func TestPeerIdentityIncludesSessionIdentity(t *testing.T) {
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5683}
	// The same address with a new session identity (a fresh DTLS handshake or a
	// reconnected transport) must not collide with the old one.
	first := peerIdentity(&gnety.Session{ID: "dtls#1", RemoteAddr: addr})
	second := peerIdentity(&gnety.Session{ID: "dtls#2", RemoteAddr: addr})
	if first == second {
		t.Fatalf("peer identity ignores the session identity: %q", first)
	}
	if peerIdentity(nil) == "" || peerIdentity(&gnety.Session{ID: "udp-peer"}) == "" {
		t.Fatal("empty peer identity")
	}
}

func TestDedupCacheInFlightAndForget(t *testing.T) {
	cache := newDedupCache(4, time.Minute)
	key := exchangeKey{peer: "peer-a", messageID: 1}

	if _, hit := cache.begin(key); hit {
		t.Fatal("first delivery reported as duplicate")
	}
	// While the handler runs the entry suppresses the duplicate and has nothing
	// to replay.
	if data, hit := cache.begin(key); !hit || data != nil {
		t.Fatalf("in-flight duplicate = %q/%v, want a hit without a response", data, hit)
	}
	// A handler that produces no response must leave no empty cached answer.
	cache.complete(key, nil)
	if _, hit := cache.begin(key); hit {
		t.Fatal("an exchange without a response stayed cached")
	}

	// A completed exchange replays exactly what was stored.
	key = exchangeKey{peer: "peer-a", messageID: 2}
	cache.begin(key)
	cache.complete(key, []byte("ack"))
	if data, hit := cache.begin(key); !hit || string(data) != "ack" {
		t.Fatalf("replay = %q/%v", data, hit)
	}
}

func TestDedupCacheSweepsExpiredEntries(t *testing.T) {
	clock := newTestClock()
	cache := newDedupCache(16, 10*time.Second)
	cache.now = clock.Now

	cache.begin(exchangeKey{peer: "peer-a", messageID: 1})
	cache.begin(exchangeKey{peer: "peer-a", messageID: 2})
	if got := cache.size(); got != 2 {
		t.Fatalf("size = %d, want 2", got)
	}
	clock.Advance(5 * time.Second)
	cache.begin(exchangeKey{peer: "peer-a", messageID: 3})
	if got := cache.size(); got != 3 {
		t.Fatalf("size = %d, want 3", got)
	}
	clock.Advance(6 * time.Second)
	if got := cache.size(); got != 1 {
		t.Fatalf("size = %d after the first two exchanges expired, want 1", got)
	}
	clock.Advance(5 * time.Second)
	if got := cache.size(); got != 0 {
		t.Fatalf("size = %d after the exchange lifetime, want 0", got)
	}
	if len(cache.entries) != 0 {
		t.Fatalf("entries map holds %d stale exchanges", len(cache.entries))
	}
}
