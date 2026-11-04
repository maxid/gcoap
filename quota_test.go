package gcoap

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

// quotaPeer is one raw UDP peer: one socket, therefore one server-side session,
// so every request it sends is counted against the same per-peer quota.
type quotaPeer struct {
	conn *net.UDPConn
	mid  uint16
}

func newQuotaPeer(t *testing.T, srv *Server) *quotaPeer {
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
	return &quotaPeer{conn: conn}
}

// do sends one request and returns the response. Quota tests do not retransmit,
// so a missing response is a test failure rather than a protocol event.
func (p *quotaPeer) do(t *testing.T, req coap.Message) coap.Message {
	t.Helper()
	p.mid++
	req.Type = coap.Confirmable
	req.MessageID = p.mid
	raw, err := req.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := p.conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.conn.Write(raw); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 1500)
	n, err := p.conn.Read(buf)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	resp, err := coap.Parse(buf[:n])
	if err != nil {
		t.Fatalf("parse response: %v", err)
	}
	return resp
}

// observe registers (register=true) or cancels an observation. One token is one
// observation, so the caller picks it: a refresh reuses the token.
func (p *quotaPeer) observe(t *testing.T, path string, token byte, register bool) coap.Message {
	t.Helper()
	value := coap.ObserveRegister
	if !register {
		value = coap.ObserveDeregister
	}
	return p.do(t, coap.Message{
		Code:  coap.CodeGET,
		Token: []byte{token},
		Options: []coap.Option{
			observeOption(value),
			{Number: coap.OptionURIPath, Value: []byte(strings.TrimPrefix(path, "/"))},
		},
	})
}

// upload sends one Block1 chunk of the upload identified by token. more=false
// completes it.
func (p *quotaPeer) upload(t *testing.T, path string, token byte, num uint32, more bool, payload []byte) coap.Message {
	t.Helper()
	block, err := coap.Block{Num: num, More: more, SizeExp: 6}.Encode() // 1024-byte blocks
	if err != nil {
		t.Fatal(err)
	}
	return p.do(t, coap.Message{
		Code:    coap.CodePOST,
		Token:   []byte{token},
		Payload: payload,
		Options: []coap.Option{
			{Number: coap.OptionBlock1, Value: block},
			{Number: coap.OptionURIPath, Value: []byte(strings.TrimPrefix(path, "/"))},
		},
	})
}

func newQuotaServer(t *testing.T, cfg Config) *Server {
	t.Helper()
	cfg.Addr = "127.0.0.1:0"
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.Handle(coap.CodeGET, "/state", func(_ context.Context, _ Request) Response {
		return Response{Code: coap.CodeContent, Payload: []byte("v1")}
	})
	srv.Handle(coap.CodePOST, "/upload", func(_ context.Context, req Request) Response {
		return Response{Code: coap.CodeChanged, Payload: req.Payload}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	return srv
}

// peerSession returns the one session the server holds for the peer that has
// already talked to it.
func peerSession(t *testing.T, srv *Server) *gnety.Session {
	t.Helper()
	srv.obsMu.Lock()
	defer srv.obsMu.Unlock()
	for _, sess := range srv.sessions {
		return sess
	}
	t.Fatal("the server holds no session for the peer")
	return nil
}

func observerTotal(srv *Server) int {
	srv.obsMu.Lock()
	defer srv.obsMu.Unlock()
	return srv.observerCountLocked()
}

func uploadTotal(srv *Server) int {
	srv.blockMu.Lock()
	defer srv.blockMu.Unlock()
	return len(srv.uploads)
}

func TestObserversPerPeerLimit(t *testing.T) {
	srv := newQuotaServer(t, Config{MaxObserversPerPeer: 2})
	a := newQuotaPeer(t, srv)
	for i, token := range []byte{1, 2} {
		if resp := a.observe(t, "/state", token, true); resp.Code != coap.CodeContent {
			t.Fatalf("registration %d = %s, want 2.05", i+1, resp.Code)
		}
	}
	if resp := a.observe(t, "/state", 3, true); resp.Code != coap.CodeServiceUnavailable {
		t.Fatalf("registration beyond the per-peer limit = %s, want 5.03", resp.Code)
	}
	// The rejected registration left no state behind.
	if got := observerTotal(srv); got != 2 {
		t.Fatalf("observers after a rejected registration = %d, want 2", got)
	}

	// Another peer has its own quota: one peer at its limit does not starve it.
	b := newQuotaPeer(t, srv)
	if resp := b.observe(t, "/state", 1, true); resp.Code != coap.CodeContent {
		t.Fatalf("second peer registration = %s, want 2.05", resp.Code)
	}
}

func TestObserverRefreshKeepsQuota(t *testing.T) {
	srv := newQuotaServer(t, Config{MaxObserversPerPeer: 1})
	a := newQuotaPeer(t, srv)
	if resp := a.observe(t, "/state", 1, true); resp.Code != coap.CodeContent {
		t.Fatalf("registration = %s, want 2.05", resp.Code)
	}

	// Same session, same token, same path: a refresh, not a new observation, so
	// it must not be counted against the quota even though the quota is full.
	if resp := a.observe(t, "/state", 1, true); resp.Code != coap.CodeContent {
		t.Fatalf("refresh at the per-peer limit = %s, want 2.05", resp.Code)
	}
	if got := observerTotal(srv); got != 1 {
		t.Fatalf("observers after a refresh = %d, want 1", got)
	}

	// A different token on the same path is a new observation, not a refresh,
	// so the full quota applies to it.
	if resp := a.observe(t, "/state", 2, true); resp.Code != coap.CodeServiceUnavailable {
		t.Fatalf("new observation at the per-peer limit = %s, want 5.03", resp.Code)
	}
	if got := observerTotal(srv); got != 1 {
		t.Fatalf("observers after a rejected observation = %d, want 1", got)
	}
}

func TestObserverQuotaReleasedOnDeregister(t *testing.T) {
	srv := newQuotaServer(t, Config{MaxObserversPerPeer: 1})
	a := newQuotaPeer(t, srv)
	if resp := a.observe(t, "/state", 1, true); resp.Code != coap.CodeContent {
		t.Fatalf("registration = %s, want 2.05", resp.Code)
	}
	if resp := a.observe(t, "/state", 1, false); resp.Code != coap.CodeContent {
		t.Fatalf("deregistration = %s, want 2.05", resp.Code)
	}
	if got := observerTotal(srv); got != 0 {
		t.Fatalf("observers after deregistration = %d, want 0", got)
	}
	if resp := a.observe(t, "/state", 1, true); resp.Code != coap.CodeContent {
		t.Fatalf("registration after the quota was released = %s, want 2.05", resp.Code)
	}
}

func TestBlockUploadsPerPeerLimit(t *testing.T) {
	srv := newQuotaServer(t, Config{MaxBlockUploads: 8, MaxBlockUploadsPerPeer: 1})
	a := newQuotaPeer(t, srv)
	chunk := bytes.Repeat([]byte{'a'}, 1024)
	if resp := a.upload(t, "/upload", 1, 0, true, chunk); resp.Code != coap.CodeContinue {
		t.Fatalf("first upload block = %s, want 2.31", resp.Code)
	}
	// A second upload from the same peer needs its own slot; the quota is one.
	if resp := a.upload(t, "/upload", 2, 0, true, chunk); resp.Code != coap.CodeServiceUnavailable {
		t.Fatalf("second upload beyond the per-peer limit = %s, want 5.03", resp.Code)
	}
	if got := uploadTotal(srv); got != 1 {
		t.Fatalf("uploads in progress after a rejected upload = %d, want 1", got)
	}

	// Another peer is unaffected.
	b := newQuotaPeer(t, srv)
	if resp := b.upload(t, "/upload", 3, 0, true, chunk); resp.Code != coap.CodeContinue {
		t.Fatalf("second peer upload block = %s, want 2.31", resp.Code)
	}

	// Completing the upload frees the slot: uploads in progress, not the
	// completed-response cache, are what the quota counts.
	if resp := a.upload(t, "/upload", 1, 1, false, []byte("done")); resp.Code != coap.CodeChanged {
		t.Fatalf("final upload block = %s, want 2.04", resp.Code)
	}
	if got := uploadTotal(srv); got != 1 {
		t.Fatalf("uploads in progress after completion = %d, want 1 (peer b)", got)
	}
	if resp := a.upload(t, "/upload", 4, 0, true, chunk); resp.Code != coap.CodeContinue {
		t.Fatalf("upload after the slot was released = %s, want 2.31", resp.Code)
	}
}

func TestQuotaReleasedOnSessionClose(t *testing.T) {
	srv := newQuotaServer(t, Config{MaxObserversPerPeer: 1, MaxBlockUploads: 8, MaxBlockUploadsPerPeer: 1})
	a := newQuotaPeer(t, srv)
	if resp := a.observe(t, "/state", 1, true); resp.Code != coap.CodeContent {
		t.Fatalf("registration = %s, want 2.05", resp.Code)
	}
	if resp := a.upload(t, "/upload", 1, 0, true, bytes.Repeat([]byte{'a'}, 1024)); resp.Code != coap.CodeContinue {
		t.Fatalf("upload block = %s, want 2.31", resp.Code)
	}
	if got := observerTotal(srv); got != 1 || uploadTotal(srv) != 1 {
		t.Fatalf("before close: observers %d uploads %d, want 1 and 1", got, uploadTotal(srv))
	}

	// The same call gnety makes on OnClose: everything the session held is
	// dropped, and with it the quota it consumed.
	srv.untrackSession(peerSession(t, srv))
	if got := observerTotal(srv); got != 0 {
		t.Fatalf("observers after the session closed = %d, want 0", got)
	}
	if got := uploadTotal(srv); got != 0 {
		t.Fatalf("uploads in progress after the session closed = %d, want 0", got)
	}

	if resp := a.observe(t, "/state", 1, true); resp.Code != coap.CodeContent {
		t.Fatalf("registration after the session closed = %s, want 2.05", resp.Code)
	}
	if resp := a.upload(t, "/upload", 2, 0, true, bytes.Repeat([]byte{'a'}, 1024)); resp.Code != coap.CodeContinue {
		t.Fatalf("upload after the session closed = %s, want 2.31", resp.Code)
	}
}

func TestQuotaDefaultsAndValidation(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if srv.maxObserversPeer != DefaultMaxObserversPerPeer {
		t.Fatalf("default observers per peer = %d, want %d", srv.maxObserversPeer, DefaultMaxObserversPerPeer)
	}
	if srv.maxUploadsPeer != DefaultMaxBlockUploadsPerPeer {
		t.Fatalf("default uploads per peer = %d, want %d", srv.maxUploadsPeer, DefaultMaxBlockUploadsPerPeer)
	}

	if _, err := New(Config{MaxObserversPerPeer: -1}); err == nil {
		t.Fatal("New accepted a negative MaxObserversPerPeer")
	}
	if _, err := New(Config{MaxBlockUploadsPerPeer: -1}); err == nil {
		t.Fatal("New accepted a negative MaxBlockUploadsPerPeer")
	}
}
