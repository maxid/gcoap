package gcoap

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// sentBlock is one Block1 request a scripted peer saw.
type sentBlock struct {
	block     coap.Block
	hasBlock1 bool
	payload   []byte
	size1     uint32
	hasSize1  bool
	token     []byte
	messageID uint16
}

// blockLog collects the Block1 requests of a scripted peer. newTestPeer runs its
// handler on the peer goroutine, so the log is mutex protected and assertions
// happen in the test goroutine.
type blockLog struct {
	mu   sync.Mutex
	seen []sentBlock
}

func (l *blockLog) record(req coap.Message) sentBlock {
	entry := sentBlock{
		payload:   append([]byte(nil), req.Payload...),
		token:     append([]byte(nil), req.Token...),
		messageID: req.MessageID,
	}
	if b, ok := req.Block1(); ok {
		entry.block, entry.hasBlock1 = b, true
	}
	if size1, ok := req.Size1(); ok {
		entry.size1, entry.hasSize1 = size1, true
	}
	l.mu.Lock()
	l.seen = append(l.seen, entry)
	l.mu.Unlock()
	return entry
}

func (l *blockLog) entries() []sentBlock {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]sentBlock(nil), l.seen...)
}

// block1Continue answers a non-final Block1 request with 2.31 Continue and the
// block option the server wants to see next.
func block1Continue(req coap.Message, b coap.Block) []coap.Message {
	option, err := blockOption(coap.OptionBlock1, b)
	if err != nil {
		return nil
	}
	return []coap.Message{{
		Type: coap.Acknowledgement, Code: coap.CodeContinue, MessageID: req.MessageID,
		Token: append([]byte(nil), req.Token...), Options: []coap.Option{option},
	}}
}

// finalAnswer answers with a final code and echoes the block option the way a
// stateless server would.
func finalAnswer(req coap.Message, code coap.Code) []coap.Message {
	return []coap.Message{{
		Type: coap.Acknowledgement, Code: code, MessageID: req.MessageID,
		Token: append([]byte(nil), req.Token...),
	}}
}

// TestClientBlock1ServerReassembly uploads a body of several blocks to an
// in-process server and checks the handler saw the whole body, for PUT and POST.
func TestClientBlock1ServerReassembly(t *testing.T) {
	for _, method := range []coap.Code{coap.CodePUT, coap.CodePOST} {
		t.Run(method.String(), func(t *testing.T) {
			srv, err := New(Config{Addr: "127.0.0.1:0"})
			if err != nil {
				t.Fatal(err)
			}
			stored := make(chan []byte, 1)
			srv.Handle(method, "/upload", func(_ context.Context, req Request) Response {
				stored <- append([]byte(nil), req.Payload...)
				return Response{Code: coap.CodeChanged}
			})
			startServer(t, srv)

			cli, err := DialConfig(context.Background(), ClientConfig{
				Addr: srv.Addr(), BlockSize: 16,
				AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1,
				MaxRetransmit: 2, ExchangeLifetime: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer cli.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			payload := bytes.Repeat([]byte("0123456789abcdef"), 3)[:40]
			resp, err := cli.Do(ctx, ClientRequest{Code: method, Path: "/upload", Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			if resp.Code != coap.CodeChanged {
				t.Fatalf("response code = %v, want 2.04", resp.Code)
			}
			select {
			case got := <-stored:
				if !bytes.Equal(got, payload) {
					t.Fatalf("server reassembled %d bytes %q, want %d bytes", len(got), got, len(payload))
				}
			case <-time.After(2 * time.Second):
				t.Fatal("handler was not called")
			}
		})
	}
}

// TestClientBlock1Sequence pins the block sequence of a multi-block upload: the
// block numbers, the size exponent, the Size1 total and one token per request.
func TestClientBlock1Sequence(t *testing.T) {
	log := &blockLog{}
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		entry := log.record(req)
		if !entry.hasBlock1 {
			return finalAnswer(req, coap.CodeBadRequest)
		}
		if entry.block.More {
			return block1Continue(req, coap.Block{Num: entry.block.Num, More: true, SizeExp: entry.block.SizeExp})
		}
		return finalAnswer(req, coap.CodeChanged)
	})
	cli, err := DialConfig(context.Background(), ClientConfig{
		Addr: peer.Addr(), BlockSize: 16,
		AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1,
		MaxRetransmit: 2, ExchangeLifetime: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	payload := bytes.Repeat([]byte("0123456789abcdef"), 4)[:50] // 16+16+16+2
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Do(ctx, ClientRequest{Code: coap.CodePUT, Path: "/upload", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Code != coap.CodeChanged {
		t.Fatalf("response code = %v, want 2.04", resp.Code)
	}

	seen := log.entries()
	wantLen := []int{16, 16, 16, 2}
	if len(seen) != len(wantLen) {
		t.Fatalf("sent %d Block1 requests, want %d: %+v", len(seen), len(wantLen), seen)
	}
	var assembled []byte
	for i, entry := range seen {
		if !entry.hasBlock1 {
			t.Fatalf("request %d has no Block1 option", i)
		}
		if entry.block.Num != uint32(i) {
			t.Fatalf("request %d block number = %d, want %d", i, entry.block.Num, i)
		}
		if entry.block.SizeExp != 0 {
			t.Fatalf("request %d SZX = %d, want 0 (16 bytes)", i, entry.block.SizeExp)
		}
		if wantMore := i < len(wantLen)-1; entry.block.More != wantMore {
			t.Fatalf("request %d More = %v, want %v", i, entry.block.More, wantMore)
		}
		if len(entry.payload) != wantLen[i] {
			t.Fatalf("request %d payload = %d bytes, want %d", i, len(entry.payload), wantLen[i])
		}
		if !entry.hasSize1 || int(entry.size1) != len(payload) {
			t.Fatalf("request %d Size1 = %d (present %v), want %d", i, entry.size1, entry.hasSize1, len(payload))
		}
		assembled = append(assembled, entry.payload...)
	}
	if !bytes.Equal(assembled, payload) {
		t.Fatalf("blocks carried %q, want %q", assembled, payload)
	}
	for i, entry := range seen {
		if !bytes.Equal(entry.token, seen[0].token) {
			t.Fatalf("request %d token = %x, want the token of the first request %x", i, entry.token, seen[0].token)
		}
		if i > 0 && entry.messageID == seen[i-1].messageID {
			t.Fatalf("request %d reused message id %d", i, entry.messageID)
		}
	}
}

// TestClientBlock1ServerDowngrade checks that a server can pick a smaller block
// size (RFC 7959 section 2.5): the block number is recomputed for the new size,
// nobody skips bytes, and a larger size in a later answer is ignored.
func TestClientBlock1ServerDowngrade(t *testing.T) {
	log := &blockLog{}
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		entry := log.record(req)
		if !entry.hasBlock1 {
			return finalAnswer(req, coap.CodeBadRequest)
		}
		szx := uint8(4) // 256 bytes, the server's choice
		switch entry.block.Num {
		case 0:
			// The client asked for 1024 bytes; downgrade to 256.
			szx = 4
		case 4:
			// An answer may not silently upgrade the transfer.
			szx = 6
		}
		if entry.block.More {
			return block1Continue(req, coap.Block{Num: entry.block.Num, More: true, SizeExp: szx})
		}
		return finalAnswer(req, coap.CodeChanged)
	})
	cli, err := DialConfig(context.Background(), ClientConfig{
		Addr: peer.Addr(),
		// Default block size: 1024 bytes, SZX 6.
		AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1,
		MaxRetransmit: 2, ExchangeLifetime: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	payload := make([]byte, 2500)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Do(ctx, ClientRequest{Code: coap.CodePUT, Path: "/upload", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Code != coap.CodeChanged {
		t.Fatalf("response code = %v, want 2.04", resp.Code)
	}

	seen := log.entries()
	wantNum := []uint32{0, 4, 5, 6, 7, 8, 9}
	wantSzx := []uint8{6, 4, 4, 4, 4, 4, 4}
	wantLen := []int{1024, 256, 256, 256, 256, 256, 196}
	if len(seen) != len(wantNum) {
		t.Fatalf("sent %d requests, want %d: %+v", len(seen), len(wantNum), seen)
	}
	var assembled []byte
	for i, entry := range seen {
		if entry.block.Num != wantNum[i] || entry.block.SizeExp != wantSzx[i] {
			t.Fatalf("request %d = num %d szx %d, want num %d szx %d",
				i, entry.block.Num, entry.block.SizeExp, wantNum[i], wantSzx[i])
		}
		if len(entry.payload) != wantLen[i] {
			t.Fatalf("request %d payload = %d bytes, want %d", i, len(entry.payload), wantLen[i])
		}
		assembled = append(assembled, entry.payload...)
	}
	if !bytes.Equal(assembled, payload) {
		t.Fatalf("blocks carried %d bytes that are not the request body", len(assembled))
	}
}

// TestClientBlock1EarlyResponse covers a server that ends the upload before the
// last block: an error response is returned as the result of the request and no
// further block is sent.
func TestClientBlock1EarlyResponse(t *testing.T) {
	log := &blockLog{}
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		log.record(req)
		return finalAnswer(req, coap.CodeBadRequest)
	})
	cli, err := DialConfig(context.Background(), ClientConfig{
		Addr: peer.Addr(), BlockSize: 16,
		AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1,
		MaxRetransmit: 2, ExchangeLifetime: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Do(ctx, ClientRequest{
		Code: coap.CodePUT, Path: "/upload", Payload: bytes.Repeat([]byte("x"), 40),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Code != coap.CodeBadRequest {
		t.Fatalf("response code = %v, want 4.00", resp.Code)
	}
	seen := log.entries()
	if len(seen) != 1 {
		t.Fatalf("sent %d requests after the early answer, want 1", len(seen))
	}
	if seen[0].block.More != true || seen[0].block.Num != 0 {
		t.Fatalf("first request = num %d more %v, want block 0 with more set",
			seen[0].block.Num, seen[0].block.More)
	}
}

// TestClientBlock1SizeLimit checks both size limits: the configured request body
// bound rejects before a datagram is sent, and a Size1 the server announces stops
// the upload.
func TestClientBlock1SizeLimit(t *testing.T) {
	t.Run("client limit", func(t *testing.T) {
		peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
			return finalAnswer(req, coap.CodeChanged)
		})
		cli, err := DialConfig(context.Background(), ClientConfig{
			Addr: peer.Addr(), BlockSize: 16, MaxRequestSize: 32,
			AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1,
			MaxRetransmit: 2, ExchangeLifetime: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer cli.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = cli.Do(ctx, ClientRequest{
			Code: coap.CodePUT, Path: "/upload", Payload: bytes.Repeat([]byte("x"), 64),
		})
		if !errors.Is(err, ErrRequestTooLarge) {
			t.Fatalf("err = %v, want ErrRequestTooLarge", err)
		}
		if got := peer.received.Load(); got != 0 {
			t.Fatalf("client sent %d datagrams for a body it should reject", got)
		}
	})

	t.Run("server Size1", func(t *testing.T) {
		log := &blockLog{}
		peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
			log.record(req)
			option, err := blockOption(coap.OptionBlock1,
				coap.Block{Num: 0, More: true, SizeExp: 0})
			if err != nil {
				return nil
			}
			resp := coap.Message{
				Type: coap.Acknowledgement, Code: coap.CodeContinue, MessageID: req.MessageID,
				Token: append([]byte(nil), req.Token...), Options: []coap.Option{option},
			}
			resp.SetSize1(32)
			return []coap.Message{resp}
		})
		cli, err := DialConfig(context.Background(), ClientConfig{
			Addr: peer.Addr(), BlockSize: 16,
			AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1,
			MaxRetransmit: 2, ExchangeLifetime: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer cli.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = cli.Do(ctx, ClientRequest{
			Code: coap.CodePUT, Path: "/upload", Payload: bytes.Repeat([]byte("x"), 64),
		})
		if !errors.Is(err, ErrRequestTooLarge) {
			t.Fatalf("err = %v, want ErrRequestTooLarge", err)
		}
		if seen := log.entries(); len(seen) != 1 {
			t.Fatalf("sent %d requests after the Size1 answer, want 1", len(seen))
		}
	})
}

// TestClientBlockSizeConfig validates the block size bound of ClientConfig.
func TestClientBlockSizeConfig(t *testing.T) {
	for _, size := range []int{8, 24, 100, 2048, 1 << 20} {
		if _, err := blockSizeExp(size); err == nil {
			t.Fatalf("blockSizeExp(%d) accepted a size outside 16..1024 power of two", size)
		}
	}
	cases := map[int]uint8{0: 6, 16: 0, 32: 1, 256: 4, 1024: 6}
	for size, want := range cases {
		exp, err := blockSizeExp(size)
		if err != nil {
			t.Fatalf("blockSizeExp(%d): %v", size, err)
		}
		if exp != want {
			t.Fatalf("blockSizeExp(%d) = %d, want %d", size, exp, want)
		}
		if got := blockSizeOf(exp); size != 0 && got != size {
			t.Fatalf("blockSizeOf(%d) = %d, want %d", exp, got, size)
		}
	}
	// DialConfig rejects an invalid block size before dialing.
	if _, err := DialConfig(context.Background(), ClientConfig{Addr: "127.0.0.1:9", BlockSize: 100}); err == nil {
		t.Fatal("DialConfig accepted BlockSize 100")
	}
}

// TestClientBodyNotBlockedBelowBlockSize keeps the single datagram path: a body
// that fits one block is sent as one request without a Block1 option.
func TestClientBodyNotBlockedBelowBlockSize(t *testing.T) {
	log := &blockLog{}
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		log.record(req)
		return finalAnswer(req, coap.CodeChanged)
	})
	cli, err := DialConfig(context.Background(), ClientConfig{
		Addr: peer.Addr(), BlockSize: 16,
		AckTimeout: 50 * time.Millisecond, AckRandomFactor: 1,
		MaxRetransmit: 2, ExchangeLifetime: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := bytes.Repeat([]byte("y"), 16)
	if _, err := cli.Do(ctx, ClientRequest{Code: coap.CodePUT, Path: "/upload", Payload: payload}); err != nil {
		t.Fatal(err)
	}
	seen := log.entries()
	if len(seen) != 1 {
		t.Fatalf("sent %d requests, want 1", len(seen))
	}
	if seen[0].hasBlock1 {
		t.Fatalf("a body of exactly one block carried a Block1 option: %+v", seen[0].block)
	}
	if !bytes.Equal(seen[0].payload, payload) {
		t.Fatalf("payload = %q, want %q", seen[0].payload, payload)
	}
}
