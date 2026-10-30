package gcoap

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// testPeer is a bare UDP socket standing in for a CoAP server: handle decides
// which datagrams are answered, so a test controls exactly when the client sees
// a response, an empty ACK or a reset.
type testPeer struct {
	conn     *net.UDPConn
	received atomic.Int32
	answered atomic.Bool
	mu       sync.Mutex
	arrivals []time.Time
	wg       sync.WaitGroup
}

func newTestPeer(t *testing.T, handle func(p *testPeer, req coap.Message) []coap.Message) *testPeer {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	peer := &testPeer{conn: conn}
	peer.wg.Add(1)
	go func() {
		defer peer.wg.Done()
		buf := make([]byte, 1500)
		for {
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			req, err := coap.Parse(append([]byte(nil), buf[:n]...))
			if err != nil {
				continue
			}
			peer.received.Add(1)
			peer.mu.Lock()
			peer.arrivals = append(peer.arrivals, time.Now())
			peer.mu.Unlock()
			for _, resp := range handle(peer, req) {
				raw, err := resp.Marshal()
				if err != nil {
					continue
				}
				_, _ = conn.WriteToUDP(raw, addr)
			}
		}
	}()
	t.Cleanup(func() {
		_ = conn.Close()
		peer.wg.Wait()
	})
	return peer
}

func (p *testPeer) Addr() string { return p.conn.LocalAddr().String() }

// arrivalGaps returns the gaps between consecutive datagrams the peer saw.
func (p *testPeer) arrivalGaps() []time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	gaps := make([]time.Duration, 0, len(p.arrivals))
	for i := 1; i < len(p.arrivals); i++ {
		gaps = append(gaps, p.arrivals[i].Sub(p.arrivals[i-1]))
	}
	return gaps
}

func acknowledge(req coap.Message) coap.Message {
	return coap.Message{
		Type: coap.Acknowledgement, Code: coap.CodeContent,
		MessageID: req.MessageID, Token: append([]byte(nil), req.Token...), Payload: []byte("ok"),
	}
}

// fastClientConfig drives the client exchange in milliseconds: no jitter, so
// the expected number of transmissions is deterministic.
func fastClientConfig(addr string) ClientConfig {
	return ClientConfig{
		Addr:             addr,
		AckTimeout:       20 * time.Millisecond,
		AckRandomFactor:  1,
		MaxRetransmit:    3,
		ExchangeLifetime: 2 * time.Second,
	}
}

func TestClientRetransmitsUntilAnswered(t *testing.T) {
	var seen atomic.Int32
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		// Ignore the first transmission; the retransmission is answered.
		if seen.Add(1) == 1 {
			return nil
		}
		return []coap.Message{acknowledge(req)}
	})
	cli, err := DialConfig(context.Background(), fastClientConfig(peer.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := cli.Do(ctx, ClientRequest{Code: coap.CodeGET, Path: "/hello"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Code != coap.CodeContent || string(resp.Payload) != "ok" {
		t.Fatalf("resp = %v %q", resp.Code, resp.Payload)
	}
	if got := peer.received.Load(); got != 2 {
		t.Fatalf("transmissions = %d, want 2: one lost attempt plus one retransmission", got)
	}
	gaps := peer.arrivalGaps()
	if len(gaps) != 1 {
		t.Fatalf("arrival gaps = %v", gaps)
	}
	// With AckRandomFactor 1 the first retransmission waits exactly ACK_TIMEOUT.
	if gaps[0] < 15*time.Millisecond || gaps[0] > time.Second {
		t.Fatalf("retransmission gap = %v, want about ACK_TIMEOUT (20ms)", gaps[0])
	}
}

func TestClientGivesUpAfterMaxRetransmit(t *testing.T) {
	peer := newTestPeer(t, func(*testPeer, coap.Message) []coap.Message { return nil })
	cfg := fastClientConfig(peer.Addr())
	cfg.AckTimeout = 5 * time.Millisecond
	cfg.MaxRetransmit = 2
	cli, err := DialConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := cli.Do(ctx, ClientRequest{Code: coap.CodeGET}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if got := peer.received.Load(); got != 3 {
		t.Fatalf("transmissions = %d, want 1 + MaxRetransmit(2)", got)
	}
	gaps := peer.arrivalGaps()
	if len(gaps) != 2 {
		t.Fatalf("arrival gaps = %v", gaps)
	}
	// The backoff doubles: 5ms, then 10ms.
	if gaps[1] <= gaps[0] {
		t.Fatalf("retransmission intervals do not increase: %v", gaps)
	}
}

func TestClientStopsOnReset(t *testing.T) {
	peer := newTestPeer(t, func(_ *testPeer, req coap.Message) []coap.Message {
		return []coap.Message{{Type: coap.Reset, Code: coap.CodeEmpty, MessageID: req.MessageID}}
	})
	cli, err := DialConfig(context.Background(), fastClientConfig(peer.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := cli.Do(ctx, ClientRequest{Code: coap.CodeGET}); !errors.Is(err, ErrReset) {
		t.Fatalf("err = %v, want ErrReset", err)
	}
	if got := peer.received.Load(); got != 1 {
		t.Fatalf("transmissions = %d, want 1: a reset cancels the retransmissions", got)
	}
}

func TestClientStopsRetransmittingOnEmptyAck(t *testing.T) {
	// An empty ACK means the peer owns the exchange; the separate response
	// arrives later and no further retransmission may happen.
	peer := newTestPeer(t, func(p *testPeer, req coap.Message) []coap.Message {
		if p.answered.CompareAndSwap(false, true) {
			return []coap.Message{{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: req.MessageID}}
		}
		return nil
	})
	cli, err := DialConfig(context.Background(), fastClientConfig(peer.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	// No separate response follows, so the call times out; the point is that the
	// empty ACK stopped the retransmissions.
	if _, err := cli.Do(ctx, ClientRequest{Code: coap.CodeGET}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if got := peer.received.Load(); got != 1 {
		t.Fatalf("transmissions = %d, want 1: an empty ACK cancels the retransmissions", got)
	}
}

func TestRetransmissionScheduleFollowsBackoff(t *testing.T) {
	params := defaultExchangeParams()
	params.ackTimeout = 100 * time.Millisecond
	params.ackRandomFactor = 1.5
	params.maxRetransmit = 4
	params.rand = func() float64 { return 0 } // no jitter
	clock := newTestClock()
	params.now = clock.Now

	schedule := params.newRetransmissionSchedule()
	var waits []time.Duration
	for {
		wait, retransmit := schedule.next()
		waits = append(waits, wait)
		clock.Advance(wait)
		if !retransmit {
			break
		}
	}
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
	}
	if len(waits) != len(want) {
		t.Fatalf("waits = %v, want %d transmissions", waits, len(want))
	}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("waits = %v, want %v", waits, want)
		}
	}
}

func TestRetransmissionJitterBounds(t *testing.T) {
	params := defaultExchangeParams()
	params.ackTimeout = 100 * time.Millisecond
	params.ackRandomFactor = 1.5
	params.maxRetransmit = 4
	params.now = newTestClock().Now

	// The jitter is picked once per exchange and lies in
	// [ACK_TIMEOUT, ACK_TIMEOUT*ACK_RANDOM_FACTOR); the retransmissions double
	// that same timeout.
	lower := params.ackTimeout
	upper := time.Duration(float64(lower) * params.ackRandomFactor)
	for i := 0; i < 500; i++ {
		schedule := params.newRetransmissionSchedule()
		first, retransmit := schedule.next()
		if !retransmit {
			t.Fatal("first wait must be followed by a retransmission")
		}
		if first < lower || first >= upper {
			t.Fatalf("first wait = %v, want [%v, %v)", first, lower, upper)
		}
		previous := first
		for {
			wait, retransmit := schedule.next()
			if !retransmit {
				break
			}
			if want := 2 * previous; wait < want-time.Nanosecond || wait > want+time.Nanosecond {
				t.Fatalf("backoff %v does not double %v", wait, previous)
			}
			previous = wait
		}
	}

	// The jitter source is the only randomness: a fixed factor makes the whole
	// schedule deterministic.
	params.rand = func() float64 { return 0.5 }
	schedule := params.newRetransmissionSchedule()
	wait, _ := schedule.next()
	if wait != 125*time.Millisecond {
		t.Fatalf("first wait with factor 1.25 = %v", wait)
	}
	params.ackRandomFactor = 1
	if got := params.jitterFactor(); got != 1 {
		t.Fatalf("jitter factor without ACK_RANDOM_FACTOR = %v", got)
	}
	if got := params.retransmitInterval(1); got != 200*time.Millisecond {
		t.Fatalf("retransmitInterval(1) = %v", got)
	}
}

func TestRetransmissionStopsAtExchangeLifetime(t *testing.T) {
	params := defaultExchangeParams()
	params.ackTimeout = 100 * time.Millisecond
	params.exchangeLifetime = 250 * time.Millisecond
	params.maxRetransmit = 10
	params.rand = func() float64 { return 0 }
	clock := newTestClock()
	params.now = clock.Now

	schedule := params.newRetransmissionSchedule()
	var waits []time.Duration
	var total time.Duration
	for {
		wait, retransmit := schedule.next()
		waits = append(waits, wait)
		total += wait
		clock.Advance(wait)
		if !retransmit {
			break
		}
	}
	if total > params.exchangeLifetime {
		t.Fatalf("transmissions span %v, want at most the exchange lifetime %v", total, params.exchangeLifetime)
	}
	if total != params.exchangeLifetime {
		t.Fatalf("final wait total = %v, want the exchange lifetime %v", total, params.exchangeLifetime)
	}
	want := []time.Duration{100 * time.Millisecond, 150 * time.Millisecond}
	if len(waits) != len(want) || waits[0] != want[0] || waits[1] != want[1] {
		t.Fatalf("waits = %v, want %v", waits, want)
	}
}

func TestTimingConfigValidation(t *testing.T) {
	if _, err := New(Config{AckTimeout: -time.Second}); err == nil {
		t.Fatal("negative AckTimeout accepted")
	}
	if _, err := New(Config{AckRandomFactor: 0.5}); err == nil {
		t.Fatal("AckRandomFactor below 1 accepted")
	}
	if _, err := New(Config{MaxExchanges: -1}); err == nil {
		t.Fatal("negative MaxExchanges accepted")
	}
	if _, err := DialConfig(context.Background(), ClientConfig{
		Addr: "127.0.0.1:0", ExchangeLifetime: -time.Second,
	}); err == nil {
		t.Fatal("negative ExchangeLifetime accepted")
	}

	// Zero keeps the RFC defaults, a negative MaxRetransmit disables
	// retransmission.
	params, err := defaultExchangeParams().withTiming(0, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if params.ackTimeout != DefaultAckTimeout || params.ackRandomFactor != DefaultAckRandomFactor ||
		params.maxRetransmit != DefaultMaxRetransmit || params.exchangeLifetime != DefaultExchangeLifetime {
		t.Fatalf("defaults = %+v", params)
	}
	params, err = defaultExchangeParams().withTiming(0, 0, -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if params.maxRetransmit != 0 {
		t.Fatalf("maxRetransmit = %d, want 0", params.maxRetransmit)
	}
}
