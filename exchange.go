package gcoap

// This file holds the CoAP exchange layer: the RFC 7252 timing parameters, the
// inbound message-id deduplication cache and the outbound retransmission
// schedule. Wire encoding stays in gnety/codec/coap; this is only exchange
// state, the part the codec package deliberately does not implement.

import (
	"container/list"
	"errors"
	rand "math/rand/v2"
	"sync"
	"time"

	"github.com/maxid/gnety"
)

// RFC 7252 section 4.8 protocol parameters. Config and ClientConfig start from
// these values and accept overrides so a test can drive the exchange in
// milliseconds instead of seconds.
const (
	// DefaultAckTimeout is ACK_TIMEOUT: the base retransmission interval.
	DefaultAckTimeout = 2 * time.Second
	// DefaultAckRandomFactor is ACK_RANDOM_FACTOR: the upper bound of the
	// initial jitter, which starts at ACK_TIMEOUT.
	DefaultAckRandomFactor = 1.5
	// DefaultMaxRetransmit is MAX_RETRANSMIT: how often a confirmable message
	// is retransmitted before the exchange is abandoned.
	DefaultMaxRetransmit = 4
	// DefaultExchangeLifetime is EXCHANGE_LIFETIME: a cached confirmable
	// response is replayed for this long and one outbound exchange never runs
	// longer than this.
	DefaultExchangeLifetime = 247 * time.Second
	// DefaultMaxExchanges bounds the server's inbound deduplication cache.
	DefaultMaxExchanges = 1024
)

// exchangeParams is the protocol timing shared by inbound deduplication and
// outbound retransmission.
type exchangeParams struct {
	ackTimeout       time.Duration
	ackRandomFactor  float64
	maxRetransmit    int
	exchangeLifetime time.Duration

	// rand returns the jitter source in [0,1) and now is the clock the exchange
	// lifetime is measured on; tests replace both.
	rand func() float64
	now  func() time.Time
}

// defaultExchangeParams returns the RFC 7252 defaults.
func defaultExchangeParams() exchangeParams {
	return exchangeParams{
		ackTimeout:       DefaultAckTimeout,
		ackRandomFactor:  DefaultAckRandomFactor,
		maxRetransmit:    DefaultMaxRetransmit,
		exchangeLifetime: DefaultExchangeLifetime,
		rand:             rand.Float64,
		now:              time.Now,
	}
}

// withTiming applies the caller's overrides. A zero value keeps the RFC
// default; negative durations, a negative factor and a factor below one are
// rejected. A negative maxRetransmit disables retransmission.
func (p exchangeParams) withTiming(ackTimeout time.Duration, ackRandomFactor float64, maxRetransmit int, exchangeLifetime time.Duration) (exchangeParams, error) {
	if ackTimeout < 0 || exchangeLifetime < 0 {
		return p, errors.New("gcoap: negative exchange timing")
	}
	if ackRandomFactor < 0 || (ackRandomFactor > 0 && ackRandomFactor < 1) {
		return p, errors.New("gcoap: AckRandomFactor must be at least 1")
	}
	if ackTimeout > 0 {
		p.ackTimeout = ackTimeout
	}
	if ackRandomFactor > 0 {
		p.ackRandomFactor = ackRandomFactor
	}
	if exchangeLifetime > 0 {
		p.exchangeLifetime = exchangeLifetime
	}
	switch {
	case maxRetransmit > 0:
		p.maxRetransmit = maxRetransmit
	case maxRetransmit < 0:
		p.maxRetransmit = 0
	}
	return p, nil
}

// retransmitInterval is the backoff before the retransmission that follows
// transmission attempt+1 (zero based), per RFC 7252 section 4.2: each
// retransmission doubles ACK_TIMEOUT.
func (p exchangeParams) retransmitInterval(attempt int) time.Duration {
	base := p.ackTimeout
	for i := 0; i < attempt; i++ {
		// Clamp at the exchange lifetime before doubling so a large
		// MaxRetransmit cannot overflow the duration.
		if base > p.exchangeLifetime/2 {
			return p.exchangeLifetime
		}
		base *= 2
	}
	return base
}

// jitterFactor is the random value RFC 7252 picks once per exchange: the first
// timeout lies between ACK_TIMEOUT and ACK_TIMEOUT*ACK_RANDOM_FACTOR, and every
// retransmission doubles that same timeout.
func (p exchangeParams) jitterFactor() float64 {
	if p.ackRandomFactor <= 1 {
		return 1
	}
	return 1 + p.rand()*(p.ackRandomFactor-1)
}

// retransmissionSchedule is the retransmission plan of one outbound confirmable
// message: at most MaxRetransmit retransmissions with the RFC 7252 backoff, all
// of them inside the exchange lifetime.
type retransmissionSchedule struct {
	params   exchangeParams
	deadline time.Time
	factor   float64
	sent     int
}

// newRetransmissionSchedule starts a plan at the current time of p.
func (p exchangeParams) newRetransmissionSchedule() *retransmissionSchedule {
	return &retransmissionSchedule{
		params:   p,
		deadline: p.now().Add(p.exchangeLifetime),
		factor:   p.jitterFactor(),
	}
}

// next records that one transmission was sent and returns how long to wait for
// the answer plus whether the message is retransmitted when that wait ends.
// retransmit is false once MaxRetransmit retransmissions are used up or the
// exchange lifetime ends, so the returned wait is the final response window.
func (s *retransmissionSchedule) next() (wait time.Duration, retransmit bool) {
	s.sent++
	remaining := s.remaining()
	if remaining <= 0 {
		return 0, false
	}
	wait = time.Duration(float64(s.params.retransmitInterval(s.sent-1)) * s.factor)
	retransmit = s.sent <= s.params.maxRetransmit
	if wait >= remaining {
		// The exchange lifetime ends before another transmission could be
		// answered, so that wait is the final one.
		return remaining, false
	}
	return wait, retransmit
}

// remaining is how much of the exchange lifetime is left.
func (s *retransmissionSchedule) remaining() time.Duration {
	remaining := s.deadline.Sub(s.params.now())
	if remaining < 0 {
		return 0
	}
	return remaining
}

// exchangeKey is the identity of one inbound confirmable exchange. CoAP message
// ids are only unique per peer, so the peer identity the session carries is part
// of the key: a message id from one peer can never hit another peer's entry.
type exchangeKey struct {
	peer      string
	messageID uint16
}

// peerIdentity folds the transport and security identity gnety attaches to a
// session into one dedup key component: the session id (for UDP and DTLS the
// peer address, for connection transports the address plus a connection
// sequence) and the remote address. A new DTLS handshake, a second connection
// from the same address or a NAT rebinding therefore gets a different identity
// and cannot replay another session's cached response.
func peerIdentity(sess *gnety.Session) string {
	if sess == nil {
		return "unknown"
	}
	addr := ""
	if sess.RemoteAddr != nil {
		addr = sess.RemoteAddr.String()
	}
	switch {
	case addr == "" || addr == sess.ID:
		return sess.ID
	case sess.ID == "":
		return addr
	}
	return sess.ID + "@" + addr
}

// dedupEntry is one remembered confirmable exchange. datagram is nil while the
// handler for the exchange is still running.
type dedupEntry struct {
	key      exchangeKey
	first    time.Time
	datagram []byte
}

// dedupCache remembers the responses of inbound confirmable exchanges so a
// retransmission replays the identical acknowledgement instead of running the
// handler twice. It is bounded twice: an entry expires after the exchange
// lifetime and a full cache evicts its oldest entry, so the maps never grow
// past max.
type dedupCache struct {
	mu       sync.Mutex
	max      int
	lifetime time.Duration
	now      func() time.Time // tests swap this before Start
	entries  map[exchangeKey]*list.Element
	order    *list.List // insertion order == expiry order
}

// newDedupCache returns a cache capped at max entries that drops each entry
// after lifetime.
func newDedupCache(max int, lifetime time.Duration) *dedupCache {
	if max < 1 {
		max = DefaultMaxExchanges
	}
	if lifetime <= 0 {
		lifetime = DefaultExchangeLifetime
	}
	return &dedupCache{
		max:      max,
		lifetime: lifetime,
		now:      time.Now,
		entries:  make(map[exchangeKey]*list.Element),
		order:    list.New(),
	}
}

// begin records the first delivery of a confirmable exchange. hit reports a
// duplicate: the cached response datagram is returned, or nil when the original
// handler is still running and the duplicate must stay unanswered. A miss
// reserves the entry so a duplicate that arrives later cannot run the handler
// again.
func (c *dedupCache) begin(key exchangeKey) (datagram []byte, hit bool) {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sweepLocked(now)
	if el, ok := c.entries[key]; ok {
		return el.Value.(*dedupEntry).datagram, true
	}
	c.entries[key] = c.order.PushBack(&dedupEntry{key: key, first: now})
	for c.order.Len() > c.max {
		c.removeLocked(c.order.Front())
	}
	return nil, false
}

// complete stores the response datagram of an exchange so a retransmission is
// answered byte for byte. An exchange without a response is dropped instead of
// cached as an empty answer.
func (c *dedupCache) complete(key exchangeKey, datagram []byte) {
	if len(datagram) == 0 {
		c.forget(key)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		el.Value.(*dedupEntry).datagram = datagram
	}
}

// forget drops the reservation of an exchange that produced no response, so the
// handler runs again if the peer retransmits.
func (c *dedupCache) forget(key exchangeKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		c.removeLocked(el)
	}
}

// size reports how many exchanges are cached after sweeping expired entries. It
// never exceeds the configured maximum.
func (c *dedupCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sweepLocked(c.now())
	return c.order.Len()
}

// reset drops every cached exchange.
func (c *dedupCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[exchangeKey]*list.Element)
	c.order.Init()
}

// sweepLocked drops the entries whose exchange lifetime is over. Entries are
// inserted in order with the same lifetime, so the list front is always the
// next one to expire.
func (c *dedupCache) sweepLocked(now time.Time) {
	for el := c.order.Front(); el != nil; el = c.order.Front() {
		if now.Sub(el.Value.(*dedupEntry).first) < c.lifetime {
			return
		}
		c.removeLocked(el)
	}
}

func (c *dedupCache) removeLocked(el *list.Element) {
	if el == nil {
		return
	}
	delete(c.entries, el.Value.(*dedupEntry).key)
	c.order.Remove(el)
}
