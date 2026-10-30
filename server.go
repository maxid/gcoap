// Package gcoap is a minimal CoAP server state machine built on gnety.
//
// Wire encoding is provided by gnety/codec/coap; transport is gnety UDP/DTLS.
// Basic Observe and block-wise transfers are implemented.
//
// The exchange layer implements the RFC 7252 section 4.2 retransmission and
// deduplication rules: a retransmitted confirmable request is answered from a
// bounded cache without running the handler again, outbound confirmable
// messages retransmit with exponential backoff inside the exchange lifetime, and
// handler contexts are cancelled when their exchange completes or the server
// stops. The same outbound exchange correlates ACKs/RSTs for confirmable Observe
// notifications (RFC 7641) and separate responses (RFC 7252 section 5.2).
package gcoap

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

// Handler handles one CoAP request.
type Handler func(ctx context.Context, req Request) Response

// Request is a parsed CoAP request.
type Request struct {
	Message coap.Message
	Path    string
	Query   url.Values
	Payload []byte
	Session *gnety.Session

	exchange *requestExchange // set by the server; nil for a caller-built Request
}

// RespondLater tells the server that this request is answered after the
// handler returns and returns the function that sends the response. For a
// confirmable request the server sends an empty ACK immediately; the returned
// response is a separate CON message with its own message id, retransmitted
// until the client ACKs or resets it (RFC 7252 section 5.2).
//
// The handler may either return a zero Response after starting its own worker
// and call the responder from that goroutine, or return the final Response
// itself (the server then sends it separately). The handler context stays alive
// until the response is sent, its session closes or the server stops. A Request
// with no exchange
// (one built by a caller rather than passed to a handler) returns a responder
// that always reports ErrResponseAlreadySent. RespondLater is optional; handlers
// that return normally keep the existing piggyback behavior.
func (r Request) RespondLater() Responder {
	if r.exchange == nil {
		return func(Response) error { return ErrResponseAlreadySent }
	}
	return r.exchange.deferResponse()
}

// Response is a CoAP response.
type Response struct {
	Code    coap.Code
	Options []coap.Option
	Payload []byte
}

// Config configures a CoAP server.
type Config struct {
	Addr    string
	Network string // "udp" (default) or "dtls"
	TLS     gnety.TLSConfig

	// Exchange timing follows RFC 7252 section 4.8. A zero value keeps the
	// default below; negative durations and a factor below one are rejected by
	// New, so tests can drive the timings in milliseconds.
	AckTimeout      time.Duration // ACK_TIMEOUT, default 2s
	AckRandomFactor float64       // ACK_RANDOM_FACTOR, default 1.5
	// MaxRetransmit is MAX_RETRANSMIT, the number of retransmissions of an
	// outbound confirmable message: zero keeps the default of 4, a negative
	// value sends once and never retransmits.
	MaxRetransmit int
	// ExchangeLifetime is EXCHANGE_LIFETIME, default 247s. It bounds both how
	// long a deduplicated confirmable exchange is replayed and how long one
	// outbound exchange may run.
	ExchangeLifetime time.Duration
	// MaxExchanges bounds the inbound message-id deduplication cache: zero keeps
	// the default of 1024.
	MaxExchanges int
	// ConfirmableNotifications sends Observe notifications as confirmable
	// messages that are retransmitted until the client ACKs or resets them.
	// The default is false, which keeps the existing NON notification behavior.
	ConfirmableNotifications bool
	// SeparateResponses runs each confirmable request handler on its own
	// goroutine, so a slow handler no longer blocks the transport read loop and a
	// duplicate request can be answered while it runs. Separate responses are
	// automatic: whenever a confirmable handler has not answered within
	// AckTimeout the server sends an empty ACK and later delivers the response
	// as a new confirmable message (RFC 7252 section 5.2). The default is false,
	// which keeps handlers on the read loop but still uses the separate response
	// mechanism for a slow handler. Handlers must be safe for concurrent calls
	// when this is enabled. A handler can also request a separate response
	// explicitly with Request.RespondLater.
	SeparateResponses bool
}

// Server is a minimal CoAP server.
type Server struct {
	srv      *gnety.Server
	mu       sync.RWMutex
	routes   map[string]Handler
	prefixes map[string]Handler
	nextID   atomic.Uint32

	params exchangeParams
	dedup  *dedupCache

	confirmableNotifications bool
	separateResponses        bool

	outMu    sync.Mutex
	outbound map[outboundKey]*outboundExchange

	baseCtx context.Context
	cancel  context.CancelFunc

	hMu      sync.Mutex
	hSeq     uint64
	handlers map[string]map[uint64]context.CancelFunc

	obsMu     sync.Mutex
	observers map[string][]*observer
	sessions  map[string]*gnety.Session

	blockMu sync.Mutex
	uploads map[string]*upload
}

// New creates a CoAP server.
func New(cfg Config) (*Server, error) {
	if cfg.MaxExchanges < 0 {
		return nil, errors.New("gcoap: MaxExchanges must not be negative")
	}
	params, err := defaultExchangeParams().withTiming(
		cfg.AckTimeout, cfg.AckRandomFactor, cfg.MaxRetransmit, cfg.ExchangeLifetime)
	if err != nil {
		return nil, err
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	s := &Server{
		routes: make(map[string]Handler), prefixes: make(map[string]Handler),
		observers: make(map[string][]*observer), sessions: make(map[string]*gnety.Session),
		uploads:  make(map[string]*upload),
		params:   params,
		dedup:    newDedupCache(cfg.MaxExchanges, params.exchangeLifetime),
		baseCtx:  baseCtx,
		cancel:   cancel,
		handlers: make(map[string]map[uint64]context.CancelFunc),

		confirmableNotifications: cfg.ConfirmableNotifications,
		separateResponses:        cfg.SeparateResponses,
		outbound:                 make(map[outboundKey]*outboundExchange),
	}
	network := cfg.Network
	if network == "" {
		network = "udp"
	}
	srv, err := gnety.New(gnety.Config{
		Network:       network,
		Addr:          cfg.Addr,
		TLS:           cfg.TLS,
		DatagramCodec: MessageCodec{},
		Handler: gnety.HandlerFuncs{
			Message: s.onMessage,
			Open:    func(sess *gnety.Session) { s.trackSession(sess) },
			Close:   func(sess *gnety.Session, _ error) { s.untrackSession(sess) },
		},
	})
	if err != nil {
		cancel()
		return nil, err
	}
	s.srv = srv
	return s, nil
}

// Handle registers a handler for method + path (e.g. coap.CodeGET, "/sensors").
func (s *Server) Handle(method coap.Code, path string, h Handler) {
	if path == "" {
		path = "/"
	}
	s.mu.Lock()
	s.routes[routeKey(method, path)] = h
	s.mu.Unlock()
}

// HandlePrefix registers a handler for a path prefix (longest match wins).
func (s *Server) HandlePrefix(method coap.Code, prefix string, h Handler) {
	if prefix == "" {
		prefix = "/"
	}
	s.mu.Lock()
	s.prefixes[routeKey(method, prefix)] = h
	s.mu.Unlock()
}

func (s *Server) match(method coap.Code, path string) Handler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if h := s.routes[routeKey(method, path)]; h != nil {
		return h
	}
	bestKey, bestLen := "", -1
	for key := range s.prefixes {
		kMethod, kPath := splitRouteKey(key)
		if kMethod != method {
			continue
		}
		if path == kPath || strings.HasPrefix(path, strings.TrimSuffix(kPath, "/")+"/") {
			if len(kPath) > bestLen {
				bestKey, bestLen = key, len(kPath)
			}
		}
	}
	if bestKey == "" {
		return nil
	}
	return s.prefixes[bestKey]
}

func splitRouteKey(key string) (coap.Code, string) {
	var code int
	var path string
	_, _ = fmt.Sscanf(key, "%d %s", &code, &path)
	return coap.Code(code), path
}

// Start starts the UDP listener.
//
// Handler contexts derive from ctx, so cancelling ctx or stopping the server
// cancels an in-flight handler.
func (s *Server) Start(ctx context.Context) error {
	baseCtx, cancel := context.WithCancel(ctx)
	s.hMu.Lock()
	s.baseCtx, s.cancel = baseCtx, cancel
	s.hMu.Unlock()
	return s.srv.Start(ctx)
}

// Stop stops the listener.
//
// In-flight handler contexts are cancelled before the listener shuts down so a
// slow handler cannot delay the stop.
func (s *Server) Stop(ctx context.Context) error {
	s.hMu.Lock()
	cancel := s.cancel
	s.hMu.Unlock()
	cancel()
	s.dedup.reset()
	return s.srv.Stop(ctx)
}

// Addr returns the listen address.
func (s *Server) Addr() string { return s.srv.Addr() }

// onMessage serves one request datagram. An ACK or RST is routed to the
// outbound exchange that owns its message id and never reaches the request
// path. A confirmable request is answered from the deduplication cache when the
// peer retransmits it, so the handler runs once per exchange (RFC 7252 section
// 4.2); a non-confirmable request is never deduplicated because it is not
// acknowledged.
func (s *Server) onMessage(sess *gnety.Session, msg any) {
	m, ok := msg.(coap.Message)
	if !ok || sess == nil {
		return
	}
	if m.Type == coap.Acknowledgement || m.Type == coap.Reset {
		s.dispatchControl(sess, m)
		return
	}
	if m.Code < coap.CodeGET || m.Code > coap.CodeDELETE {
		return
	}
	var key exchangeKey
	confirmable := m.Type == coap.Confirmable
	if confirmable {
		key = exchangeKey{peer: peerIdentity(sess), messageID: m.MessageID}
		if cached, hit := s.dedup.begin(key); hit {
			if len(cached) > 0 {
				_ = sess.Send(cached)
			}
			return
		}
	}
	st := &requestExchange{srv: s, sess: sess, msg: m, key: key, confirmable: confirmable}
	if confirmable && s.separateResponses {
		// Detach the handler so the read loop can receive the client's ACK for
		// the separate response (and duplicate requests) while it runs.
		go s.answerRequest(st)
		return
	}
	s.answerRequest(st)
}

// baseContext returns the server lifetime context outbound exchanges derive
// from.
func (s *Server) baseContext() context.Context {
	s.hMu.Lock()
	defer s.hMu.Unlock()
	return s.baseCtx
}

// handlerContext returns the context one handler invocation runs with. It is
// cancelled when the handler returns, when its session closes and when the
// server stops.
func (s *Server) handlerContext(sess *gnety.Session) (context.Context, func()) {
	s.hMu.Lock()
	baseCtx := s.baseCtx
	s.hMu.Unlock()
	ctx, cancel := context.WithCancel(baseCtx)
	if sess == nil {
		return ctx, cancel
	}
	id := sess.ID
	s.hMu.Lock()
	if s.handlers == nil {
		s.handlers = make(map[string]map[uint64]context.CancelFunc)
	}
	seq := s.hSeq
	s.hSeq++
	if s.handlers[id] == nil {
		s.handlers[id] = make(map[uint64]context.CancelFunc)
	}
	s.handlers[id][seq] = cancel
	s.hMu.Unlock()
	return ctx, func() {
		cancel()
		s.hMu.Lock()
		if open := s.handlers[id]; open != nil {
			delete(open, seq)
			if len(open) == 0 {
				delete(s.handlers, id)
			}
		}
		s.hMu.Unlock()
	}
}

// cancelSessionHandlers cancels the handler contexts of a closing session.
func (s *Server) cancelSessionHandlers(id string) {
	s.hMu.Lock()
	pending := s.handlers[id]
	delete(s.handlers, id)
	s.hMu.Unlock()
	for _, cancel := range pending {
		cancel()
	}
}

// responseMessage builds the response for req: a piggyback acknowledgement for a
// confirmable request, and a non-confirmable message with a fresh id for a non
// confirmable one.
func responseMessage(next func(uint32) uint32, req coap.Message, resp Response) coap.Message {
	if resp.Code == 0 {
		resp.Code = coap.CodeContent
	}
	out := coap.Message{
		Type: coap.Acknowledgement, Code: resp.Code, MessageID: req.MessageID,
		Token: append([]byte(nil), req.Token...), Options: resp.Options, Payload: resp.Payload,
	}
	if req.Type == coap.NonConfirmable {
		out.Type = coap.NonConfirmable
		out.MessageID = uint16(next(1))
	}
	return out
}

func routeKey(method coap.Code, path string) string {
	return fmt.Sprintf("%d %s", method, path)
}

func parsePath(m coap.Message) (path string, query url.Values) {
	path = "/"
	if segments := m.URIPath(); len(segments) > 0 {
		path = "/" + strings.Join(segments, "/")
	}
	query = url.Values{}
	if args := m.URIQuery(); len(args) > 0 {
		if parsed, err := url.ParseQuery(strings.Join(args, "&")); err == nil {
			query = parsed
		}
	}
	return path, query
}
