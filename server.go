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
// stops.
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

// onMessage serves one request datagram. A confirmable request is answered from
// the deduplication cache when the peer retransmits it, so the handler runs once
// per exchange (RFC 7252 section 4.2); a non-confirmable request is never
// deduplicated because it is not acknowledged.
func (s *Server) onMessage(sess *gnety.Session, msg any) {
	m, ok := msg.(coap.Message)
	if !ok || m.Code < coap.CodeGET || m.Code > coap.CodeDELETE || sess == nil {
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
	out, answered := s.respond(sess, m)
	if !answered {
		if confirmable {
			s.dedup.forget(key)
		}
		return
	}
	raw, err := out.Marshal()
	if err == nil {
		err = sess.Send(raw)
	}
	if err != nil {
		if confirmable {
			s.dedup.forget(key)
		}
		return
	}
	if confirmable {
		s.dedup.complete(key, raw)
	}
}

// respond runs the handler for one request and builds the response to send.
// answered is false when the request produced no immediate response (a handler
// that will answer with a separate response); the caller must then not cache
// anything for the exchange.
func (s *Server) respond(sess *gnety.Session, m coap.Message) (coap.Message, bool) {
	path, query := parsePath(m)
	var finalBlock1 *coap.Option
	if m.Code == coap.CodePUT {
		if b, ok, err := requestBlock(m, coap.OptionBlock1); err != nil {
			return responseMessage(s.nextID.Add, m, Response{Code: coap.CodeBadRequest}), true
		} else if ok {
			full, final, err := s.processBlock1(sess, m, b)
			if err != nil {
				return responseMessage(s.nextID.Add, m, Response{Code: coap.CodeBadRequest}), true
			}
			// 2.31 acknowledges the block just accepted; on the final block the same
			// option rides on the handler's response below.
			option, err := blockOption(coap.OptionBlock1, final)
			if err != nil {
				return responseMessage(s.nextID.Add, m, Response{Code: coap.CodeBadRequest}), true
			}
			if b.More {
				return responseMessage(s.nextID.Add, m, Response{Code: coap.CodeContinue, Options: []coap.Option{option}}), true
			}
			m.Payload = full
			finalBlock1 = &option
		}
	}
	obs, hasObs := m.Observe()
	h := s.match(m.Code, path)
	if h != nil && m.Code == coap.CodeGET && hasObs {
		if obs == coap.ObserveDeregister {
			s.cancelObserver(sess, m.Token, path)
		} else {
			s.addObserver(sess, m.Token, path)
		}
	}
	resp := Response{Code: coap.CodeNotFound}
	if h != nil {
		ctx, done := s.handlerContext(sess)
		resp = h(ctx, Request{
			Message: m, Path: path, Query: query,
			Payload: append([]byte(nil), m.Payload...), Session: sess,
		})
		done()
	}
	if resp.Code == 0 {
		resp.Code = coap.CodeContent
	}
	if m.Code == coap.CodeGET {
		if b, ok, err := requestBlock(m, coap.OptionBlock2); ok {
			var chunk []byte
			var option coap.Option
			if err == nil {
				var outBlock coap.Block
				chunk, outBlock, err = sliceBlock(resp.Payload, b)
				if err == nil {
					option, err = blockOption(coap.OptionBlock2, outBlock)
				}
			}
			if err != nil {
				resp.Code = coap.CodeBadRequest
				resp.Payload = nil
			} else {
				resp.Payload = chunk
				resp.Options = append(resp.Options, option)
			}
		}
	}
	if finalBlock1 != nil {
		resp.Options = append(resp.Options, *finalBlock1)
	}
	if m.Code == coap.CodeGET && hasObs && obs == coap.ObserveRegister && h != nil {
		resp.Options = append(resp.Options, observeOption(coap.ObserveRegister))
	}
	return responseMessage(s.nextID.Add, m, resp), true
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
