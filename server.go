// Package gcoap is a minimal CoAP server state machine built on gnety.
//
// Wire encoding is provided by gnety/codec/coap; transport is gnety UDP/DTLS.
// Basic Observe and block-wise transfers are implemented.
package gcoap

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

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
}

// Server is a minimal CoAP server.
type Server struct {
	srv      *gnety.Server
	mu       sync.RWMutex
	routes   map[string]Handler
	prefixes map[string]Handler
	nextID   atomic.Uint32

	obsMu     sync.Mutex
	observers map[string][]*observer
	sessions  map[string]*gnety.Session

	blockMu sync.Mutex
	uploads map[string]*upload
}

// New creates a CoAP server.
func New(cfg Config) (*Server, error) {
	s := &Server{
		routes: make(map[string]Handler), prefixes: make(map[string]Handler),
		observers: make(map[string][]*observer), sessions: make(map[string]*gnety.Session),
		uploads: make(map[string]*upload),
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
func (s *Server) Start(ctx context.Context) error { return s.srv.Start(ctx) }

// Stop stops the listener.
func (s *Server) Stop(ctx context.Context) error { return s.srv.Stop(ctx) }

// Addr returns the listen address.
func (s *Server) Addr() string { return s.srv.Addr() }

func (s *Server) onMessage(sess *gnety.Session, msg any) {
	m, ok := msg.(coap.Message)
	if !ok || m.Code < coap.CodeGET || m.Code > coap.CodeDELETE {
		return
	}
	path, query := parsePath(m)
	var finalBlock1 *coap.Option
	if m.Code == coap.CodePUT {
		if b, ok, err := requestBlock(m, coap.OptionBlock1); err != nil {
			s.sendResponse(sess, m, Response{Code: coap.CodeBadRequest})
			return
		} else if ok {
			full, final, err := s.processBlock1(sess, m, b)
			if err != nil {
				s.sendResponse(sess, m, Response{Code: coap.CodeBadRequest})
				return
			}
			// 2.31 acknowledges the block just accepted; on the final block the same
			// option rides on the handler's response below.
			option, err := blockOption(coap.OptionBlock1, final)
			if err != nil {
				s.sendResponse(sess, m, Response{Code: coap.CodeBadRequest})
				return
			}
			if b.More {
				s.sendResponse(sess, m, Response{Code: coap.CodeContinue, Options: []coap.Option{option}})
				return
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
		resp = h(context.Background(), Request{
			Message: m, Path: path, Query: query,
			Payload: append([]byte(nil), m.Payload...), Session: sess,
		})
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
	out := coap.Message{
		Type:      coap.Acknowledgement,
		Code:      resp.Code,
		MessageID: m.MessageID,
		Token:     append([]byte(nil), m.Token...),
		Options:   resp.Options,
		Payload:   resp.Payload,
	}
	if m.Type == coap.NonConfirmable {
		out.Type = coap.NonConfirmable
		out.MessageID = uint16(s.nextID.Add(1))
	}
	if m.Code == coap.CodeGET && hasObs && obs == coap.ObserveRegister && h != nil {
		out.Options = append(out.Options, observeOption(coap.ObserveRegister))
	}
	_ = sess.Send(out)
}

func (s *Server) sendResponse(sess *gnety.Session, req coap.Message, resp Response) {
	if resp.Code == 0 {
		resp.Code = coap.CodeContent
	}
	out := coap.Message{
		Type: coap.Acknowledgement, Code: resp.Code, MessageID: req.MessageID,
		Token: append([]byte(nil), req.Token...), Options: resp.Options, Payload: resp.Payload,
	}
	if req.Type == coap.NonConfirmable {
		out.Type = coap.NonConfirmable
		out.MessageID = uint16(s.nextID.Add(1))
	}
	_ = sess.Send(out)
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
