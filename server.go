// Package gcoap is a minimal CoAP server state machine built on gnety.
//
// Wire encoding is provided by gnety/codec/coap; transport is gnety UDP.
// Observe/block-wise/DTLS are not implemented yet.
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
	Addr string
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
}

// New creates a CoAP server.
func New(cfg Config) (*Server, error) {
	s := &Server{
		routes: make(map[string]Handler), prefixes: make(map[string]Handler),
		observers: make(map[string][]*observer), sessions: make(map[string]*gnety.Session),
	}
	srv, err := gnety.New(gnety.Config{
		Network:       "udp",
		Addr:          cfg.Addr,
		DatagramCodec: datagramCodec{},
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
	obs, hasObs := observeValue(m)
	h := s.match(m.Code, path)
	if h != nil && m.Code == coap.CodeGET && hasObs {
		if obs == 1 {
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
	if m.Code == coap.CodeGET && hasObs && obs == 0 && h != nil {
		out.Options = append(out.Options, coap.Option{
			Number: coap.OptionObserve,
			Value:  []byte{0, 0, 0},
		})
	}
	_ = sess.Send(out)
}

func routeKey(method coap.Code, path string) string {
	return fmt.Sprintf("%d %s", method, path)
}

func parsePath(m coap.Message) (path string, query url.Values) {
	var segs, qs []string
	for _, opt := range m.Options {
		switch opt.Number {
		case coap.OptionURIPath:
			segs = append(segs, string(opt.Value))
		case coap.OptionURIQuery:
			qs = append(qs, string(opt.Value))
		}
	}
	path = "/"
	if len(segs) > 0 {
		path = "/" + strings.Join(segs, "/")
	}
	query = url.Values{}
	if len(qs) > 0 {
		if parsed, err := url.ParseQuery(strings.Join(qs, "&")); err == nil {
			query = parsed
		}
	}
	return path, query
}

type datagramCodec struct{}

func (datagramCodec) Decode(data []byte) (any, error) { return coap.Parse(data) }
func (datagramCodec) Encode(msg any) ([]byte, error) {
	m, ok := msg.(coap.Message)
	if !ok {
		return nil, fmt.Errorf("gcoap: expected coap.Message, got %T", msg)
	}
	return m.Marshal()
}
