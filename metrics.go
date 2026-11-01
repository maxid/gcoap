package gcoap

import (
	"context"
	"errors"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// Exchange result labels handed to Metrics.Exchange. They are deliberately
// coarse: a metric never carries a token, a URI path, a peer address or a
// payload.
const (
	// ResultOK is a 2.xx response.
	ResultOK = "ok"
	// ResultClientError is a 4.xx response.
	ResultClientError = "client_error"
	// ResultServerError is a 5.xx response.
	ResultServerError = "server_error"
	// ResultTimeout is an exchange that ended without any response in time.
	ResultTimeout = "timeout"
	// ResultError is any other failure: a reset, a closed client, a transport
	// error or a response the codec rejected.
	ResultError = "error"
)

// Retransmission kinds handed to Metrics.Retransmit.
const (
	RetransmitRequest      = "request"
	RetransmitResponse     = "response"
	RetransmitNotification = "notification"
)

// Observe events handed to Metrics.Observe. The server reports registered,
// cancelled, expired and notified (or failed); the client reports started,
// stopped and notification.
const (
	ObserveRegistered   = "registered"
	ObserveCancelled    = "cancelled"
	ObserveExpired      = "expired"
	ObserveNotified     = "notified"
	ObserveFailed       = "failed"
	ObserveStarted      = "started"
	ObserveStopped      = "stopped"
	ObserveNotification = "notification"
)

// Block directions and results handed to Metrics.Block.
const (
	Block1 = "1"
	Block2 = "2"

	BlockContinue  = "continue"
	BlockComplete  = "complete"
	BlockReplay    = "replay"
	BlockTooLarge  = "too_large"
	BlockTooMany   = "too_many"
	BlockSequence  = "sequence"
	BlockExpired   = "expired"
	BlockAssembled = "assembled"
	BlockError     = "error"
)

// Metrics receives low-cardinality protocol events from a server or a client.
// A nil Metrics costs nothing: every hook is skipped.
//
// Implementations must be safe for concurrent use and must not block; the hooks
// run on the transport, handler or caller goroutine. They are called outside the
// library's locks, so an implementation may call back into the library.
type Metrics interface {
	// Exchange reports one completed exchange. On the server, d is the time
	// from receiving the request to producing the response; on the client it is
	// the time Do spent, including block-wise follow-ups.
	Exchange(method coap.Code, result string, d time.Duration)
	// Dedup reports a duplicate confirmable request answered from the
	// deduplication cache.
	Dedup()
	// Retransmit reports one retransmission of kind RetransmitRequest,
	// RetransmitResponse or RetransmitNotification.
	Retransmit(kind string)
	// Observe reports one observe lifecycle event or notification outcome.
	Observe(event string)
	// Block reports one block-wise outcome: direction Block1 or Block2 and a
	// result such as BlockContinue, BlockComplete, BlockTooLarge or
	// BlockSequence.
	Block(direction, result string)
}

// NoopMetrics drops every event.
type NoopMetrics struct{}

// Exchange implements Metrics.
func (NoopMetrics) Exchange(coap.Code, string, time.Duration) {}

// Dedup implements Metrics.
func (NoopMetrics) Dedup() {}

// Retransmit implements Metrics.
func (NoopMetrics) Retransmit(string) {}

// Observe implements Metrics.
func (NoopMetrics) Observe(string) {}

// Block implements Metrics.
func (NoopMetrics) Block(string, string) {}

// resultOf maps a response code to the coarse exchange result label.
func resultOf(code coap.Code) string {
	switch code.Class() {
	case 2:
		return ResultOK
	case 4:
		return ResultClientError
	case 5:
		return ResultServerError
	default:
		return ResultError
	}
}

// resultOfError maps a failed exchange to the coarse result label.
func resultOfError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return ResultTimeout
	}
	return ResultError
}

// block1Result maps a Block1 failure to the metric label.
func block1Result(err error) string {
	var protocol *block1Error
	if !errors.As(err, &protocol) {
		return BlockError
	}
	switch protocol.code {
	case coap.CodeRequestEntityTooLarge:
		return BlockTooLarge
	case coap.CodeServiceUnavailable:
		return BlockTooMany
	case coap.CodeRequestEntityIncomplete:
		return BlockSequence
	default:
		return BlockError
	}
}

// serverMetric records one server-side event, skipping a nil hook.
func (s *Server) serverMetric() Metrics {
	if s.metrics == nil {
		return nil
	}
	return s.metrics
}

// recordExchange reports one served exchange.
func (s *Server) recordExchange(method coap.Code, code coap.Code, d time.Duration) {
	if m := s.serverMetric(); m != nil {
		m.Exchange(method, resultOf(code), d)
	}
}

// recordDedup reports one deduplication hit.
func (s *Server) recordDedup() {
	if m := s.serverMetric(); m != nil {
		m.Dedup()
	}
}

// recordRetransmit reports one server retransmission.
func (s *Server) recordRetransmit(kind string) {
	if m := s.serverMetric(); m != nil {
		m.Retransmit(kind)
	}
}

// recordObserve reports one observe event.
func (s *Server) recordObserve(event string) {
	if m := s.serverMetric(); m != nil {
		m.Observe(event)
	}
}

// recordBlock reports one block-wise outcome.
func (s *Server) recordBlock(direction, result string) {
	if m := s.serverMetric(); m != nil {
		m.Block(direction, result)
	}
}

// recordExchange reports one client exchange.
func (c *Client) recordExchange(method coap.Code, code coap.Code, err error, d time.Duration) {
	if c.metrics == nil {
		return
	}
	result := resultOf(code)
	if err != nil {
		result = resultOfError(err)
	}
	c.metrics.Exchange(method, result, d)
}

// recordRetransmit reports one client retransmission.
func (c *Client) recordRetransmit(kind string) {
	if c.metrics != nil {
		c.metrics.Retransmit(kind)
	}
}

// recordBlock reports one client block-wise outcome.
func (c *Client) recordBlock(direction, result string) {
	if c.metrics != nil {
		c.metrics.Block(direction, result)
	}
}

// recordObserve reports one client observe event.
func (c *Client) recordObserve(event string) {
	if c.metrics != nil {
		c.metrics.Observe(event)
	}
}
