// Package hook is the extension contract of this package: the interfaces a consumer
// implements to observe a server or a client.
//
// It exists as its own package so the extension surface is one visible directory instead of
// a section of an implementation file. Nothing here imports the root package, so an
// implementation cannot reach internals it is not supposed to see, and the root package
// re-exports every name as an alias — existing code keeps using gcoap.Metrics.
package hook

import (
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

// MetricsFuncs implements Metrics with optional function fields, so a consumer implements
// only the events it cares about instead of five methods. A nil field is skipped.
//
// The field names carry a Func suffix because a field and a method cannot share a name on
// one type.
type MetricsFuncs struct {
	ExchangeFunc   func(method coap.Code, result string, d time.Duration)
	DedupFunc      func()
	RetransmitFunc func(kind string)
	ObserveFunc    func(event string)
	BlockFunc      func(direction, result string)
}

// Exchange calls ExchangeFunc when it is set.
func (m MetricsFuncs) Exchange(method coap.Code, result string, d time.Duration) {
	if m.ExchangeFunc != nil {
		m.ExchangeFunc(method, result, d)
	}
}

// Dedup calls DedupFunc when it is set.
func (m MetricsFuncs) Dedup() {
	if m.DedupFunc != nil {
		m.DedupFunc()
	}
}

// Retransmit calls RetransmitFunc when it is set.
func (m MetricsFuncs) Retransmit(kind string) {
	if m.RetransmitFunc != nil {
		m.RetransmitFunc(kind)
	}
}

// Observe calls ObserveFunc when it is set.
func (m MetricsFuncs) Observe(event string) {
	if m.ObserveFunc != nil {
		m.ObserveFunc(event)
	}
}

// Block calls BlockFunc when it is set.
func (m MetricsFuncs) Block(direction, result string) {
	if m.BlockFunc != nil {
		m.BlockFunc(direction, result)
	}
}
