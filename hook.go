package gcoap

import "github.com/maxid/gcoap/hook"

// The extension contract lives in ./hook: Metrics and the low-cardinality labels it
// reports. Every name below is an alias, so a consumer keeps importing this package and
// still gets the hook type — one definition, two import paths. New code may import
// github.com/maxid/gcoap/hook directly; both spellings are the same type.
type (
	Metrics      = hook.Metrics
	MetricsFuncs = hook.MetricsFuncs
	NoopMetrics  = hook.NoopMetrics
)

const (
	ResultOK          = hook.ResultOK
	ResultClientError = hook.ResultClientError
	ResultServerError = hook.ResultServerError
	ResultTimeout     = hook.ResultTimeout
	ResultError       = hook.ResultError

	RetransmitRequest      = hook.RetransmitRequest
	RetransmitResponse     = hook.RetransmitResponse
	RetransmitNotification = hook.RetransmitNotification

	ObserveRegistered   = hook.ObserveRegistered
	ObserveCancelled    = hook.ObserveCancelled
	ObserveExpired      = hook.ObserveExpired
	ObserveNotified     = hook.ObserveNotified
	ObserveFailed       = hook.ObserveFailed
	ObserveStarted      = hook.ObserveStarted
	ObserveStopped      = hook.ObserveStopped
	ObserveNotification = hook.ObserveNotification

	Block1 = hook.Block1
	Block2 = hook.Block2

	BlockContinue  = hook.BlockContinue
	BlockComplete  = hook.BlockComplete
	BlockReplay    = hook.BlockReplay
	BlockTooLarge  = hook.BlockTooLarge
	BlockTooMany   = hook.BlockTooMany
	BlockSequence  = hook.BlockSequence
	BlockExpired   = hook.BlockExpired
	BlockAssembled = hook.BlockAssembled
	BlockError     = hook.BlockError
)
