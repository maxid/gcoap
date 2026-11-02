package gcoap

import (
	"testing"

	"github.com/maxid/gcoap/hook"
)

// The extension contract is defined in gcoap/hook and re-exported here as aliases. These
// assertions fail to compile the moment one of them becomes a distinct named type, which
// is the compatibility promise this package makes: importing gcoap and importing
// gcoap/hook give you the same types.
var (
	_ hook.Metrics      = (Metrics)(nil)
	_ hook.MetricsFuncs = MetricsFuncs{}
	_ hook.NoopMetrics  = NoopMetrics{}

	_ Metrics      = hook.NoopMetrics{}
	_ Metrics      = hook.MetricsFuncs{}
	_ MetricsFuncs = hook.MetricsFuncs{}
)

func TestMetricLabelsAreAliased(t *testing.T) {
	for _, pair := range [][2]string{
		{ResultOK, hook.ResultOK},
		{ResultClientError, hook.ResultClientError},
		{ResultServerError, hook.ResultServerError},
		{ResultTimeout, hook.ResultTimeout},
		{ResultError, hook.ResultError},
		{RetransmitRequest, hook.RetransmitRequest},
		{RetransmitResponse, hook.RetransmitResponse},
		{RetransmitNotification, hook.RetransmitNotification},
		{ObserveRegistered, hook.ObserveRegistered},
		{ObserveCancelled, hook.ObserveCancelled},
		{ObserveExpired, hook.ObserveExpired},
		{ObserveNotified, hook.ObserveNotified},
		{ObserveFailed, hook.ObserveFailed},
		{ObserveStarted, hook.ObserveStarted},
		{ObserveStopped, hook.ObserveStopped},
		{ObserveNotification, hook.ObserveNotification},
		{Block1, hook.Block1},
		{Block2, hook.Block2},
		{BlockContinue, hook.BlockContinue},
		{BlockComplete, hook.BlockComplete},
		{BlockReplay, hook.BlockReplay},
		{BlockTooLarge, hook.BlockTooLarge},
		{BlockTooMany, hook.BlockTooMany},
		{BlockSequence, hook.BlockSequence},
		{BlockExpired, hook.BlockExpired},
		{BlockAssembled, hook.BlockAssembled},
		{BlockError, hook.BlockError},
	} {
		if pair[0] != pair[1] {
			t.Fatalf("label mismatch: %q != %q", pair[0], pair[1])
		}
	}
}

func TestMetricsFuncsSkipNilFields(t *testing.T) {
	var metrics Metrics = hook.MetricsFuncs{}
	// Every call with a nil field must be a no-op, not a panic.
	metrics.Exchange(0, ResultOK, 0)
	metrics.Dedup()
	metrics.Retransmit(RetransmitRequest)
	metrics.Observe(ObserveRegistered)
	metrics.Block(Block1, BlockComplete)
}
