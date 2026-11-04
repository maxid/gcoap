package gcoap

import "testing"

// faultMatrix is the weak-network matrix: one entry per (fault, feature) pair.
// The runner turns each into a subtest named after the fault and the feature, so a
// failure points straight at the situation that broke — and a fault or feature
// with no entry is a gap someone can see.
//
// The scenarios live next to their helpers: the server side in fault_test.go, the
// client side in fault_client_test.go.
var faultMatrix = []struct {
	fault   string
	feature string
	run     func(t *testing.T)
}{
	{"duplicate", "request", testDuplicateRequest},
	{"duplicate", "non-confirmable", testNonConfirmableNotDeduplicated},
	{"loss", "separate", testSeparateResponseAckLost},
	{"delay", "separate", testSeparateResponseLateAck},
	{"reset", "separate", testSeparateResponseReset},
	{"loss", "observe", testNotificationAckLost},
	{"reset", "observe", testNotificationResetCancels},
	{"delay", "observe", testNotificationCoalescing},
	{"reorder", "block1", testBlock1Reordered},
	{"duplicate", "block1", testBlock1DuplicateFinal},
	{"reconnect", "request", testReconnectNotDeduplicated},
	// The client side: the same faults applied to the requests a gcoap client
	// sends and to the notifications it receives.
	{"loss", "client-request", testClientResponseLost},
	{"duplicate", "client-response", testClientDuplicateResponse},
	{"reset", "client-request", testClientReset},
	{"delay", "client-separate", testClientEmptyAckThenSeparate},
	{"reorder", "client-observe", testClientStaleNotification},
}

func TestFaultMatrix(t *testing.T) {
	for _, scenario := range faultMatrix {
		t.Run(scenario.fault+"/"+scenario.feature, func(t *testing.T) {
			scenario.run(t)
		})
	}
}
