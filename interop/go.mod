// This module holds the interoperability tests against an independent CoAP
// implementation. It is deliberately separate so the library's module graph
// does not carry a third-party CoAP dependency.
module github.com/maxid/gcoap/interop

go 1.27

require (
	github.com/maxid/gcoap v0.0.0
	github.com/maxid/gnety v0.0.0-20261101091924-9df1f8681c89
	github.com/pion/dtls/v3 v3.1.15
	github.com/plgd-dev/go-coap/v3 v3.5.9
)

require (
	github.com/dsnet/golib/memfile v1.0.0 // indirect
	github.com/pion/dtls/v2 v2.2.12 // indirect
	github.com/pion/logging v0.2.4 // indirect
	github.com/pion/transport/v2 v2.2.4 // indirect
	github.com/pion/transport/v5 v5.1.4 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/exp v0.0.0-20261014143641-f0251eb4aa3e // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)

replace github.com/maxid/gcoap => ../
