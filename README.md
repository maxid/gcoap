English | [简体中文](README.zh-CN.md)

# gcoap

A minimal **CoAP server state machine** for Go, built on the
[gnety](https://github.com/maxid/gnety) network framework.

> Status: **development snapshot (untagged)** — request routing,
> confirmable/non-confirmable responses, Observe/Notify and block-wise transfer
> on both sides of the wire, with bounded Block1 uploads, a concurrency-safe
> client and low-cardinality metrics. DTLS is available.
> No tags are published until a stable release; use commit pseudo-versions.

## Features

- CoAP wire format via `gnety/codec/coap` (no duplicated packet codec)
- UDP transport via gnety
- Request routing by method + URI path
- CON → ACK piggyback response, NON → NON response
- Query parsing and URI-path options
- Observe/Notify (RFC 7641): register, notify, cancel, plus opt-in confirmable
  notifications with retransmission, ACK/RST handling and a bounded pending
  notification per observation
- Block-wise transfer (RFC 7959): Block2 reads and Block1 writes on the server,
  Block1 request-body upload and Block2 response assembly on the client
- Client state machine: `Dial` + `Do` (CON retransmission, automatic Block1
  upload and Block2 assembly, configurable block size and body limit)
- Client Observe (RFC 7641): `Observe` registers and returns the registration
  response, notifications arrive on a callback with the sequence-number rules of
  section 3.4 (stale notifications are dropped, confirmable ones are ACKed),
  `Observation.Cancel` or context cancellation deregisters
- Separate responses (RFC 7252 section 5.2): a handler that exceeds `ACK_TIMEOUT`
  gets an empty ACK, then its response is sent as a new CON message and
  retransmitted; `Request.RespondLater` lets a handler choose this explicitly
- RFC 7252 exchange layer: message-id deduplication per peer (bounded cache,
  exchange lifetime, empty-ACK replay for pending separate responses),
  exponential-backoff retransmission with `ACK_RANDOM_FACTOR` jitter and
  `MAX_RETRANSMIT`, cancellable handler contexts
- DTLS transport (server/client) via gnety
- Bounded Block1 uploads: `MaxBlockUploads`, `MaxBlockPayload` (with Size1
  pre-check), `BlockIdleTimeout`; a repeated block replays 2.31 and a repeated
  final block replays the cached final response; over-limit, out-of-order and
  method/URI-changing blocks are answered with 4.13, 4.08 or 5.03
- Server-initiated Block2: a response body larger than `BlockSize` is sliced even
  when the request carried no Block2 option
- `Client.Do` is safe for concurrent use: every request owns its token and
  message id, so responses never cross between callers
- `Metrics` hooks on the server and the client (nil by default) for exchanges,
  deduplication hits, retransmissions, observe events and block outcomes
- 4.04 for unknown routes

## Example

```go
srv, _ := gcoap.New(gcoap.Config{Addr: ":5683"})
srv.Handle(coap.CodeGET, "/hello", func(ctx context.Context, req gcoap.Request) gcoap.Response {
    return gcoap.Response{Code: coap.CodeContent, Payload: []byte("hello")}
})
_ = srv.Start(context.Background())
```

Client request and observation:

```go
cli, _ := gcoap.Dial(context.Background(), "127.0.0.1:5683")
defer cli.Close()

// A body larger than ClientConfig.BlockSize is uploaded with Block1.
resp, err := cli.Do(ctx, gcoap.ClientRequest{
    Code: coap.CodePUT, Path: "/config", Payload: body,
})

obs, initial, err := cli.Observe(ctx, gcoap.ClientRequest{
    Code: coap.CodeGET, Path: "/state",
}, func(resp gcoap.ClientResponse) {
    fmt.Println(string(resp.Payload)) // later notifications
})
defer obs.Cancel(context.Background())
fmt.Println(string(initial.Payload)) // registration response
```

## Runnable examples

[`examples/server-client`](examples/server-client/main.go) runs a server and a
client in one program: the server exposes `/hello`, the client sends a
confirmable GET and prints the response, and both sides stop. The server
listens on an ephemeral loopback port and prints the address it chose:

```sh
go run ./examples/server-client
go run ./examples/server-client -addr 127.0.0.1:5683 -path /hello
```

## Limits and metrics

| Field | Default | Meaning |
| --- | --- | --- |
| `Config.BlockSize` | 1024 | Block2 size a large response is sliced into |
| `Config.MaxObservers` | 256 | observers across all resources; beyond it a registration is 5.03 |
| `Config.MaxBlockUploads` | 128 | Block1 uploads in progress; beyond it a new upload is 5.03 |
| `Config.MaxBlockPayload` | 1 MiB | total body of one upload, also checked against Size1 (4.13) |
| `Config.BlockIdleTimeout` | 60s | idle upload and completed-response cleanup |
| `ClientConfig.MaxResponseSize` | 1 MiB | assembled Block2 body; beyond it `ErrResponseTooLarge` |

`Config.Metrics` and `ClientConfig.Metrics` receive low-cardinality events:
`Exchange(method, result, duration)`, `Dedup()`, `Retransmit(kind)`,
`Observe(event)` and `Block(direction, result)`. A nil hook costs nothing;
`NoopMetrics` is the explicit no-op. Labels never carry a token, URI path or
peer address.

## Extension points

The observability contract lives in [`hook`](hook/): `Metrics` and the coarse labels it
reports (`Result*`, `Retransmit*`, `Observe*`, `Block1`/`Block2` and the block results). The
root package re-exports every one of them as an alias, so the two import paths are the same
types and existing code needs no change:

```go
var m gcoap.Metrics = hook.MetricsFuncs{RecordExchange: nil} // literally the same type
```

`hook.MetricsFuncs` implements the five methods with optional function fields — set only the
events you care about, and nil fields are skipped. `hook.NoopMetrics` drops everything.

Handlers stay where they are (`Handler` is a plain func type with the server's
`Request`/`Response`), and `Server` exposes its own routing surface: those two are the
package's main extension points, and the metrics hook only observes them.

To make extensions switchable at run time — start, stop, inspect — wrap the server in a
plugin registry: `gplugin.NewRegistry(server)` from
[`gplugin`](https://github.com/maxid/gplugin) gives named plugins, a three-state lifecycle,
an observer and per-plugin start/stop; this package only defines what a plugin can plug
into.

## Interoperability tests

The tests against an independent CoAP implementation
(`github.com/plgd-dev/go-coap/v3`) live in the separate
[`interop`](interop/README.md) module, so this module's dependency graph carries
no third-party CoAP stack:

```sh
cd interop && go test ./...
```

They cover UDP and DTLS in both directions, including Block1, Block2 and Observe.

## Roadmap

- bounded Block1 upload state on the server (TTL, global cap) — done
- independent interoperability testing — done; weak-network testing remains
- per-peer observer and upload quotas

## License

MIT. See [LICENSE](LICENSE).
