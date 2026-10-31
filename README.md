English | [简体中文](README.zh-CN.md)

# gcoap

A minimal **CoAP server state machine** for Go, built on the
[gnety](https://github.com/maxid/gnety) network framework.

> Status: **development snapshot (untagged)** — request routing,
> confirmable/non-confirmable responses, Observe/Notify and block-wise transfer
> on both sides of the wire. DTLS is available.
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

## Roadmap

- bounded Block1 upload state on the server (TTL, global cap)
- independent interoperability and weak-network testing

## License

MIT. See [LICENSE](LICENSE).
