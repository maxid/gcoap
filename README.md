# gcoap

A minimal **CoAP server state machine** for Go, built on the
[gnety](https://github.com/maxid/gnety) network framework.

> Status: **development snapshot (untagged)** — request routing,
> confirmable/non-confirmable responses, Observe/Notify and block-wise transfer.
> DTLS is available; advanced client features (observe, Block1 assembly) are roadmap.
> No tags are published until a stable release; use commit pseudo-versions.

## Features

- CoAP wire format via `gnety/codec/coap` (no duplicated packet codec)
- UDP transport via gnety
- Request routing by method + URI path
- CON → ACK piggyback response, NON → NON response
- Query parsing and URI-path options
- Observe/Notify (RFC 7641 minimal): register, notify, cancel
- Block-wise transfer (RFC 7959): Block2 reads and Block1 writes
- Client state machine: `Dial` + `Do` (CON retransmission, automatic Block2 assembly)
- RFC 7252 exchange layer: message-id deduplication per peer (bounded cache,
  exchange lifetime), exponential-backoff retransmission with `ACK_RANDOM_FACTOR`
  jitter and `MAX_RETRANSMIT`, cancellable handler contexts
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

## Roadmap

- separate responses and confirmable (retransmitted) notifications
- bounded Observe/Block state and advanced client Observe/Block1 support
- independent interoperability and weak-network testing

## License

MIT. See [LICENSE](LICENSE).
