# interop

Interoperability tests against an independent CoAP implementation
([`plgd-dev/go-coap/v3`](https://github.com/plgd-dev/go-coap)).

The tests live in their own Go module so the library's module graph does not
carry a third-party CoAP stack. The module replaces `github.com/maxid/gcoap`
with the parent directory, so it always exercises the working copy and is meant
to be run from a checkout, never published on its own:

```sh
cd interop && go test ./...
```

Coverage:

- this client against the third-party server and the third-party client against
  this server, over UDP and DTLS
- Block1 uploads, Block2 downloads, Observe (UDP)
- a self-signed certificate for the DTLS directions

CI runs this module separately. When the parent module changes its dependency
pins, refresh the sums here:

```sh
cd interop && go mod tidy
```
