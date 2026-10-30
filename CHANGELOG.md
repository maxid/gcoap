# Changelog

> Development snapshots are untagged; entries below are chronological.

## [Unreleased]

- Minimal CoAP server on gnety UDP: routing, CON/ACK and NON responses,
  query/path parsing, 4.04 handling.
- Observe/Notify: register (Observe=0), notifications, cancel (Observe=1).
- Block-wise transfer: Block2 response slicing and Block1 upload assembly.
- Client (`Dial`/`Do`) on gnety client connector: CON retry, token matching, Block2 assembly.
- DTLS server/client configuration via gnety.
- Options, blocks and observe values are decoded and encoded through the typed
  `gnety/codec/coap` accessors; gcoap keeps the exchange, observe-registry and
  block-transfer state plus its compact Block and fixed three-byte Observe wire
  forms.
