# Roadmap

- [x] UDP server via gnety
- [x] CoAP request routing (GET/POST/PUT/DELETE)
- [x] CON/ACK and NON responses
- [x] Query/URI-path parsing
- [x] Observe / notifications (minimal)
- [x] Block-wise transfer (Block1/Block2)
- [x] DTLS transport (server/client)
- [x] Client state machine (minimal: Dial/Do, retry, Block2)
- [x] RFC 7252 retransmission, message-id deduplication and exchange lifetime
- [x] Confirmable Observe notifications with ACK/RST handling and a bounded
      per-observation queue
- [x] Separate responses (empty ACK then a retransmitted CON response)
- [x] Client Block1 upload (size negotiation, Size1, early server response)
- [x] Client Observe (sequence tracking, NON/CON notifications, ACK, cancel)
- [x] Bounded Block1 upload state (TTL, size and count limits, duplicate replay,
      cached final response)
- [x] Server-initiated Block2 slicing for large response bodies
- [x] Concurrency-safe Client.Do and a configurable Block2 assembly limit
- [x] Metrics hooks (exchange, dedup, retransmit, observe, block)
- [x] Independent interoperability tests (plgd-dev/go-coap, UDP and DTLS,
      both directions)
- [x] Benchmarks over real loopback UDP and a `docs/PERFORMANCE.md` with the
      numbers, the reproduction command and the machine they were taken on
- [ ] Weak-network fault-injection matrix
- [x] Per-peer observer and upload quotas
