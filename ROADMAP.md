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
