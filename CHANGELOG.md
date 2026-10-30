# Changelog

> Development snapshots are untagged; entries below are chronological.

## [Unreleased]

- Minimal CoAP server on gnety UDP: routing, CON/ACK and NON responses,
  query/path parsing, 4.04 handling.
- Observe/Notify: register (Observe=0), notifications, cancel (Observe=1).
- Block-wise transfer: Block2 response slicing and Block1 upload assembly.
- Client (`Dial`/`Do`) on gnety client connector: CON retry, token matching, Block2 assembly.
- DTLS server/client configuration via gnety.
- RFC 7252 exchange layer (C1): inbound confirmable requests are deduplicated by
  peer identity plus message id in a bounded cache (`MaxExchanges`, exchange
  lifetime); a retransmitted CON replays the cached response byte for byte and
  never re-runs the handler. NON requests are not deduplicated.
- Outbound confirmable retransmission with exponential backoff, ACK_RANDOM_FACTOR
  jitter, MAX_RETRANSMIT and EXCHANGE_LIFETIME; an ACK or RST for the message id
  ends the exchange. The client uses this schedule instead of the fixed 1500 ms /
  two-attempt retry, stops retransmitting on an empty ACK and returns the new
  `ErrReset` when the peer resets the message.
- Handler contexts are cancelled when the exchange completes, its session closes
  or the server stops, instead of being `context.Background()`.
- `Config` and `ClientConfig` gain the RFC 7252 timing fields (`AckTimeout`,
  `AckRandomFactor`, `MaxRetransmit`, `ExchangeLifetime`); `Config` also gains
  `MaxExchanges`. Defaults stay at 2s, 1.5, 4, 247s and 1024.
- Options, blocks and observe values are decoded and encoded through the typed
  `gnety/codec/coap` accessors; gcoap keeps the exchange, observe-registry and
  block-transfer state plus its compact Block and fixed three-byte Observe wire
  forms.
- Confirmable notifications (C2): `Config.ConfirmableNotifications` sends an
  Observe notification as CON and retransmits it with the RFC 7252 schedule
  until ACK/RST or the retransmission limit. RST and the final timeout remove
  the observation (RFC 7641 section 4.5); an ACK releases at most one coalesced
  newer notification, so each observation holds one in-flight plus one pending
  message regardless of `Notify` frequency. Defaults stay NON.
- Separate responses (C3): a confirmable handler that takes longer than
  `ACK_TIMEOUT` gets an empty ACK, then its response is delivered as a new CON
  message with its own message id and retransmission schedule. The dedup cache
  replays that empty ACK to duplicate requests (never as the final answer) and
  keeps the handler single-run. `Request.RespondLater` exposes the same path
  explicitly; `Config.SeparateResponses` moves handlers off the transport read
  loop so duplicates can be answered while they run. The client now ACKs a
  separate CON response so the server stops retransmitting.
