# Changelog

> Development snapshots are untagged; entries below are chronological.

## [Unreleased]

### Changed

- **The metrics contract moved to `gcoap/hook`.** `Metrics`, `NoopMetrics` and every label
  (`Result*`, `Retransmit*`, `Observe*`, `Block1`/`Block2`, block results) are defined there
  and re-exported by the root package as aliases, so existing imports, names and interface
  assertions are unchanged. `gcoap/hook` imports nothing from the root package.

### Added

- `hook.MetricsFuncs`: implement the five metrics methods with optional function fields; a
  nil field is skipped.

- Block1 upload state is bounded: `Config.MaxBlockUploads` (128) and
  `Config.MaxBlockPayload` (1 MiB, also checked against a declared Size1) limit
  the state and memory one misbehaving client can occupy, and
  `Config.BlockIdleTimeout` (60s) expires idle uploads and completed responses.
  A block that changes the method, URI or block size, or arrives out of order,
  is answered with 4.08; an over-limit body with 4.13; a new upload beyond the
  limit with 5.03. A repeated block replays 2.31 without appending the body
  twice, and a repeated final block with a new message id replays the cached
  final response.
- A GET response larger than `Config.BlockSize` (1024) is sliced into Block2
  even when the request carried no Block2 option, so a body larger than the MTU
  is deliverable on a constrained link.
- `Client.Do` is safe for concurrent use: the global serialization mutex is gone,
  message ids avoid in-flight exchanges, and every caller keeps its own token.
  `ClientConfig.MaxResponseSize` (1 MiB) bounds Block2 assembly and reports
  `ErrResponseTooLarge`.
- `Config.Metrics` and `ClientConfig.Metrics` receive low-cardinality events
  (`Exchange`, `Dedup`, `Retransmit`, `Observe`, `Block`); a nil hook costs
  nothing and `NoopMetrics` is the explicit no-op.
- `Config.MaxObservers` (256) bounds observer registrations; beyond it a
  registration is answered with 5.03.
- Uploads are now cleaned up when their session closes (the old helper was never
  called).

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
- Client Block1 (C3): `Do` uploads a request body larger than
  `ClientConfig.BlockSize` (default 1024, power of two from 16 to 1024) as a
  Block1 sequence instead of one datagram. Every non-final block must be
  answered with 2.31 Continue; a smaller SZX in the answer downgrades the
  remaining blocks (a larger one is ignored), the block number is recomputed for
  the new size and no byte is skipped. `Size1` carries the total size, a server
  `Size1` below the body stops the upload, and `ClientConfig.MaxRequestSize`
  (default 1 MiB) rejects larger bodies with the new `ErrRequestTooLarge` before
  sending. An early error or final response on a non-final block ends the upload
  and is returned to the caller.
- Client Observe (C3): `Client.Observe` registers with Observe=0 and returns the
  registration response plus an `Observation`; notifications arrive on a
  callback. The per-observation sequence number follows RFC 7641 section 3.4, so
  stale notifications are dropped, NON notifications are tolerated and CON
  notifications are ACKed (stale ones too, to stop retransmission). The
  observation ends on context cancellation, `Observation.Cancel` (confirmable
  deregistration) or an error notification; no goroutine or map entry is left
  behind, and `Client.Close` ends every observation.
- The client now reads the connection from one goroutine and routes datagrams by
  token and message id, so an observation and a request can be in flight at the
  same time. `Do` stays serialized. Observable behavior changes: a request body
  above the BlockSize is sent block-wise, a body above MaxRequestSize is
  rejected, Block2 follow-up requests no longer repeat the request body, and a
  read failure after `Close` is reported as `ErrClientClosed`.
- The server reassembles Block1 request bodies for POST as well as PUT, which the
  client upload path needs for POST.
