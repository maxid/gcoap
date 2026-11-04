English | [简体中文](PERFORMANCE.zh-CN.md)

# Performance: one machine, loopback UDP

> Measured 2026-11-04 on the machine below. These are a **baseline, not a
> promise**: they say what the current code costs on one host, and they exist so a
> later change can be compared against something. Nothing here is a throughput
> target and no number is extrapolated to a real network.

## Machine and toolchain

| Item | Value |
|---|---|
| CPU | Apple M1 Max (10 cores: 8 performance + 2 efficiency) |
| OS / kernel | macOS (darwin/arm64) |
| Go | go1.27.1 |
| `GOMAXPROCS` | default (10); the parallel rows below set it with `-cpu` |
| Transport | loopback UDP (`127.0.0.1`), server and client in one process |
| Commit | `c632aa8` plus the benchmarks in `benchmark_test.go` |

A macOS laptop is not a quiet machine: run-to-run spread in these numbers is up to
roughly ±10 %, and the energy cores make parallel scaling look worse than it is on a
server CPU. Treat the third significant digit as noise.

## What is measured

`benchmark_test.go` starts a real gcoap server and a real gcoap client over loopback
UDP — no mocked transport — and times only the steady-state loop. Setup (listening,
dialing, registering the observation) runs outside the timer.

| Benchmark | Request | Median of 3 runs | Equivalent rate |
|---|---|---|---|
| `BenchmarkRequest` | one confirmable GET, 64-byte response, one at a time | 46.0 µs/op | 21.7k round trips/s |
| `BenchmarkRequestParallel` (`-cpu=4`) | the same, from every goroutine | 25.7 µs/op | 38.9k round trips/s |
| `BenchmarkRequestParallel` (`-cpu=10`) | the same, from every goroutine | 29.2 µs/op | 34.2k round trips/s |
| `BenchmarkRequestBlock2` | one GET with an 8 KiB response, assembled from 8 Block2 blocks | 431 µs/op | 2.3k requests/s, 19.0 MB/s |
| `BenchmarkObserveNotify/non-confirmable` | one observer, one NON notification, delivered to the callback | 25.0 µs/op | 40.0k notifications/s |
| `BenchmarkObserveNotify/confirmable` | one observer, one CON notification acknowledged by the client | 38.4 µs/op | 26.0k notifications/s |

Allocations per operation are the same in every run and are reported by the same
benchmarks:

| Benchmark | Bytes/op | Allocs/op |
|---|---|---|
| `BenchmarkRequest` | 68.6 kB | 64 |
| `BenchmarkRequestBlock2` | 594 kB | 600 |
| `BenchmarkObserveNotify/non-confirmable` | 66.7 kB | 26 |
| `BenchmarkObserveNotify/confirmable` | 67.4 kB | 47 |

## How to read it

- **A serial round trip costs about 46 µs**, and roughly half of that is the
  exchange bookkeeping around two loopback syscalls, not the wire. Serial throughput
  is therefore ~21k requests/s per client.
- **Throughput stops scaling around four cores** (38.9k requests/s at `-cpu=4`,
  34.2k at `-cpu=10`). That ceiling is structural, not accidental: one goroutine
  reads the socket and, by default, runs handlers on that same goroutine (see
  `docs/ARCHITECTURE.md`, "Server: one read loop, two execution modes"). More client
  goroutines cannot outrun it.
- **Block-wise transfer costs one round trip per block.** Eight KiB in eight blocks
  takes 431 µs — about nine times the small-request latency — and 19 MB/s is the
  price of a 1 KiB block size over loopback. A larger `Config.BlockSize` buys fewer
  round trips; MTU and constrained links cap it in practice.
- **A confirmable notification costs ~13 µs more than a non-confirmable one**,
  because it adds an ACK round trip and its own outbound exchange goroutine. It also
  paces itself: at most one notification per observer is in flight, so a slow client
  slows the stream instead of growing a queue.
- **Allocations are not optimized.** ~69 kB per request is dominated by per-datagram
  buffers on the gnety side and per-exchange state on the gcoap side; nothing is
  pooled. The numbers document the current cost; they are not a target.

## Reproduction

```sh
cd libs/gcoap

# Serial request, Block2 and observe notifications (median of 3 runs):
go test -run '^$' -bench 'BenchmarkRequest$|BenchmarkRequestBlock2$|BenchmarkObserveNotify' -benchtime=2s -count=3

# Incremental throughput: one, four and ten Ps:
go test -run '^$' -bench 'BenchmarkRequestParallel' -benchtime=2s -count=3 -cpu=1,4,10
```

`-run '^$'` keeps the unit tests out of the run; benchmarks are never part of
`go test ./...` and not part of CI, per the protocol-library baseline: numbers are a
manual, release-time activity.

## What these numbers do not cover

- **Real networks.** No packet loss, no RTT, no reordering, no NAT, no small MTU. The
  retransmission, deduplication and separate-response paths are exercised by the unit
  tests, not by these benchmarks.
- **DTLS.** All benchmarks run plain UDP; a DTLS handshake and record protection add
  cost that is not measured here.
- **Block1 uploads, and observe with block-wise bodies.** Block1 upload and Block2
  over observe are not benchmarked.
- **Server capacity.** One client, one observer, one process. The number of peers the
  server can hold is bounded by `Config.MaxExchanges`, `MaxObservers` and
  `MaxBlockUploads`, not measured by these benchmarks.
- **Metrics hooks.** No `Metrics` implementation is attached; a hook that allocates
  or blocks would change the numbers.
- **Other hardware.** ARM64 laptop figures do not transfer to a small ARM32 gateway,
  which is the deployment these numbers would be most interesting for.
