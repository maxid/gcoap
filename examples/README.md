English | [简体中文](README.zh-CN.md)

# Examples

Two runnable programs, each answering one question. Both are self-contained `main`
packages, so `go run ./examples/<name>` from the repository root is enough — no
third-party CoAP server, no configuration file. Each one starts the gcoap server it
talks to, on a loopback port, so the two can run next to each other and next to a
real CoAP service.

- [server-client](#server-client) — what a confirmable request/response round trip
  looks like, what an unknown path gets, and how to point an external CoAP client at
  a gcoap server.
- [observe-client](#observe-client) — how CoAP observation works: one registration,
  then notifications without any further request.

## Common conventions

- Each example prints one line per step — `[n/N] action … result` — and `[done] …`
  when it finishes.
- Failures go to stderr as `<example>: <action>: <error>` with a non-zero exit code.
- Ctrl-C stops either example. The scripted run prints `[stop] interrupted`, the
  long-running `-serve` mode prints `[stop] server stopped`; both exit 0 — an
  example is not a service.
- `-h` prints what the example answers plus commands you can copy verbatim.
- The scripted runs listen on an ephemeral loopback port and print the address they
  chose. `-serve` uses the CoAP port `127.0.0.1:5683` and moves to the next free UDP
  port when it is taken, so the address it prints is always the one to use.

## server-client

**Answers:** what a request/response round trip costs, and what a server answers on
a path nobody registered. The program starts a server with one GET resource, sends a
confirmable GET to it, then GETs `/missing` to show the 4.04.

```sh
go run ./examples/server-client
go run ./examples/server-client -path /state -timeout 2s
go run ./examples/server-client -serve   # then: coap-client -m get coap://127.0.0.1:5683/hello
```

Expected:

```
[1/4] server listening on 127.0.0.1:59983, resource GET /hello
[2/4] client dialed 127.0.0.1:59983
[3/4] CON GET /hello -> 2.05 "hello from gcoap for /hello"
[4/4] GET /missing -> 4.04 (every unknown route is 4.04)
[done] confirmable round trip complete; server and client closed
```

`-serve` skips the client and holds the server open instead. That is the manual
interoperability mode: libcoap's `coap-client`, another gcoap process or any other
CoAP implementation can query the resource until Ctrl-C.

## observe-client

**Answers:** how an observation is registered, what arrives afterwards without a
request, and how it is cancelled. The server exposes one resource whose value it
changes itself every `-interval`; the client registers with a GET carrying
`Observe=0`, prints the registration response plus `-count` notifications, then
sends `Observe=1` to deregister.

```sh
go run ./examples/observe-client
go run ./examples/observe-client -interval 100ms -count 5
go run ./examples/observe-client -serve   # then: coap-client -m get -s 30 coap://127.0.0.1:5683/sensor
```

Expected:

```
[1/5] server listening on 127.0.0.1:51015, resource GET /sensor
[2/5] client dialed 127.0.0.1:51015
[3/5] registered: 2.05 "value=0" (the registration response is the current state)
[4/5] notification 1: 2.05 "value=1"
[4/5] notification 2: 2.05 "value=2"
[4/5] notification 3: 2.05 "value=3"
[5/5] deregistered after 3 notifications
[done] observation cancelled; server and client closed
```

Two details the output does not show:

- RFC 7641 section 3.2 makes the registration response the first notification, which
  is why `Client.Observe` returns it instead of passing it to the callback. Every
  later notification carries an `Observe` option with an increasing sequence number;
  `gcoap.Observation` drops a stale one and acknowledges it when it is confirmable.
- The callback runs on the client's receive goroutine, in arrival order. It must not
  block, so this example only forwards to a buffered channel and prints elsewhere.

[`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md) describes the observe state machine
on both sides; the [README](../README.md) lists the observer and notification limits.
