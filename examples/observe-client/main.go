// Command observe-client runs a gcoap server and a client in one program and
// demonstrates CoAP observation (RFC 7641): the server exposes a resource whose
// value changes, the client registers an observation on it, prints the
// registration response plus three notifications, then deregisters.
//
// The point of observe — the one CoAP feature MQTT has no exact equivalent for —
// is that the registration response is the current state and every later
// notification arrives without another request.
//
// The server listens on an ephemeral loopback port by default, so the example is
// safe to run next to a real CoAP service and prints the address it chose. With
// -serve it keeps publishing values and holds the server open instead, so an
// external CoAP client — libcoap's coap-client -s — can observe it until Ctrl-C.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/maxid/gcoap"
	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

const steps = 5

func main() {
	addr := flag.String("addr", "", "server UDP listen address; empty means 127.0.0.1:0 (demo) or 127.0.0.1:5683, moving to the next free port when it is taken (-serve)")
	path := flag.String("path", "/sensor", "resource path the server exposes and the client observes")
	interval := flag.Duration("interval", 250*time.Millisecond, "how often the server publishes a new value")
	count := flag.Int("count", 3, "notifications the scripted client waits for before deregistering")
	serve := flag.Bool("serve", false, "hold the server open and keep publishing for an external CoAP client instead of running the scripted round trip; Ctrl-C stops it")
	timeout := flag.Duration("timeout", 10*time.Second, "overall deadline of the scripted round trip; -serve is not affected")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `observe-client registers a CoAP observation and prints what arrives.

The server exposes one resource whose value the server itself changes. The
client subscribes to it with a GET carrying Observe=0, prints the registration
response (the current state), the notifications that follow without any further
request, and then sends Observe=1 to deregister.

Examples:
  go run ./examples/observe-client
  go run ./examples/observe-client -interval 100ms -count 5
  go run ./examples/observe-client -serve    # then: coap-client -m get -s 30 coap://127.0.0.1:5683/sensor

Flags:
`)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *count < 1 {
		log.Fatalf("observe-client: -count %d must be at least 1", *count)
	}
	if *interval <= 0 {
		log.Fatalf("observe-client: -interval %s must be positive", *interval)
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listen := *addr
	if listen == "" {
		listen = "127.0.0.1:0"
		if *serve {
			// A fixed port is the point of -serve: it goes into another
			// program's command line, so it must not be ephemeral. 5683 is the
			// CoAP port and may already be taken on this machine; then the next
			// free UDP port is announced instead. The probe is inherently racy
			// (bind, close, bind), so a Start failure is still reported below.
			const preferred = 5683
			port, err := gnety.AvailableUDPPortOn("127.0.0.1", preferred, nil)
			if err != nil {
				log.Fatalf("observe-client: find a free port above %d: %v", preferred, err)
			}
			if port != preferred {
				fmt.Printf("[serve] 127.0.0.1:%d is taken; using 127.0.0.1:%d\n", preferred, port)
			}
			listen = "127.0.0.1:" + strconv.Itoa(port)
		}
	}

	// The resource value is shared by the handler (which answers the registration
	// GET) and the publisher (which pushes notifications), which is the whole
	// state this example needs.
	var value atomic.Int64

	srv, err := gcoap.New(gcoap.Config{Addr: listen})
	if err != nil {
		log.Fatalf("observe-client: create server: %v", err)
	}
	srv.Handle(coap.CodeGET, *path, func(ctx context.Context, req gcoap.Request) gcoap.Response {
		return gcoap.Response{Code: coap.CodeContent, Payload: valuePayload(value.Load())}
	})
	if err := srv.Start(sigCtx); err != nil {
		log.Fatalf("observe-client: start server on %s: %v", listen, err)
	}
	// Stop takes the cancelled ctx off the table, so Ctrl-C still closes it.
	defer func() { _ = srv.Stop(context.Background()) }()
	go publish(sigCtx, srv, *path, *interval, &value)

	if *serve {
		fmt.Printf("[serve] observing GET %s on %s, a new value every %s\n", *path, srv.Addr(), *interval)
		fmt.Printf("[serve] try: coap-client -m get -s 30 coap://%s%s\n", srv.Addr(), *path)
		fmt.Println("[serve] Ctrl-C stops the server")
		<-sigCtx.Done()
		fmt.Println("[stop] server stopped")
		return
	}

	ctx, cancel := context.WithTimeout(sigCtx, *timeout)
	defer cancel()

	fmt.Printf("[1/%d] server listening on %s, resource GET %s\n", steps, srv.Addr(), *path)

	cli, err := gcoap.Dial(ctx, srv.Addr())
	if err != nil {
		interruptedOrFatal(ctx, "dial %s: %v", srv.Addr(), err)
	}
	defer func() { _ = cli.Close() }()
	fmt.Printf("[2/%d] client dialed %s\n", steps, srv.Addr())

	// The callback runs on the client's receive goroutine and must not block, so
	// notifications go to a buffered channel and this goroutine does the printing.
	notifications := make(chan gcoap.ClientResponse, *count+4)
	obs, initial, err := cli.Observe(ctx, gcoap.ClientRequest{Code: coap.CodeGET, Path: *path},
		func(resp gcoap.ClientResponse) {
			select {
			case notifications <- resp:
			default: // a slow printer never stalls the client
			}
		})
	if err != nil {
		interruptedOrFatal(ctx, "observe %s: %v", *path, err)
	}
	fmt.Printf("[3/%d] registered: %s %q (the registration response is the current state)\n",
		steps, initial.Code, initial.Payload)

	for i := 1; i <= *count; i++ {
		select {
		case n := <-notifications:
			fmt.Printf("[4/%d] notification %d: %s %q\n", steps, i, n.Code, n.Payload)
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				fmt.Println("[stop] interrupted")
				return
			}
			log.Fatalf("observe-client: wait for notification %d: %v", i, ctx.Err())
		}
	}

	// Deregister on a fresh context: the observation stopped with ctx, which may
	// already be cancelled by a deadline.
	deregisterCtx, deregisterCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer deregisterCancel()
	if err := obs.Cancel(deregisterCtx); err != nil {
		log.Fatalf("observe-client: deregister: %v", err)
	}
	fmt.Printf("[5/%d] deregistered after %d notifications\n", steps, *count)

	if err := cli.Close(); err != nil {
		log.Fatalf("observe-client: close client: %v", err)
	}
	fmt.Println("[done] observation cancelled; server and client closed")
}

// publish is the "device" side of the example: it advances the resource value
// and hands it to every observer of path. A real integration does exactly this
// from wherever its state changes.
func publish(ctx context.Context, srv *gcoap.Server, path string, interval time.Duration, value *atomic.Int64) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			srv.Notify(path, gcoap.Response{Code: coap.CodeContent, Payload: valuePayload(value.Add(1))})
		}
	}
}

func valuePayload(v int64) []byte {
	return []byte("value=" + strconv.FormatInt(v, 10))
}

// interruptedOrFatal prints the [stop] line and returns when the failure is a
// Ctrl-C (cancelling the context is how the signal reaches an in-flight request),
// and exits non-zero for a real error.
func interruptedOrFatal(ctx context.Context, format string, args ...any) {
	if ctx.Err() != nil {
		fmt.Println("[stop] interrupted")
		os.Exit(0)
	}
	log.Fatalf("observe-client: "+format, args...)
}
