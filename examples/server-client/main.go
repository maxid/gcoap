// Command server-client runs a gcoap server and a client in one program: the
// server exposes one resource, the client sends a confirmable GET and prints the
// response, then asks for a path that does not exist to show the 4.04 an unknown
// route produces.
//
// The server listens on an ephemeral loopback port by default, so the example is
// safe to run next to a real CoAP service and prints the address it chose. With
// -serve it skips the client and holds the server open instead, so an external
// CoAP client — libcoap's coap-client, another gcoap process — can query it until
// Ctrl-C.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/maxid/gcoap"
	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

const steps = 4

func main() {
	addr := flag.String("addr", "", "server UDP listen address; empty means 127.0.0.1:0 (demo) or 127.0.0.1:5683, moving to the next free port when it is taken (-serve)")
	path := flag.String("path", "/hello", "resource path the server exposes and the client GETs")
	serve := flag.Bool("serve", false, "hold the server open for an external CoAP client instead of running the scripted round trip; Ctrl-C stops it")
	timeout := flag.Duration("timeout", 10*time.Second, "overall deadline of the scripted round trip; -serve is not affected")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `server-client runs a gcoap server and a client in this process and prints
one line per step. The client sends a confirmable GET to the resource the server
registers and then GETs a path nobody registered, which answers 4.04.

Examples:
  go run ./examples/server-client
  go run ./examples/server-client -path /state -timeout 2s
  go run ./examples/server-client -serve    # then: coap-client -m get coap://127.0.0.1:5683/hello

Flags:
`)
		flag.PrintDefaults()
	}
	flag.Parse()

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
				log.Fatalf("server-client: find a free port above %d: %v", preferred, err)
			}
			if port != preferred {
				fmt.Printf("[serve] 127.0.0.1:%d is taken; using 127.0.0.1:%d\n", preferred, port)
			}
			listen = "127.0.0.1:" + strconv.Itoa(port)
		}
	}

	srv, err := gcoap.New(gcoap.Config{Addr: listen})
	if err != nil {
		log.Fatalf("server-client: create server: %v", err)
	}
	srv.Handle(coap.CodeGET, *path, func(ctx context.Context, req gcoap.Request) gcoap.Response {
		return gcoap.Response{Code: coap.CodeContent, Payload: []byte("hello from gcoap for " + req.Path)}
	})
	if err := srv.Start(sigCtx); err != nil {
		log.Fatalf("server-client: start server on %s: %v", listen, err)
	}
	// Stop takes the cancelled ctx off the table, so Ctrl-C still closes it.
	defer func() { _ = srv.Stop(context.Background()) }()

	if *serve {
		fmt.Printf("[serve] serving GET %s on %s\n", *path, srv.Addr())
		fmt.Printf("[serve] try: coap-client -m get coap://%s%s\n", srv.Addr(), *path)
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
		interruptedOrFatal(ctx, "%s: dial: %v", srv.Addr(), err)
	}
	defer func() { _ = cli.Close() }()
	fmt.Printf("[2/%d] client dialed %s\n", steps, srv.Addr())

	resp, err := cli.Do(ctx, gcoap.ClientRequest{Code: coap.CodeGET, Path: *path})
	if err != nil {
		interruptedOrFatal(ctx, "GET %s: %v", *path, err)
	}
	fmt.Printf("[3/%d] CON GET %s -> %s %q\n", steps, *path, resp.Code, resp.Payload)

	missing, err := cli.Do(ctx, gcoap.ClientRequest{Code: coap.CodeGET, Path: "/missing"})
	if err != nil {
		interruptedOrFatal(ctx, "GET /missing: %v", err)
	}
	fmt.Printf("[4/%d] GET /missing -> %s (every unknown route is 4.04)\n", steps, missing.Code)

	if err := cli.Close(); err != nil {
		log.Fatalf("server-client: close client: %v", err)
	}
	fmt.Println("[done] confirmable round trip complete; server and client closed")
}

// interruptedOrFatal prints the [stop] line and returns when the failure is a
// Ctrl-C (cancelling the context is how the signal reaches an in-flight request),
// and exits non-zero for a real error.
func interruptedOrFatal(ctx context.Context, format string, args ...any) {
	if ctx.Err() != nil {
		fmt.Println("[stop] interrupted")
		os.Exit(0)
	}
	log.Fatalf("server-client: "+format, args...)
}
