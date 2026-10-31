// Command server-client runs a gcoap server and a client in one program: the
// server exposes one resource, the client issues a confirmable GET against it,
// prints the response and both sides stop.
//
// The server listens on an ephemeral loopback port by default, so the example
// is safe to run next to a real CoAP service and prints the address it chose.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/maxid/gcoap"
	"github.com/maxid/gnety/codec/coap"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "server UDP listen address; port 0 picks an ephemeral port")
	path := flag.String("path", "/hello", "resource path the server exposes and the client GETs")
	timeout := flag.Duration("timeout", 5*time.Second, "overall deadline for the round trip")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	srv, err := gcoap.New(gcoap.Config{Addr: *addr})
	if err != nil {
		log.Fatalf("gcoap: create server: %v", err)
	}
	srv.Handle(coap.CodeGET, *path, func(ctx context.Context, req gcoap.Request) gcoap.Response {
		return gcoap.Response{Code: coap.CodeContent, Payload: []byte("hello from gcoap for " + req.Path)}
	})
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("gcoap: start server: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()
	fmt.Printf("server listening on %s (GET %s)\n", srv.Addr(), *path)

	cli, err := gcoap.Dial(ctx, srv.Addr())
	if err != nil {
		log.Fatalf("gcoap: dial %s: %v", srv.Addr(), err)
	}
	defer cli.Close()

	resp, err := cli.Do(ctx, gcoap.ClientRequest{Code: coap.CodeGET, Path: *path})
	if err != nil {
		log.Fatalf("gcoap: GET %s: %v", *path, err)
	}
	fmt.Printf("GET %s -> %s %q\n", *path, resp.Code, resp.Payload)

	missing, err := cli.Do(ctx, gcoap.ClientRequest{Code: coap.CodeGET, Path: "/missing"})
	if err != nil {
		log.Fatalf("gcoap: GET /missing: %v", err)
	}
	fmt.Printf("GET /missing -> %s (unknown routes are 4.04)\n", missing.Code)
}
