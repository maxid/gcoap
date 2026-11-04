package gcoap

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/coap"
)

// The benchmarks below run a real gcoap server and a real gcoap client over
// loopback UDP. Nothing is mocked: the numbers include the syscalls, the codec,
// the exchange bookkeeping and the goroutine handoff, because that is what a
// caller pays. They are the source of docs/PERFORMANCE.md; see that file for
// measured values, the machine and the caveats.

const benchPayloadSize = 64

func benchPayload() []byte {
	return bytes.Repeat([]byte{'x'}, benchPayloadSize)
}

// benchmarkPair starts a server with one GET resource and a client dialed to it,
// and tears both down when the benchmark ends.
func benchmarkPair(b *testing.B) (*Server, *Client) {
	b.Helper()
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		b.Fatalf("new server: %v", err)
	}
	payload := benchPayload()
	srv.Handle(coap.CodeGET, "/bench", func(_ context.Context, _ Request) Response {
		return Response{Code: coap.CodeContent, Payload: payload}
	})
	if err := srv.Start(context.Background()); err != nil {
		b.Fatalf("start server: %v", err)
	}
	b.Cleanup(func() { _ = srv.Stop(context.Background()) })

	cli, err := Dial(context.Background(), srv.Addr())
	if err != nil {
		b.Fatalf("dial %s: %v", srv.Addr(), err)
	}
	b.Cleanup(func() { _ = cli.Close() })
	return srv, cli
}

// BenchmarkRequest measures one confirmable request/response round trip, one at a
// time: the latency a serial caller sees, dominated by the two loopback syscalls
// and the exchange bookkeeping around them.
func BenchmarkRequest(b *testing.B) {
	_, cli := benchmarkPair(b)
	ctx := context.Background()
	req := ClientRequest{Code: coap.CodeGET, Path: "/bench"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := cli.Do(ctx, req)
		if err != nil {
			b.Fatalf("Do: %v", err)
		}
		if resp.Code != coap.CodeContent || len(resp.Payload) != benchPayloadSize {
			b.Fatalf("response = %s with %d bytes, want 2.05 with %d", resp.Code, len(resp.Payload), benchPayloadSize)
		}
	}
	b.StopTimer()
}

// BenchmarkRequestParallel measures the same round trip from every goroutine at
// once. Client.Do is concurrency-safe by contract, so this is the throughput
// number: it shows where the serial latency stops adding up.
func BenchmarkRequestParallel(b *testing.B) {
	_, cli := benchmarkPair(b)
	ctx := context.Background()
	req := ClientRequest{Code: coap.CodeGET, Path: "/bench"}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := cli.Do(ctx, req)
			if err != nil {
				b.Fatalf("Do: %v", err)
			}
			if resp.Code != coap.CodeContent {
				b.Fatalf("response code = %s, want 2.05", resp.Code)
			}
		}
	})
	b.StopTimer()
}

// BenchmarkRequestBlock2 measures a GET whose response exceeds the default block
// size, so the client assembles eight Block2 responses. Compare it with
// BenchmarkRequest to see what block-wise transfer costs per round trip.
func BenchmarkRequestBlock2(b *testing.B) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		b.Fatalf("new server: %v", err)
	}
	payload := bytes.Repeat([]byte{'y'}, 8*1024)
	srv.Handle(coap.CodeGET, "/bench", func(_ context.Context, _ Request) Response {
		return Response{Code: coap.CodeContent, Payload: payload}
	})
	if err := srv.Start(context.Background()); err != nil {
		b.Fatalf("start server: %v", err)
	}
	b.Cleanup(func() { _ = srv.Stop(context.Background()) })
	cli, err := Dial(context.Background(), srv.Addr())
	if err != nil {
		b.Fatalf("dial %s: %v", srv.Addr(), err)
	}
	b.Cleanup(func() { _ = cli.Close() })

	ctx := context.Background()
	req := ClientRequest{Code: coap.CodeGET, Path: "/bench"}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := cli.Do(ctx, req)
		if err != nil {
			b.Fatalf("Do: %v", err)
		}
		if len(resp.Payload) != len(payload) || !bytes.Equal(resp.Payload, payload) {
			b.Fatalf("assembled %d bytes, want %d", len(resp.Payload), len(payload))
		}
	}
	b.StopTimer()
}

// BenchmarkObserveNotify measures one notification end to end: Server.Notify on
// the writing side, the wire, and the observation callback on the reading side.
// The two sub-benchmarks are the two notification kinds the server offers:
//
//   - non-confirmable is the default: one send, no acknowledgement, so the number
//     is the notification itself;
//   - confirmable retransmits until the client ACKs, and at most one notification
//     per observer is in flight (MaxNotificationsInFlight), so each iteration also
//     waits for the previous ACK. That pacing is the feature, and it is visible in
//     the number.
func BenchmarkObserveNotify(b *testing.B) {
	for _, confirmable := range []bool{false, true} {
		name := "non-confirmable"
		if confirmable {
			name = "confirmable"
		}
		b.Run(name, func(b *testing.B) {
			srv, err := New(Config{Addr: "127.0.0.1:0", ConfirmableNotifications: confirmable})
			if err != nil {
				b.Fatalf("new server: %v", err)
			}
			srv.Handle(coap.CodeGET, "/sensor", func(_ context.Context, _ Request) Response {
				return Response{Code: coap.CodeContent, Payload: []byte("value=0")}
			})
			if err := srv.Start(context.Background()); err != nil {
				b.Fatalf("start server: %v", err)
			}
			b.Cleanup(func() { _ = srv.Stop(context.Background()) })

			cli, err := Dial(context.Background(), srv.Addr())
			if err != nil {
				b.Fatalf("dial %s: %v", srv.Addr(), err)
			}
			b.Cleanup(func() { _ = cli.Close() })

			// The callback runs on the client's receive goroutine and must not
			// block, so it hands the payload over and the benchmark goroutine does
			// the waiting. One slot is enough: an iteration waits for its own
			// notification before it sends the next one.
			delivered := make(chan []byte, 1)
			obs, _, err := cli.Observe(context.Background(),
				ClientRequest{Code: coap.CodeGET, Path: "/sensor"},
				func(resp ClientResponse) {
					select {
					case delivered <- append([]byte(nil), resp.Payload...):
					default:
					}
				})
			if err != nil {
				b.Fatalf("observe: %v", err)
			}
			b.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = obs.Cancel(ctx)
			})

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				payload := fmt.Appendf(nil, "value=%d", i)
				srv.Notify("/sensor", Response{Code: coap.CodeContent, Payload: payload})
				select {
				case got := <-delivered:
					if !bytes.Equal(got, payload) {
						b.Fatalf("notification %d delivered %q, want %q", i, got, payload)
					}
				case <-time.After(5 * time.Second):
					b.Fatalf("notification %d did not arrive", i)
				}
			}
			b.StopTimer()
		})
	}
}
