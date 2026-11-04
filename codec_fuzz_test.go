package gcoap

import (
	"bytes"
	"testing"

	"github.com/maxid/gnety/codec/coap"
)

// FuzzDecodeMessage feeds peer-controlled bytes through the datagram codec, which
// is the first thing the server and the client run on anything that arrives.
//
// Three properties matter, and none of them is about coverage:
//
//   - a datagram, however malformed, must be rejected with an error and never
//     panic;
//   - a datagram that parses must yield a message whose payload and options are
//     no larger than the input, i.e. no length field can be believed into an
//     allocation the sender did not pay for;
//   - encode(decode(x)) must be stable: whatever parses re-encodes, re-parses and
//     re-encodes to the same bytes. The deduplication cache replays encoded
//     datagrams, so a message that cannot be sent back would break the exchange.
func FuzzDecodeMessage(f *testing.F) {
	codec := MessageCodec{}

	seed := func(build func(m *coap.Message)) {
		m := coap.Message{Type: coap.Confirmable, Code: coap.CodeGET, MessageID: 0x1234, Token: []byte{0xde, 0xad}}
		build(&m)
		raw, err := m.Marshal()
		if err != nil {
			f.Fatalf("marshal seed: %v", err)
		}
		f.Add(raw)
	}

	// CON GET with a URI path.
	seed(func(m *coap.Message) { _ = m.SetURIPath("hello") })
	// NON response with a payload.
	seed(func(m *coap.Message) {
		m.Type, m.Code = coap.NonConfirmable, coap.CodeContent
		m.Payload = []byte("hi")
	})
	// Empty ACK and RST: no token, no options, no payload.
	seed(func(m *coap.Message) { m.Type, m.Code, m.Token = coap.Acknowledgement, coap.CodeEmpty, nil })
	seed(func(m *coap.Message) { m.Type, m.Code, m.Token = coap.Reset, coap.CodeEmpty, nil })
	// CON GET carrying Observe=0 and a query string, the shape a registration has.
	seed(func(m *coap.Message) {
		_ = m.SetURIPath("sensor")
		_ = m.SetURIQuery("interval=5", "unit=c")
		if err := m.SetObserve(coap.ObserveRegister); err != nil {
			f.Fatalf("set observe: %v", err)
		}
	})
	// CON GET with a Block2 option, the shape a sliced download asks with.
	seed(func(m *coap.Message) {
		_ = m.SetURIPath("data")
		block, err := coap.Block{Num: 2, SizeExp: 6}.Encode()
		if err != nil {
			f.Fatalf("encode block2: %v", err)
		}
		if err := m.SetOption(coap.OptionBlock2, block); err != nil {
			f.Fatalf("set block2: %v", err)
		}
	})
	// CON POST with one Block1 chunk, a declared Size1 and a content format.
	seed(func(m *coap.Message) {
		m.Code = coap.CodePOST
		_ = m.SetURIPath("config")
		block, err := coap.Block{Num: 0, More: true, SizeExp: 6}.Encode()
		if err != nil {
			f.Fatalf("encode block1: %v", err)
		}
		if err := m.SetOption(coap.OptionBlock1, block); err != nil {
			f.Fatalf("set block1: %v", err)
		}
		if err := m.SetOption(coap.OptionSize1, []byte{0x0c, 0x00}); err != nil { // 3072
			f.Fatalf("set size1: %v", err)
		}
		m.SetContentFormat(coap.ApplicationCBOR)
		m.Payload = bytes.Repeat([]byte{0xa1}, 1024)
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := codec.Decode(data)
		if err != nil {
			return // a rejected datagram is the expected outcome for most inputs
		}
		msg, ok := decoded.(coap.Message)
		if !ok {
			t.Fatalf("Decode returned %T, want coap.Message", decoded)
		}
		if len(msg.Payload) > len(data) {
			t.Fatalf("Decode(% x) produced a %d-byte payload from %d bytes", data, len(msg.Payload), len(data))
		}
		for _, opt := range msg.Options {
			if len(opt.Value) > len(data) {
				t.Fatalf("Decode(% x) produced a %d-byte option %d from %d bytes", data, len(opt.Value), opt.Number, len(data))
			}
		}

		first, err := codec.Encode(msg)
		if err != nil {
			t.Fatalf("Encode of the message decoded from % x failed: %v", data, err)
		}
		reparsed, err := coap.Parse(first)
		if err != nil {
			t.Fatalf("Parse of the re-encoded message % x failed: %v", first, err)
		}
		second, err := codec.Encode(reparsed)
		if err != nil {
			t.Fatalf("Encode of the re-parsed message % x failed: %v", first, err)
		}
		if !bytes.Equal(first, second) {
			t.Fatalf("encode(decode(x)) is not stable:\ninput  % x\nfirst  % x\nsecond % x", data, first, second)
		}
	})
}
