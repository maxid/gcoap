package gcoap

import (
	"fmt"

	"github.com/maxid/gnety/codec/coap"
)

// MessageCodec adapts coap.Message to gnety's DatagramCodec.
type MessageCodec struct{}

// Decode parses one CoAP datagram.
func (MessageCodec) Decode(data []byte) (any, error) { return coap.Parse(data) }

// Encode marshals one CoAP message.
//
// A []byte is already encoded and is written as is, which lets the server replay
// a cached acknowledgement byte for byte.
func (MessageCodec) Encode(msg any) ([]byte, error) {
	switch m := msg.(type) {
	case coap.Message:
		return m.Marshal()
	case []byte:
		return m, nil
	default:
		return nil, fmt.Errorf("gcoap: expected coap.Message, got %T", msg)
	}
}
