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
func (MessageCodec) Encode(msg any) ([]byte, error) {
	m, ok := msg.(coap.Message)
	if !ok {
		return nil, fmt.Errorf("gcoap: expected coap.Message, got %T", msg)
	}
	return m.Marshal()
}
