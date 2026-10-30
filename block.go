package gcoap

import (
	"fmt"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

// requestBlock decodes the Block1 or Block2 option of a message. ok is false
// when the message carries no such option; a value that does not decode as a
// block option comes back with ok set and the error.
func requestBlock(m coap.Message, number uint16) (block coap.Block, ok bool, err error) {
	value, ok := m.OptionValue(number)
	if !ok {
		return coap.Block{}, false, nil
	}
	block, err = coap.DecodeBlock(value)
	return block, true, err
}

// blockValue returns one block option value in the compact form gcoap emits:
// RFC 7959 carries the value as an unsigned integer of 0 to 3 bytes, codec/coap
// encodes the fixed three-byte form, so the leading zero bytes are trimmed here.
// One byte is always kept.
func blockValue(b coap.Block) ([]byte, error) {
	raw, err := b.Encode()
	if err != nil {
		return nil, err
	}
	for len(raw) > 1 && raw[0] == 0 {
		raw = raw[1:]
	}
	return raw, nil
}

// blockOption returns b as the option with the given number.
func blockOption(number uint16, b coap.Block) (coap.Option, error) {
	value, err := blockValue(b)
	if err != nil {
		return coap.Option{}, err
	}
	return coap.Option{Number: number, Value: value}, nil
}

type upload struct {
	sessionID string
	path      string
	token     string
	data      []byte
	nextNum   uint32
	sizeExp   uint8
}

func uploadKey(sess *gnety.Session, token []byte) string {
	id := "unknown"
	if sess != nil {
		id = sess.ID
	}
	return id + "|" + string(token)
}

// processBlock1 handles one Block1 upload chunk. When complete it returns the
// full payload and the final block option.
func (s *Server) processBlock1(sess *gnety.Session, m coap.Message, b coap.Block) ([]byte, coap.Block, error) {
	size := b.Size()
	if size == 0 {
		return nil, coap.Block{}, fmt.Errorf("gcoap: invalid block szx %d", b.SizeExp)
	}
	if len(m.Payload) > size {
		return nil, coap.Block{}, fmt.Errorf("gcoap: block payload too large")
	}
	key := uploadKey(sess, m.Token)
	s.blockMu.Lock()
	defer s.blockMu.Unlock()
	if s.uploads == nil {
		s.uploads = make(map[string]*upload)
	}
	up := s.uploads[key]
	if b.Num == 0 {
		up = &upload{sessionID: uploadKey(sess, nil), path: "", token: string(m.Token), nextNum: 0, sizeExp: b.SizeExp}
		s.uploads[key] = up
	}
	if up == nil || up.sizeExp != b.SizeExp || b.Num != up.nextNum || uint32(len(up.data)) != b.Num*uint32(size) {
		delete(s.uploads, key)
		return nil, coap.Block{}, fmt.Errorf("gcoap: invalid block sequence")
	}
	up.data = append(up.data, m.Payload...)
	up.nextNum++
	up.path = ""
	if b.More {
		return nil, b, nil
	}
	data := up.data
	delete(s.uploads, key)
	final := coap.Block{Num: b.Num, More: false, SizeExp: b.SizeExp}
	return data, final, nil
}

func (s *Server) cleanupUploads(sessionID string) {
	s.blockMu.Lock()
	for key, up := range s.uploads {
		if up.sessionID == sessionID {
			delete(s.uploads, key)
		}
	}
	s.blockMu.Unlock()
}

func sliceBlock(payload []byte, b coap.Block) ([]byte, coap.Block, error) {
	size := b.Size()
	if size == 0 {
		return nil, coap.Block{}, fmt.Errorf("gcoap: invalid block szx %d", b.SizeExp)
	}
	start := int(b.Num) * size
	if start >= len(payload) {
		return nil, coap.Block{}, fmt.Errorf("gcoap: block offset out of range")
	}
	end := start + size
	more := end < len(payload)
	if end > len(payload) {
		end = len(payload)
	}
	return payload[start:end], coap.Block{Num: b.Num, More: more, SizeExp: b.SizeExp}, nil
}
