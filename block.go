package gcoap

import (
	"fmt"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

// block describes a CoAP block option (RFC 7959).
type block struct {
	Num  uint32
	More bool
	SZX  uint8
}

func blockSize(szx uint8) (int, error) {
	if szx > 6 {
		return 0, fmt.Errorf("gcoap: invalid block szx %d", szx)
	}
	return 16 << szx, nil
}

func parseBlock(m coap.Message, number uint16) (block, bool, error) {
	for _, opt := range m.Options {
		if opt.Number != number {
			continue
		}
		var raw uint32
		for _, b := range opt.Value {
			raw = raw<<8 | uint32(b)
		}
		return block{Num: raw >> 4, More: raw&0x08 != 0, SZX: uint8(raw & 0x07)}, true, nil
	}
	return block{}, false, nil
}

func encodeBlock(b block) []byte {
	raw := b.Num<<4 | uint32(b.SZX)
	if b.More {
		raw |= 0x08
	}
	if raw <= 0xFF {
		return []byte{byte(raw)}
	}
	if raw <= 0xFFFF {
		return []byte{byte(raw >> 8), byte(raw)}
	}
	return []byte{byte(raw >> 16), byte(raw >> 8), byte(raw)}
}

type upload struct {
	sessionID string
	path      string
	token     string
	data      []byte
	nextNum   uint32
	szx       uint8
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
func (s *Server) processBlock1(sess *gnety.Session, m coap.Message, b block) ([]byte, block, error) {
	size, err := blockSize(b.SZX)
	if err != nil {
		return nil, block{}, err
	}
	if len(m.Payload) > size {
		return nil, block{}, fmt.Errorf("gcoap: block payload too large")
	}
	key := uploadKey(sess, m.Token)
	s.blockMu.Lock()
	defer s.blockMu.Unlock()
	if s.uploads == nil {
		s.uploads = make(map[string]*upload)
	}
	up := s.uploads[key]
	if b.Num == 0 {
		up = &upload{sessionID: uploadKey(sess, nil), path: "", token: string(m.Token), nextNum: 0, szx: b.SZX}
		s.uploads[key] = up
	}
	if up == nil || up.szx != b.SZX || b.Num != up.nextNum || uint32(len(up.data)) != b.Num*uint32(size) {
		delete(s.uploads, key)
		return nil, block{}, fmt.Errorf("gcoap: invalid block sequence")
	}
	up.data = append(up.data, m.Payload...)
	up.nextNum++
	up.path = ""
	if b.More {
		return nil, b, nil
	}
	data := up.data
	delete(s.uploads, key)
	final := block{Num: b.Num, More: false, SZX: b.SZX}
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

func sliceBlock(payload []byte, b block) ([]byte, block, error) {
	size, err := blockSize(b.SZX)
	if err != nil {
		return nil, block{}, err
	}
	start := int(b.Num) * size
	if start >= len(payload) {
		return nil, block{}, fmt.Errorf("gcoap: block offset out of range")
	}
	end := start + size
	more := end < len(payload)
	if end > len(payload) {
		end = len(payload)
	}
	return payload[start:end], block{Num: b.Num, More: more, SZX: b.SZX}, nil
}
