package gcoap

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"time"

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

// block1Error carries the CoAP response code a Block1 failure must produce.
type block1Error struct {
	code coap.Code
	err  error
}

func (e *block1Error) Error() string { return e.err.Error() }

func (e *block1Error) Unwrap() error { return e.err }

// upload is one Block1 upload in progress. Every block of an upload must repeat
// the method and the request URI, and the blocks arrive in order without gaps.
type upload struct {
	sessionID string
	method    coap.Code
	// path is the request URI with a canonical query string, so a block that
	// changes the target is rejected instead of being appended.
	path      string
	sizeExp   uint8
	nextNum   uint32
	data      []byte
	last      []byte
	updatedAt time.Time
}

// completedUpload remembers the final response of a finished Block1 upload, so
// a repeated final block that carries a new message id is answered instead of
// being treated as a sequence error.
type completedUpload struct {
	sessionID string
	method    coap.Code
	path      string
	finalNum  uint32
	response  Response
	updatedAt time.Time
}

// uploadKey identifies one upload by session and token.
func uploadKey(sess *gnety.Session, token []byte) string {
	id := "unknown"
	if sess != nil {
		id = sess.ID
	}
	return id + "|" + string(token)
}

// blockRequestPath renders the request URI in the form the upload state
// compares: the path plus a canonically ordered query string.
func blockRequestPath(path string, query url.Values) string {
	if len(query) == 0 {
		return path
	}
	return path + "?" + query.Encode()
}

// processBlock1 handles one Block1 upload chunk. When the body is complete it
// returns the full payload and the final block option. When the request must be
// answered without running the handler it returns a replay response instead:
// the interim 2.31 of a repeated block, or the cached final response of an
// upload that already completed.
func (s *Server) processBlock1(sess *gnety.Session, m coap.Message, b coap.Block, path string) ([]byte, coap.Block, *Response, error) {
	size := b.Size()
	if size == 0 {
		return nil, coap.Block{}, nil, &block1Error{code: coap.CodeBadRequest, err: fmt.Errorf("gcoap: invalid block szx %d", b.SizeExp)}
	}
	if len(m.Payload) > size {
		return nil, coap.Block{}, nil, &block1Error{code: coap.CodeBadRequest, err: errors.New("gcoap: block payload larger than the declared block size")}
	}
	key := uploadKey(sess, m.Token)

	s.blockMu.Lock()
	defer s.blockMu.Unlock()
	now := s.params.now()
	s.sweepBlocksLocked(now)

	up := s.uploads[key]
	if b.Num == 0 {
		// A new transfer replaces whatever used this token before, so a client
		// that restarts an upload does not inherit half of the old body.
		delete(s.uploads, key)
		delete(s.completed, key)
		if len(s.uploads) >= s.maxBlockUploads {
			return nil, coap.Block{}, nil, &block1Error{
				code: coap.CodeServiceUnavailable,
				err:  fmt.Errorf("gcoap: more than %d block-wise uploads in progress", s.maxBlockUploads),
			}
		}
		up = &upload{
			sessionID: sessionID(sess),
			method:    m.Code,
			path:      path,
			sizeExp:   b.SizeExp,
			updatedAt: now,
		}
		s.uploads[key] = up
	}
	if up == nil {
		if done := s.completed[key]; done != nil &&
			done.method == m.Code && done.path == path && done.finalNum == b.Num {
			replay := cloneResponse(done.response)
			return nil, coap.Block{}, &replay, nil
		}
		return nil, coap.Block{}, nil, &block1Error{
			code: coap.CodeRequestEntityIncomplete,
			err:  fmt.Errorf("gcoap: block %d does not belong to an upload in progress", b.Num),
		}
	}
	if up.method != m.Code || up.path != path || up.sizeExp != b.SizeExp {
		delete(s.uploads, key)
		return nil, coap.Block{}, nil, &block1Error{
			code: coap.CodeRequestEntityIncomplete,
			err:  fmt.Errorf("gcoap: block %d changes the method, URI or block size of the upload", b.Num),
		}
	}
	if size1, ok := m.Size1(); ok && int(size1) > s.maxBlockPayload {
		delete(s.uploads, key)
		return nil, coap.Block{}, nil, &block1Error{
			code: coap.CodeRequestEntityTooLarge,
			err:  fmt.Errorf("gcoap: declared body of %d bytes exceeds %d", size1, s.maxBlockPayload),
		}
	}
	switch {
	case b.Num == up.nextNum:
		if len(up.data)+len(m.Payload) > s.maxBlockPayload {
			delete(s.uploads, key)
			return nil, coap.Block{}, nil, &block1Error{
				code: coap.CodeRequestEntityTooLarge,
				err:  fmt.Errorf("gcoap: body exceeds %d bytes", s.maxBlockPayload),
			}
		}
		up.data = append(up.data, m.Payload...)
		up.last = append(up.last[:0], m.Payload...)
		up.nextNum++
		up.updatedAt = now
	case b.Num < up.nextNum && b.Num == up.nextNum-1 && bytes.Equal(up.last, m.Payload):
		// The previous block was already accepted: repeat the interim answer
		// instead of appending the body twice.
		option, err := blockOption(coap.OptionBlock1, b)
		if err != nil {
			return nil, coap.Block{}, nil, &block1Error{code: coap.CodeBadRequest, err: err}
		}
		return nil, coap.Block{}, &Response{Code: coap.CodeContinue, Options: []coap.Option{option}}, nil
	case b.Num < up.nextNum:
		delete(s.uploads, key)
		return nil, coap.Block{}, nil, &block1Error{
			code: coap.CodeRequestEntityIncomplete,
			err:  fmt.Errorf("gcoap: repeated block %d does not match the accepted body", b.Num),
		}
	default:
		delete(s.uploads, key)
		return nil, coap.Block{}, nil, &block1Error{
			code: coap.CodeRequestEntityIncomplete,
			err:  fmt.Errorf("gcoap: block %d arrived while %d was expected", b.Num, up.nextNum),
		}
	}
	if b.More {
		return nil, b, nil, nil
	}
	data := up.data
	delete(s.uploads, key)
	return data, coap.Block{Num: b.Num, More: false, SizeExp: b.SizeExp}, nil, nil
}

// rememberCompletedBlock1 caches the final response of a completed upload so a
// repeated final block with a new message id replays it.
func (s *Server) rememberCompletedBlock1(st *requestExchange, resp Response) {
	if st.plan.finalBlock1 == nil {
		return
	}
	key := uploadKey(st.sess, st.msg.Token)
	s.blockMu.Lock()
	defer s.blockMu.Unlock()
	if s.completed == nil {
		s.completed = make(map[string]*completedUpload)
	}
	now := s.params.now()
	s.sweepBlocksLocked(now)
	s.completed[key] = &completedUpload{
		sessionID: sessionID(st.sess),
		method:    st.msg.Code,
		path:      st.plan.requestPath,
		finalNum:  st.plan.finalBlock1Num,
		response:  cloneResponse(resp),
		updatedAt: now,
	}
	for len(s.completed) > s.maxBlockUploads {
		s.dropOldestCompletedLocked()
	}
}

// sweepBlocksLocked expires idle uploads and completed responses, then keeps the
// completed cache within its bound. The caller holds blockMu.
func (s *Server) sweepBlocksLocked(now time.Time) {
	if s.blockIdleTimeout > 0 {
		for key, up := range s.uploads {
			if now.Sub(up.updatedAt) > s.blockIdleTimeout {
				delete(s.uploads, key)
			}
		}
		for key, done := range s.completed {
			if now.Sub(done.updatedAt) > s.blockIdleTimeout {
				delete(s.completed, key)
			}
		}
	}
	for len(s.completed) > s.maxBlockUploads {
		s.dropOldestCompletedLocked()
	}
}

// dropOldestCompletedLocked removes the oldest completed response. The cache is
// small (bounded by MaxBlockUploads), so a linear scan is cheaper than another
// index.
func (s *Server) dropOldestCompletedLocked() {
	var (
		oldestKey  string
		oldestTime time.Time
		set        bool
	)
	for key, done := range s.completed {
		if !set || done.updatedAt.Before(oldestTime) {
			oldestKey, oldestTime, set = key, done.updatedAt, true
		}
	}
	if set {
		delete(s.completed, oldestKey)
	}
}

// cloneResponse deep-copies a response so the cache and the transport cannot
// share slices.
func cloneResponse(resp Response) Response {
	out := resp
	out.Options = append([]coap.Option(nil), resp.Options...)
	out.Payload = append([]byte(nil), resp.Payload...)
	return out
}

// sessionID returns the session id or a stable placeholder.
func sessionID(sess *gnety.Session) string {
	if sess == nil {
		return "unknown"
	}
	return sess.ID
}

func (s *Server) cleanupUploads(sessionID string) {
	s.blockMu.Lock()
	for key, up := range s.uploads {
		if up.sessionID == sessionID {
			delete(s.uploads, key)
		}
	}
	for key, done := range s.completed {
		if done.sessionID == sessionID {
			delete(s.completed, key)
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
