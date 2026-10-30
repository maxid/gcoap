package gcoap

import (
	"context"
	"errors"
	"fmt"

	"github.com/maxid/gnety/codec/coap"
)

// DefaultClientBlockSize is the block size a client uploads a request body in
// when ClientConfig.BlockSize is zero: 1024 bytes, the largest block RFC 7959
// can express (SZX 6).
const DefaultClientBlockSize = 1024

// DefaultMaxRequestSize is the request body bound a client applies when
// ClientClientConfig.MaxRequestSize is zero: 1 MiB, 4096 blocks of 256 bytes.
const DefaultMaxRequestSize = 1 << 20

// ErrBlockSequence is returned when a server answers a block-wise transfer with
// a block that does not continue the sequence, e.g. a block number the client
// did not send.
var ErrBlockSequence = errors.New("gcoap: invalid block sequence")

// blockSizeExp converts a block size in bytes to the RFC 7959 size exponent. A
// zero size selects DefaultClientBlockSize; every other value must be a power of
// two from 16 to 1024, the range coap.Block can encode.
func blockSizeExp(size int) (uint8, error) {
	if size == 0 {
		size = DefaultClientBlockSize
	}
	if size < 16 || size > 1024 || size&(size-1) != 0 {
		return 0, fmt.Errorf("gcoap: BlockSize %d is not a power of two from 16 to 1024", size)
	}
	exp := uint8(0)
	for blockSizeOf(exp) < size {
		exp++
	}
	return exp, nil
}

// blockSizeOf returns the block size in bytes of an RFC 7959 size exponent.
func blockSizeOf(exp uint8) int { return 1 << (exp + 4) }

// uploadRequest sends a request body with Block1 (RFC 7959 section 2.5):
// block 0 carries the size exponent the client negotiated, every following
// block is sent only after the server answered 2.31 Continue, and the block
// number and offset are recomputed from the size exponent the server answers
// with. A smaller server SZX downgrades the transfer; a larger one is ignored,
// so the client never silently upgrades.
//
// The server may end the upload early: an error code or a final response to a
// non-final block is returned to the caller as the response of the request and
// no further block is sent. Once the body is complete, a block-wise response is
// followed with Block2, repeating the final Block1 option instead of the body
// (RFC 7959 section 3.3).
func (c *Client) uploadRequest(ctx context.Context, req ClientRequest, token []byte) (ClientResponse, error) {
	if len(req.Payload) > c.maxRequest {
		return ClientResponse{}, ErrRequestTooLarge
	}
	szx := c.blockExp
	offset := 0
	num := uint32(0)
	body := len(req.Payload)
	for {
		size := blockSizeOf(szx)
		if num > 0x0fffff {
			return ClientResponse{}, fmt.Errorf("%w: block number overflow", ErrBlockSequence)
		}
		if offset >= body {
			return ClientResponse{}, fmt.Errorf("%w: no bytes left for block %d", ErrBlockSequence, num)
		}
		end := offset + size
		if end > body {
			end = body
		}
		block := coap.Block{Num: num, More: end < body, SizeExp: szx}
		msg, err := c.buildMessage(req, token, messageOptions{
			payload: req.Payload[offset:end],
			block1:  &block,
			size1:   body,
		})
		if err != nil {
			return ClientResponse{}, err
		}
		resp, err := c.exchange(ctx, &msg, false)
		if err != nil {
			return ClientResponse{}, err
		}
		// A response Size1 announces the largest request body the server is
		// willing to accept (RFC 7959 section 4); continuing cannot succeed.
		if limit, ok := resp.Size1(); ok && int(limit) < body {
			return ClientResponse{}, fmt.Errorf("%w: server accepts %d of %d bytes", ErrRequestTooLarge, limit, body)
		}
		if !block.More {
			if resp.Code == coap.CodeContinue {
				return ClientResponse{}, fmt.Errorf("%w: server continued after the final block", ErrBlockSequence)
			}
			return c.collectResponse(ctx, req, token, resp, &block)
		}
		if resp.Code != coap.CodeContinue {
			// Early response: the server answered before the body was complete.
			// An error ends the upload; a final success means the server already
			// had what it needed. Either way the response belongs to the caller.
			return ClientResponse{
				Code:    resp.Code,
				Options: resp.Options,
				Payload: resp.Payload,
			}, nil
		}
		next, ok, err := requestBlock(resp, coap.OptionBlock1)
		if err != nil {
			return ClientResponse{}, err
		}
		if !ok {
			// Some servers leave the Block1 option out of 2.31; keep the
			// negotiated size and continue with the next block.
			num++
			offset = end
			continue
		}
		if next.Num != num {
			return ClientResponse{}, fmt.Errorf("%w: server acknowledged block %d of block %d",
				ErrBlockSequence, next.Num, num)
		}
		if next.SizeExp < szx {
			szx = next.SizeExp
		}
		// The server accepted blocks 0..num, so the next byte to send is end.
		// It may prefer a smaller block size for the following blocks (RFC
		// 7959 section 2.5), in which case the block number has to be
		// recomputed to count in that smaller size; no byte is skipped.
		offset = end
		num = uint32(offset / blockSizeOf(szx))
	}
}
