package gcoap

import (
	"context"
	"crypto/rand"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

// ClientRequest is a client-side CoAP request.
type ClientRequest struct {
	Code    coap.Code
	Path    string
	Query   url.Values
	Options []coap.Option
	Payload []byte
}

// ClientResponse is a client-side CoAP response.
type ClientResponse struct {
	Code    coap.Code
	Options []coap.Option
	Payload []byte
}

// Client is a minimal CoAP client over gnety UDP.
type Client struct {
	conn   *gnety.Client
	nextID atomic.Uint32
	mu     sync.Mutex
}

// ClientConfig configures a CoAP client.
type ClientConfig struct {
	Addr    string
	Network string // "udp" (default) or "dtls"
	TLS     gnety.TLSConfig
}

// Dial connects to a CoAP server over UDP.
func Dial(ctx context.Context, addr string) (*Client, error) {
	return DialConfig(ctx, ClientConfig{Addr: addr})
}

// DialConfig connects to a CoAP server over UDP or DTLS.
func DialConfig(ctx context.Context, cfg ClientConfig) (*Client, error) {
	network := cfg.Network
	if network == "" {
		network = "udp"
	}
	conn, err := gnety.Dial(ctx, gnety.ClientConfig{
		Network: network, Addr: cfg.Addr, TLS: cfg.TLS, DatagramCodec: MessageCodec{},
	})
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// Close closes the client.
func (c *Client) Close() error { return c.conn.Close() }

// Do sends a confirmable request, retries once on timeout and automatically
// follows Block2 responses until the full payload is assembled.
func (c *Client) Do(ctx context.Context, req ClientRequest) (ClientResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	token := make([]byte, 4)
	_, _ = rand.Read(token)
	msg, err := c.buildMessage(req, token, nil)
	if err != nil {
		return ClientResponse{}, err
	}
	resp, err := c.exchange(ctx, msg)
	if err != nil {
		return ClientResponse{}, err
	}
	full := append([]byte(nil), resp.Payload...)
	for {
		b, ok := resp.Block2()
		if !ok || !b.More {
			break
		}
		next := coap.Block{Num: b.Num + 1, SizeExp: b.SizeExp}
		msg, err = c.buildMessage(req, token, &next)
		if err != nil {
			return ClientResponse{}, err
		}
		part, err := c.exchange(ctx, msg)
		if err != nil {
			return ClientResponse{}, err
		}
		full = append(full, part.Payload...)
		resp = part
	}
	resp.Payload = full
	return ClientResponse{Code: resp.Code, Options: resp.Options, Payload: resp.Payload}, nil
}

// buildMessage assembles one request datagram. block2, when set, replaces any
// Block2 option the request carries so a block-wise read can ask for one block.
func (c *Client) buildMessage(req ClientRequest, token []byte, block2 *coap.Block) (coap.Message, error) {
	m := coap.Message{
		Type:      coap.Confirmable,
		Code:      req.Code,
		MessageID: uint16(c.nextID.Add(1)),
		Token:     append([]byte(nil), token...),
		Options:   append([]coap.Option(nil), req.Options...),
		Payload:   req.Payload,
	}
	segments := m.URIPath()
	if req.Path != "" && req.Path != "/" {
		segments = append(segments, strings.Split(strings.Trim(req.Path, "/"), "/")...)
	}
	if len(segments) > 0 {
		if err := m.SetURIPath(segments...); err != nil {
			return coap.Message{}, err
		}
	}
	query := m.URIQuery()
	for key, values := range req.Query {
		for _, v := range values {
			query = append(query, key+"="+v)
		}
	}
	if len(query) > 0 {
		if err := m.SetURIQuery(query...); err != nil {
			return coap.Message{}, err
		}
	}
	if block2 != nil {
		value, err := blockValue(*block2)
		if err != nil {
			return coap.Message{}, err
		}
		if err := m.SetOption(coap.OptionBlock2, value); err != nil {
			return coap.Message{}, err
		}
	}
	return m, nil
}

func (c *Client) exchange(ctx context.Context, req coap.Message) (coap.Message, error) {
	for attempt := 0; attempt < 2; attempt++ {
		if err := c.conn.Send(req); err != nil {
			return coap.Message{}, err
		}
		waitCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		msg, err := c.conn.Receive(waitCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return coap.Message{}, ctx.Err()
			}
			continue
		}
		resp, ok := msg.(coap.Message)
		if !ok {
			continue
		}
		if string(resp.Token) != string(req.Token) {
			continue
		}
		return resp, nil
	}
	return coap.Message{}, context.DeadlineExceeded
}
