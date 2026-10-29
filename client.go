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
		b, ok, err := parseBlock(coap.Message{Options: resp.Options}, coap.OptionBlock2)
		if err != nil || !ok || !b.More {
			break
		}
		next := block{Num: b.Num + 1, SZX: b.SZX}
		msg, err = c.buildMessage(req, token, []coap.Option{{Number: coap.OptionBlock2, Value: encodeBlock(next)}})
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

func (c *Client) buildMessage(req ClientRequest, token []byte, extra []coap.Option) (coap.Message, error) {
	skipBlock2 := false
	for _, o := range extra {
		if o.Number == coap.OptionBlock2 {
			skipBlock2 = true
		}
	}
	m := coap.Message{
		Type:      coap.Confirmable,
		Code:      req.Code,
		MessageID: uint16(c.nextID.Add(1)),
		Token:     append([]byte(nil), token...),
		Payload:   req.Payload,
	}
	for _, o := range req.Options {
		if skipBlock2 && o.Number == coap.OptionBlock2 {
			continue
		}
		m.Options = append(m.Options, o)
	}
	if req.Path != "" && req.Path != "/" {
		for _, seg := range strings.Split(strings.Trim(req.Path, "/"), "/") {
			m.Options = append(m.Options, coap.Option{Number: coap.OptionURIPath, Value: []byte(seg)})
		}
	}
	for key, values := range req.Query {
		for _, v := range values {
			m.Options = append(m.Options, coap.Option{Number: coap.OptionURIQuery, Value: []byte(key + "=" + v)})
		}
	}
	m.Options = append(m.Options, extra...)
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
