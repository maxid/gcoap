package gcoap

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
)

// ErrReset is returned when the peer answers a confirmable request with a reset,
// i.e. it rejects the message instead of processing it.
var ErrReset = errors.New("gcoap: reset by peer")

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
	params exchangeParams
}

// ClientConfig configures a CoAP client.
type ClientConfig struct {
	Addr    string
	Network string // "udp" (default) or "dtls"
	TLS     gnety.TLSConfig

	// Exchange timing follows RFC 7252 section 4.8 and is the same as on the
	// server side: a zero value keeps the default, negative values are rejected
	// by DialConfig and a negative MaxRetransmit disables retransmission.
	AckTimeout       time.Duration // ACK_TIMEOUT, default 2s
	AckRandomFactor  float64       // ACK_RANDOM_FACTOR, default 1.5
	MaxRetransmit    int           // MAX_RETRANSMIT, default 4
	ExchangeLifetime time.Duration // EXCHANGE_LIFETIME, default 247s
}

// Dial connects to a CoAP server over UDP.
func Dial(ctx context.Context, addr string) (*Client, error) {
	return DialConfig(ctx, ClientConfig{Addr: addr})
}

// DialConfig connects to a CoAP server over UDP or DTLS.
func DialConfig(ctx context.Context, cfg ClientConfig) (*Client, error) {
	params, err := defaultExchangeParams().withTiming(
		cfg.AckTimeout, cfg.AckRandomFactor, cfg.MaxRetransmit, cfg.ExchangeLifetime)
	if err != nil {
		return nil, err
	}
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
	return &Client{conn: conn, params: params}, nil
}

// Close closes the client.
func (c *Client) Close() error { return c.conn.Close() }

// Do sends a confirmable request, retransmits it on the RFC 7252 backoff
// schedule and automatically follows Block2 responses until the full payload is
// assembled.
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
	schedule := c.params.newRetransmissionSchedule()
	for {
		if err := c.conn.Send(req); err != nil {
			return coap.Message{}, err
		}
		wait, retransmit := schedule.next()
		for {
			resp, kind, err := c.await(ctx, req, wait)
			if err != nil {
				return coap.Message{}, err
			}
			if kind == awaitResponse {
				return resp, nil
			}
			if kind == awaitReset {
				// A reset for this message id ends the exchange: the peer
				// rejected the message, so retransmitting it cannot help.
				return coap.Message{}, ErrReset
			}
			if kind != awaitAcked {
				break // the window ran out: retransmit or give up
			}
			// Empty ACK: the peer owns the exchange and answers separately. Stop
			// retransmitting and keep listening for the response.
			wait, retransmit = schedule.remaining(), false
			if wait <= 0 {
				return coap.Message{}, context.DeadlineExceeded
			}
		}
		if !retransmit {
			break
		}
	}
	return coap.Message{}, context.DeadlineExceeded
}

// awaitKind classifies what the client learned while waiting for a response.
type awaitKind int

const (
	awaitTimeout  awaitKind = iota // nothing arrived inside the window
	awaitResponse                  // the response for this token arrived
	awaitAcked                     // an empty ACK arrived; the response follows later
	awaitReset                     // a reset for this message id arrived
)

// isDeadline reports a socket read that hit its deadline before the context
// timer fired; the transport reports those as net.Error timeouts.
func isDeadline(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// await waits up to wait for a datagram belonging to the exchange. Datagrams
// that belong to another exchange are ignored, so a late duplicate cannot finish
// a newer exchange.
func (c *Client) await(ctx context.Context, req coap.Message, wait time.Duration) (coap.Message, awaitKind, error) {
	if wait <= 0 {
		return coap.Message{}, awaitTimeout, nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for {
		msg, err := c.conn.Receive(waitCtx)
		if err != nil {
			if ctx.Err() != nil {
				return coap.Message{}, awaitTimeout, ctx.Err()
			}
			if waitCtx.Err() != nil || isDeadline(err) {
				return coap.Message{}, awaitTimeout, nil
			}
			return coap.Message{}, awaitTimeout, err
		}
		resp, ok := msg.(coap.Message)
		if !ok {
			continue
		}
		switch {
		case resp.Type == coap.Reset && resp.MessageID == req.MessageID:
			return coap.Message{}, awaitReset, nil
		case resp.Type == coap.Acknowledgement && resp.Code == coap.CodeEmpty && resp.MessageID == req.MessageID:
			return coap.Message{}, awaitAcked, nil
		case bytes.Equal(resp.Token, req.Token):
			return resp, awaitResponse, nil
		}
	}
}
