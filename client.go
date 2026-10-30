package gcoap

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
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

// ErrClientClosed is returned by an exchange that can no longer complete
// because the client was closed or its transport failed.
var ErrClientClosed = errors.New("gcoap: client closed")

// ErrRequestTooLarge is returned when a request body exceeds
// ClientConfig.MaxRequestSize, or when the server announces a Size1 limit below
// the body being uploaded.
var ErrRequestTooLarge = errors.New("gcoap: request body exceeds the size limit")

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
//
// One goroutine reads the connection and hands each datagram to the exchange or
// observation it belongs to, so an observation keeps receiving notifications
// while a request runs. Do is serialized (one request at a time), observations
// are independent of it.
type Client struct {
	conn   *gnety.Client
	nextID atomic.Uint32
	mu     sync.Mutex // serializes Do
	params exchangeParams

	// Block1 upload configuration, set once by DialConfig.
	blockSize  int
	blockExp   uint8
	maxRequest int

	// readPoll bounds one receive call so the reader notices Close. It is a
	// field rather than a constant so tests can shorten it.
	readPoll time.Duration

	baseCtx context.Context
	cancel  context.CancelFunc
	reader  sync.WaitGroup
	closed  sync.Once
	closeTo error // set before Close returns, guarded by closed

	exMu         sync.Mutex
	requests     map[string]*clientExchange // by token
	requestsByID map[uint16]*clientExchange // for ACK and RST by message id
	observes     map[string]*Observation    // by token
	failure      error                      // first fatal transport error
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

	// BlockSize is the block size in bytes a request body is uploaded in with
	// Block1 (RFC 7959): a power of two from 16 to 1024, the range the CoAP block
	// size exponent can express. Zero keeps DefaultClientBlockSize. A server that
	// answers with a smaller block size downgrades the transfer; a larger one is
	// never adopted.
	BlockSize int

	// MaxRequestSize bounds a request body: a larger one is rejected with
	// ErrRequestTooLarge before anything is sent. Zero keeps
	// DefaultMaxRequestSize.
	MaxRequestSize int
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
	blockExp, err := blockSizeExp(cfg.BlockSize)
	if err != nil {
		return nil, err
	}
	maxRequest := cfg.MaxRequestSize
	if maxRequest == 0 {
		maxRequest = DefaultMaxRequestSize
	}
	if maxRequest < 0 {
		return nil, errors.New("gcoap: MaxRequestSize must not be negative")
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
	baseCtx, cancel := context.WithCancel(context.Background())
	c := &Client{
		conn:         conn,
		params:       params,
		blockSize:    blockSizeOf(blockExp),
		blockExp:     blockExp,
		maxRequest:   maxRequest,
		readPoll:     defaultReadPoll,
		baseCtx:      baseCtx,
		cancel:       cancel,
		requests:     make(map[string]*clientExchange),
		requestsByID: make(map[uint16]*clientExchange),
		observes:     make(map[string]*Observation),
	}
	c.reader.Add(1)
	go c.readLoop()
	return c, nil
}

// Close stops the client: it cancels the exchanges that are still in flight,
// ends every observation locally and closes the transport. Observations are not
// deregistered from the server, because the socket is going away anyway.
func (c *Client) Close() error {
	c.closed.Do(func() {
		c.cancel()
		c.stopAllObservations()
		c.reader.Wait()
		c.closeTo = c.conn.Close()
	})
	return c.closeTo
}

// Do sends a confirmable request, retransmits it on the RFC 7252 backoff
// schedule and automatically follows Block2 responses until the full payload is
// assembled.
//
// A request body larger than the configured block size is uploaded with Block1:
// the blocks are sent in order, every non-final block must be answered with
// 2.31 Continue, and the size the server asks for is honoured (RFC 7959 section
// 2.5). A server that answers a non-final block with an error or a final
// response ends the upload and that response is returned (a block-wise body in
// such an early response is not followed). Every request body is checked against
// ClientConfig.MaxRequestSize first.
func (c *Client) Do(ctx context.Context, req ClientRequest) (ClientResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.baseCtx.Err() != nil {
		return ClientResponse{}, c.closedError()
	}
	if len(req.Payload) > c.maxRequest {
		return ClientResponse{}, ErrRequestTooLarge
	}
	token := c.newToken()
	if len(req.Payload) > c.blockSize {
		return c.uploadRequest(ctx, req, token)
	}
	msg, err := c.buildMessage(req, token, messageOptions{payload: req.Payload})
	if err != nil {
		return ClientResponse{}, err
	}
	resp, err := c.exchange(ctx, &msg, false)
	if err != nil {
		return ClientResponse{}, err
	}
	return c.collectResponse(ctx, req, token, resp, nil)
}

// collectResponse follows Block2 response blocks until the body is complete and
// returns the assembled response. finalBlock, when set, is the last Block1 block
// of the request; RFC 7959 section 3.3 requires the follow-up requests to repeat
// it instead of the body, which is already on the server.
func (c *Client) collectResponse(ctx context.Context, req ClientRequest, token []byte, resp coap.Message, finalBlock *coap.Block) (ClientResponse, error) {
	full := append([]byte(nil), resp.Payload...)
	for {
		b, ok := resp.Block2()
		if !ok || !b.More {
			break
		}
		if b.Num == 0x0fffff {
			return ClientResponse{}, fmt.Errorf("%w: Block2 number overflow", ErrBlockSequence)
		}
		next := coap.Block{Num: b.Num + 1, SizeExp: b.SizeExp}
		msg, err := c.buildMessage(req, token, messageOptions{
			payload: []byte{}, block1: finalBlock, block2: &next,
		})
		if err != nil {
			return ClientResponse{}, err
		}
		part, err := c.exchange(ctx, &msg, false)
		if err != nil {
			return ClientResponse{}, err
		}
		full = append(full, part.Payload...)
		resp = part
	}
	return ClientResponse{Code: resp.Code, Options: resp.Options, Payload: full}, nil
}

// messageOptions overrides the parts of a request that one exchange of a
// block-wise or observe transfer changes. A nil block leaves the corresponding
// option of the request untouched.
type messageOptions struct {
	// payload replaces ClientRequest.Payload. A non-nil empty slice sends no
	// payload (the follow-up requests of a Block2 read must not repeat a body);
	// nil keeps the request payload.
	payload []byte
	block1  *coap.Block
	block2  *coap.Block
	observe *coap.Observe
	size1   int // > 0 sets Size1; 0 leaves it unset
}

// buildMessage assembles one request datagram, applying the overrides of opts.
func (c *Client) buildMessage(req ClientRequest, token []byte, opts messageOptions) (coap.Message, error) {
	m := coap.Message{
		Type:      coap.Confirmable,
		Code:      req.Code,
		MessageID: uint16(c.nextID.Add(1)),
		Token:     append([]byte(nil), token...),
		Options:   append([]coap.Option(nil), req.Options...),
		Payload:   req.Payload,
	}
	if opts.payload != nil {
		m.Payload = opts.payload
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
	if opts.block1 != nil {
		value, err := blockValue(*opts.block1)
		if err != nil {
			return coap.Message{}, err
		}
		if err := m.SetOption(coap.OptionBlock1, value); err != nil {
			return coap.Message{}, err
		}
	}
	if opts.block2 != nil {
		value, err := blockValue(*opts.block2)
		if err != nil {
			return coap.Message{}, err
		}
		if err := m.SetOption(coap.OptionBlock2, value); err != nil {
			return coap.Message{}, err
		}
	}
	if opts.observe != nil {
		if err := m.SetObserve(*opts.observe); err != nil {
			return coap.Message{}, err
		}
	}
	if opts.size1 > 0 {
		m.SetSize1(uint32(opts.size1))
	}
	return m, nil
}

// newToken returns a fresh request token that no exchange or observation uses.
func (c *Client) newToken() []byte {
	for {
		token := make([]byte, 4)
		_, _ = rand.Read(token)
		c.exMu.Lock()
		_, pending := c.requests[string(token)]
		_, observing := c.observes[string(token)]
		c.exMu.Unlock()
		if !pending && !observing {
			return token
		}
	}
}

// exchange sends one confirmable message and waits for its response, using the
// RFC 7252 retransmission schedule. With ignoreNotifications a datagram that
// carries an Observe option is not taken as the response; that is what a
// deregistration exchange needs while notifications of the cancelled
// observation are still in flight.
func (c *Client) exchange(ctx context.Context, msg *coap.Message, ignoreNotifications bool) (coap.Message, error) {
	ex := c.registerExchange(msg, ignoreNotifications)
	defer c.releaseExchange(ex)

	schedule := c.params.newRetransmissionSchedule()
	for {
		if err := c.conn.Send(*msg); err != nil {
			if c.baseCtx.Err() != nil {
				return coap.Message{}, c.closedError()
			}
			return coap.Message{}, err
		}
		wait, retransmit := schedule.next()
		for {
			resp, kind, err := c.await(ctx, ex, wait)
			if err != nil {
				return coap.Message{}, err
			}
			if kind == awaitResponse {
				if resp.Type == coap.Confirmable {
					// A separate response is a new confirmable message, not the
					// acknowledgement of our request; acknowledge it so the server
					// stops retransmitting.
					ack := coap.Message{
						Type: coap.Acknowledgement, Code: coap.CodeEmpty,
						MessageID: resp.MessageID,
					}
					_ = c.conn.Send(ack)
				}
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

// clientEvent is one datagram routed to an exchange.
type clientEvent struct {
	msg  coap.Message
	kind awaitKind
}

// clientExchange is the demultiplexing state of one outbound exchange. events is
// buffered so the reader never blocks on an exchange that is already finishing.
type clientExchange struct {
	token               string
	messageID           uint16
	events              chan clientEvent
	ignoreNotifications bool
}

// deliver hands one event to the exchange without blocking the reader. A
// duplicate event is dropped: the first answer wins.
func (ex *clientExchange) deliver(ev clientEvent) {
	select {
	case ex.events <- ev:
	default:
	}
}

// registerExchange records msg so the reader can route the ACK, RST and response
// that belong to it. A message id that is still in use gets the next value.
func (c *Client) registerExchange(msg *coap.Message, ignoreNotifications bool) *clientExchange {
	ex := &clientExchange{
		events:              make(chan clientEvent, 2),
		ignoreNotifications: ignoreNotifications,
	}
	c.exMu.Lock()
	defer c.exMu.Unlock()
	for {
		if _, used := c.requestsByID[msg.MessageID]; !used {
			break
		}
		msg.MessageID = uint16(c.nextID.Add(1))
	}
	ex.token = string(msg.Token)
	ex.messageID = msg.MessageID
	c.requests[ex.token] = ex
	c.requestsByID[ex.messageID] = ex
	return ex
}

// releaseExchange removes ex from the routing tables. A late datagram for the
// same token is dropped by the reader.
func (c *Client) releaseExchange(ex *clientExchange) {
	c.exMu.Lock()
	if c.requests[ex.token] == ex {
		delete(c.requests, ex.token)
	}
	if c.requestsByID[ex.messageID] == ex {
		delete(c.requestsByID, ex.messageID)
	}
	c.exMu.Unlock()
}

// isDeadline reports a socket read that hit its deadline before the context
// timer fired; the transport reports those as net.Error timeouts.
func isDeadline(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// await waits up to wait for an event of the exchange. The wait uses the same
// injectable timer as the retransmission schedule, so a test drives the exchange
// on a manual clock.
func (c *Client) await(ctx context.Context, ex *clientExchange, wait time.Duration) (coap.Message, awaitKind, error) {
	if wait <= 0 {
		return coap.Message{}, awaitTimeout, nil
	}
	timer := c.params.after(wait)
	select {
	case <-ctx.Done():
		return coap.Message{}, awaitTimeout, ctx.Err()
	case <-c.baseCtx.Done():
		return coap.Message{}, awaitTimeout, c.closedError()
	case <-timer:
		return coap.Message{}, awaitTimeout, nil
	case ev := <-ex.events:
		return ev.msg, ev.kind, nil
	}
}

// closedError reports why the client stopped, wrapping ErrClientClosed.
func (c *Client) closedError() error {
	c.exMu.Lock()
	failure := c.failure
	c.exMu.Unlock()
	if failure != nil {
		return fmt.Errorf("%w: %v", ErrClientClosed, failure)
	}
	return ErrClientClosed
}

// fail records the first fatal transport error and stops the reader.
func (c *Client) fail(err error) {
	c.exMu.Lock()
	if c.failure == nil {
		c.failure = err
	}
	c.exMu.Unlock()
	c.cancel()
}

// defaultReadPoll bounds one receive call. It is short enough that Close joins
// the reader promptly and long enough that an idle client does not spin.
const defaultReadPoll = 50 * time.Millisecond

// readLoop owns the connection: exactly one goroutine reads it, so a response
// and a notification cannot be consumed by competing readers. The receive
// timeout only exists so a cancelled client can stop reading: the gnety client
// has no separate way to interrupt a read.
func (c *Client) readLoop() {
	defer c.reader.Done()
	for {
		if c.baseCtx.Err() != nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.baseCtx, c.readPoll)
		msg, err := c.conn.Receive(ctx)
		cancel()
		if err != nil {
			if c.baseCtx.Err() != nil {
				return
			}
			if isDeadline(err) {
				continue
			}
			c.fail(err)
			return
		}
		if m, ok := msg.(coap.Message); ok {
			c.dispatch(m)
		}
	}
}

// dispatch routes one inbound datagram. An ACK or RST is matched by message id;
// a response or notification by token. An exchange wins over an observation, so
// the response to a registration or deregistration request is never mistaken
// for a notification.
func (c *Client) dispatch(m coap.Message) {
	switch m.Type {
	case coap.Acknowledgement:
		if m.Code == coap.CodeEmpty {
			if ex := c.exchangeByID(m.MessageID); ex != nil {
				ex.deliver(clientEvent{kind: awaitAcked})
				return
			}
		}
	case coap.Reset:
		if ex := c.exchangeByID(m.MessageID); ex != nil {
			ex.deliver(clientEvent{kind: awaitReset})
			return
		}
	}
	if ex := c.exchangeByToken(m.Token); ex != nil {
		if ex.ignoreNotifications && isNotification(m) {
			// A notification of the observation being deregistered is not the
			// answer to the deregistration request. Acknowledge it so the server
			// stops retransmitting and keep waiting.
			c.ackConfirmable(m)
			return
		}
		ex.deliver(clientEvent{msg: m, kind: awaitResponse})
		return
	}
	if o := c.observationByToken(m.Token); o != nil {
		c.ackConfirmable(m)
		o.deliver(m)
	}
}

func (c *Client) exchangeByID(id uint16) *clientExchange {
	c.exMu.Lock()
	defer c.exMu.Unlock()
	return c.requestsByID[id]
}

func (c *Client) exchangeByToken(token []byte) *clientExchange {
	c.exMu.Lock()
	defer c.exMu.Unlock()
	return c.requests[string(token)]
}

func (c *Client) observationByToken(token []byte) *Observation {
	c.exMu.Lock()
	defer c.exMu.Unlock()
	return c.observes[string(token)]
}

// isNotification reports a response that carries an Observe option, i.e. a
// notification (RFC 7641 section 3.2).
func isNotification(m coap.Message) bool {
	if !m.Code.IsResponse() {
		return false
	}
	_, ok := m.Observe()
	return ok
}

// ackConfirmable acknowledges a confirmable notification. RFC 7641 section 3.5
// requires the ACK even when the notification itself is stale and not delivered.
func (c *Client) ackConfirmable(m coap.Message) {
	if m.Type != coap.Confirmable || m.Code == coap.CodeEmpty {
		return
	}
	ack := coap.Message{Type: coap.Acknowledgement, Code: coap.CodeEmpty, MessageID: m.MessageID}
	_ = c.conn.Send(ack)
}
