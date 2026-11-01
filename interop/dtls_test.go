package interop

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxid/gcoap"
	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/coap"
	piondtls "github.com/pion/dtls/v3"
	"github.com/plgd-dev/go-coap/v3/dtls"
	plgdMessage "github.com/plgd-dev/go-coap/v3/message"
	plgdCodes "github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/mux"
	plgdNet "github.com/plgd-dev/go-coap/v3/net"
	"github.com/plgd-dev/go-coap/v3/net/blockwise"
	"github.com/plgd-dev/go-coap/v3/options"
)

// dtlsMaterial holds one self-signed certificate trusted by both stacks: the
// PEM files are for gcoap/gnety, the parsed certificate and pool are for the
// third-party library.
type dtlsMaterial struct {
	certFile string
	keyFile  string
	caFile   string
	cert     tls.Certificate
	pool     *x509.CertPool
}

func newDTLSMaterial(t *testing.T) dtlsMaterial {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gcoap interop"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	material := dtlsMaterial{
		certFile: filepath.Join(dir, "cert.pem"),
		keyFile:  filepath.Join(dir, "key.pem"),
		caFile:   filepath.Join(dir, "ca.pem"),
		cert:     tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf},
		pool:     x509.NewCertPool(),
	}
	material.pool.AddCert(leaf)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	for path, data := range map[string][]byte{
		material.certFile: certPEM,
		material.caFile:   certPEM,
		material.keyFile:  keyPEM,
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return material
}

// startPlgdDTLSServer starts the third-party DTLS server on an ephemeral port.
func startPlgdDTLSServer(t *testing.T, material dtlsMaterial, router mux.Handler) string {
	t.Helper()
	ln, err := plgdNet.NewDTLSListener("udp", "127.0.0.1:0", plgdNet.NewDTLSServerOptions(
		piondtls.WithCertificates(material.cert),
	))
	if err != nil {
		t.Fatalf("DTLS listen: %v", err)
	}
	srv := dtls.NewServer(
		options.WithMux(router),
		options.WithBlockwise(true, blockwise.SZX1024, 5*time.Second),
	)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	})
	return ln.Addr().String()
}

// TestOurClientToPlgdDTLSServer covers this library's DTLS client against an
// independent DTLS server, including a Block1 upload and a Block2 download.
func TestOurClientToPlgdDTLSServer(t *testing.T) {
	material := newDTLSMaterial(t)
	uploaded := make(chan []byte, 4)
	large := largeBody('c')
	router := mux.NewRouter()
	_ = router.Handle("/hello", mux.HandlerFunc(func(w mux.ResponseWriter, _ *mux.Message) {
		_ = w.SetResponse(plgdCodes.Content, plgdMessage.TextPlain, bytes.NewReader([]byte("hello dtls")))
	}))
	_ = router.Handle("/upload", mux.HandlerFunc(func(w mux.ResponseWriter, r *mux.Message) {
		body, err := io.ReadAll(r.Body())
		if err != nil {
			_ = w.SetResponse(plgdCodes.BadRequest, plgdMessage.TextPlain, nil)
			return
		}
		select {
		case uploaded <- body:
		default:
		}
		_ = w.SetResponse(plgdCodes.Changed, plgdMessage.TextPlain, nil)
	}))
	_ = router.Handle("/big", mux.HandlerFunc(func(w mux.ResponseWriter, _ *mux.Message) {
		_ = w.SetResponse(plgdCodes.Content, plgdMessage.TextPlain, bytes.NewReader(large))
	}))
	addr := startPlgdDTLSServer(t, material, router)

	cli, err := gcoap.DialConfig(context.Background(), gcoap.ClientConfig{
		Network:   "dtls",
		Addr:      addr,
		TLS:       gnety.TLSConfig{CAFile: material.caFile},
		BlockSize: 1024,
	})
	if err != nil {
		t.Fatalf("DTLS dial: %v", err)
	}
	defer func() { _ = cli.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := cli.Do(ctx, gcoap.ClientRequest{Code: coap.CodeGET, Path: "/hello"})
	if err != nil || string(resp.Payload) != "hello dtls" {
		t.Fatalf("DTLS GET = %q, %v", resp.Payload, err)
	}

	payload := largeBody('C')
	if _, err := cli.Do(ctx, gcoap.ClientRequest{Code: coap.CodePOST, Path: "/upload", Payload: payload}); err != nil {
		t.Fatalf("DTLS Block1 POST: %v", err)
	}
	select {
	case got := <-uploaded:
		if !bytes.Equal(got, payload) {
			t.Fatalf("third-party DTLS server received %d bytes, want %d", len(got), len(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("third-party DTLS server never received the upload")
	}

	block2, err := coap.Block{Num: 0, SizeExp: 2}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	resp, err = cli.Do(ctx, gcoap.ClientRequest{
		Code: coap.CodeGET, Path: "/big",
		Options: []coap.Option{{Number: coap.OptionBlock2, Value: block2}},
	})
	if err != nil {
		t.Fatalf("DTLS Block2 GET: %v", err)
	}
	if !bytes.Equal(resp.Payload, large) {
		t.Fatalf("DTLS Block2 GET returned %d bytes, want %d", len(resp.Payload), len(large))
	}
}

// TestPlgdClientToOurDTLSServer covers an independent DTLS client against this
// library's DTLS server.
func TestPlgdClientToOurDTLSServer(t *testing.T) {
	material := newDTLSMaterial(t)
	srv, err := gcoap.New(gcoap.Config{
		Network: "dtls",
		Addr:    "127.0.0.1:0",
		TLS:     gnety.TLSConfig{CertFile: material.certFile, KeyFile: material.keyFile},
	})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	large := largeBody('d')
	uploaded := make(chan []byte, 4)
	srv.Handle(coap.CodeGET, "/hello", func(context.Context, gcoap.Request) gcoap.Response {
		return gcoap.Response{Code: coap.CodeContent, Payload: []byte("hello dtls")}
	})
	srv.Handle(coap.CodePOST, "/upload", func(_ context.Context, req gcoap.Request) gcoap.Response {
		select {
		case uploaded <- append([]byte(nil), req.Payload...):
		default:
		}
		return gcoap.Response{Code: coap.CodeChanged}
	})
	srv.Handle(coap.CodeGET, "/big", func(context.Context, gcoap.Request) gcoap.Response {
		return gcoap.Response{Code: coap.CodeContent, Payload: large}
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	co, err := dtls.Dial(srv.Addr(), dtls.NewDTLSClientOptions(
		piondtls.WithRootCAs(material.pool),
		piondtls.WithServerName("127.0.0.1"),
	), options.WithBlockwise(true, blockwise.SZX1024, 5*time.Second))
	if err != nil {
		t.Fatalf("third-party DTLS dial: %v", err)
	}
	defer func() { _ = co.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := co.Get(ctx, "/hello")
	if err != nil {
		t.Fatalf("third-party DTLS GET: %v", err)
	}
	body, err := resp.ReadBody()
	if err != nil || string(body) != "hello dtls" {
		t.Fatalf("third-party DTLS GET = %q, %v", body, err)
	}

	payload := largeBody('D')
	resp, err = co.Post(ctx, "/upload", plgdMessage.TextPlain, bytes.NewReader(payload))
	if err != nil || resp.Code() != plgdCodes.Changed {
		t.Fatalf("third-party DTLS POST = %v, %v", resp.Code(), err)
	}
	select {
	case got := <-uploaded:
		if !bytes.Equal(got, payload) {
			t.Fatalf("our DTLS server received %d bytes, want %d", len(got), len(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("our DTLS server never received the upload")
	}

	resp, err = co.Get(ctx, "/big")
	if err != nil {
		t.Fatalf("third-party DTLS Block2 GET: %v", err)
	}
	body, err = resp.ReadBody()
	if err != nil {
		t.Fatalf("read block2 body: %v", err)
	}
	if !bytes.Equal(body, large) {
		t.Fatalf("third-party DTLS Block2 received %d bytes, want %d", len(body), len(large))
	}
}
