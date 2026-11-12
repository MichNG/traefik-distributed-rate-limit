package redis

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Self-signed material for the TLS tests.
// ---------------------------------------------------------------------------

// testPKI holds a CA and a leaf certificate, both as PEM strings.
type testPKI struct {
	CACert     string
	LeafCert   string
	LeafKey    string
	TLSConfig  *tls.Config
	ServerName string
}

func newTestPKI(t *testing.T) testPKI {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}

	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating CA certificate: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating leaf key: %v", err)
	}

	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caTmpl, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating leaf certificate: %v", err)
	}

	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshaling leaf key: %v", err)
	}

	caPEM := pemEncode("CERTIFICATE", caDER)
	leafPEM := pemEncode("CERTIFICATE", leafDER)
	keyPEM := pemEncode("EC PRIVATE KEY", leafKeyDER)

	serverCert, err := tls.X509KeyPair([]byte(leafPEM), []byte(keyPEM))
	if err != nil {
		t.Fatalf("building server key pair: %v", err)
	}

	return testPKI{
		CACert:     caPEM,
		LeafCert:   leafPEM,
		LeafKey:    keyPEM,
		ServerName: "127.0.0.1",
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{serverCert},
		},
	}
}

func pemEncode(kind string, der []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}))
}

// ---------------------------------------------------------------------------
// Fake Redis master/replica.
// ---------------------------------------------------------------------------

type fakeRedis struct {
	ln net.Listener

	mu           sync.Mutex
	replica      bool
	scripts      map[string]bool
	cmdLog       []string
	connCount    int
	scriptLoads  int
	evalshaCount int
}

func newFakeRedis(t *testing.T, tlsConfig *tls.Config) *fakeRedis {
	t.Helper()

	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var ln net.Listener = tcpLn
	if tlsConfig != nil {
		ln = tls.NewListener(tcpLn, tlsConfig)
	}

	fr := &fakeRedis{ln: ln, scripts: map[string]bool{}}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			fr.mu.Lock()
			fr.connCount++
			fr.mu.Unlock()
			go fr.handle(conn)
		}
	}()

	t.Cleanup(func() { _ = ln.Close() })

	return fr
}

func (f *fakeRedis) hostPort() (string, string) {
	host, port, err := net.SplitHostPort(f.ln.Addr().String())
	if err != nil {
		panic(err)
	}
	return host, port
}

func (f *fakeRedis) addr() string { return f.ln.Addr().String() }

func (f *fakeRedis) setReplica(v bool) {
	f.mu.Lock()
	f.replica = v
	f.mu.Unlock()
}

func (f *fakeRedis) forgetScripts() {
	f.mu.Lock()
	f.scripts = map[string]bool{}
	f.mu.Unlock()
}

func (f *fakeRedis) counts() (conns, loads, evalshas int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connCount, f.scriptLoads, f.evalshaCount
}

func (f *fakeRedis) commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cmdLog...)
}

func (f *fakeRedis) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	rd := bufio.NewReader(conn)

	for {
		value, err := readValue(rd)
		if err != nil {
			return
		}

		items := value.Array()
		if len(items) == 0 {
			continue
		}

		args := make([]string, 0, len(items))
		for _, item := range items {
			args = append(args, item.String())
		}

		name := strings.ToUpper(args[0])

		f.mu.Lock()
		f.cmdLog = append(f.cmdLog, name)
		f.mu.Unlock()

		switch name {
		case "PING":
			_ = writeRaw(conn, "+PONG\r\n")

		case "AUTH", "SELECT":
			_ = writeRaw(conn, "+OK\r\n")

		case "DEL":
			_ = writeRaw(conn, ":1\r\n")

		case "SCRIPT":
			if len(args) < 3 {
				_ = writeRaw(conn, "-ERR wrong number of arguments\r\n")
				continue
			}
			sum := sha1.Sum([]byte(args[2]))
			sha := hex.EncodeToString(sum[:])

			f.mu.Lock()
			f.scripts[sha] = true
			f.scriptLoads++
			f.mu.Unlock()

			_ = writeRaw(conn, fmt.Sprintf("$40\r\n%s\r\n", sha))

		case "EVALSHA":
			if len(args) < 2 {
				_ = writeRaw(conn, "-ERR wrong number of arguments\r\n")
				continue
			}

			f.mu.Lock()
			loaded := f.scripts[args[1]]
			replica := f.replica
			f.evalshaCount++
			f.mu.Unlock()

			switch {
			case !loaded:
				_ = writeRaw(conn, "-NOSCRIPT No matching script. Please use EVAL.\r\n")
			case replica:
				_ = writeRaw(conn, "-READONLY You can't write against a read only replica.\r\n")
			default:
				// Same shape as the token bucket script reply:
				// {allowed, remaining, retry_after, reset_after}
				_ = writeRaw(conn, "*4\r\n:1\r\n:4\r\n$2\r\n-1\r\n$1\r\n0\r\n")
			}

		default:
			_ = writeRaw(conn, "-ERR unknown command\r\n")
		}
	}
}

func stringReader(s string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(s))
}
