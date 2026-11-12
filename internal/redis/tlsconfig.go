package redis

// TLS support for Redis/Sentinel connections.
//
// This mirrors, field for field and behaviour for behaviour, the internal
// Traefik OSS mechanism used by the core Redis-backed rate limiter and the KV
// stores (traefik/pkg/types/tls.go):
//
//	type ClientTLS struct {
//	    CA                 string // path or PEM content
//	    Cert               string // path or PEM content
//	    Key                string // path or PEM content
//	    InsecureSkipVerify bool
//	}
//
// A Yaegi plugin cannot import github.com/traefik/traefik/v3/pkg/types, so the
// contract is re-implemented here with the same semantics.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"time"
)

// TLSConfig holds the client TLS configuration of the Redis store
// (`store.redis.tls`).
type TLSConfig struct {
	// CA is the CA bundle, either a file path or the PEM content itself
	// (Traefik Hub: `store.redis.tls.caBundle`, Traefik OSS: `.ca`).
	CA string
	// Cert is the client certificate, either a file path or the PEM content.
	Cert string
	// Key is the client private key, either a file path or the PEM content.
	Key string
	// InsecureSkipVerify disables the verification of the server certificate.
	InsecureSkipVerify bool
}

// CreateTLSConfig builds a *tls.Config, reproducing the semantics of
// types.ClientTLS.CreateTLSConfig from Traefik OSS. A nil receiver yields a nil
// config, i.e. a plain TCP connection.
func (c *TLSConfig) CreateTLSConfig() (*tls.Config, error) {
	if c == nil {
		return nil, nil
	}

	// Not initialized, to rely on the system bundle.
	var caPool *x509.CertPool

	if c.CA != "" {
		var ca []byte
		if _, errCA := os.Stat(c.CA); errCA == nil {
			var err error
			ca, err = os.ReadFile(c.CA)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA: %w", err)
			}
		} else {
			ca = []byte(c.CA)
		}

		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(ca) {
			return nil, errors.New("failed to parse CA")
		}
	}

	hasCert := len(c.Cert) > 0
	hasKey := len(c.Key) > 0

	if hasCert != hasKey {
		return nil, errors.New("both TLS cert and key must be defined")
	}

	if !hasCert || !hasKey {
		return &tls.Config{
			RootCAs:            caPool,
			InsecureSkipVerify: c.InsecureSkipVerify,
		}, nil
	}

	cert, err := loadKeyPair(c.Cert, c.Key)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caPool,
		InsecureSkipVerify: c.InsecureSkipVerify,
	}, nil
}

// loadKeyPair accepts both inline PEM contents and file paths, exactly like
// traefik/pkg/types.loadKeyPair.
func loadKeyPair(cert, key string) (tls.Certificate, error) {
	keyPair, err := tls.X509KeyPair([]byte(cert), []byte(key))
	if err == nil {
		return keyPair, nil
	}

	if _, err := os.Stat(cert); err != nil {
		return tls.Certificate{}, errors.New("cert file does not exist")
	}

	if _, err := os.Stat(key); err != nil {
		return tls.Certificate{}, errors.New("key file does not exist")
	}

	keyPair, err = tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return tls.Certificate{}, err
	}

	return keyPair, nil
}

// Dialer opens plain TCP or TLS connections to Redis/Sentinel nodes.
//
// TLS is applied at the socket level (not through a Redis client library),
// which is what allows the same configuration to protect both the Sentinel
// connections and the data connections, and to survive a master switch (a TLS
// connection to the new master is re-established from scratch).
type Dialer struct {
	TLS         *tls.Config
	DialTimeout time.Duration
	// ServerName overrides the SNI/verification name; normally empty and
	// derived from the dialed address.
	ServerName string
}

// Dial connects to addr, wrapping the connection in TLS when configured.
func (d *Dialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	timeout := d.DialTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	nd := &net.Dialer{Timeout: timeout}
	conn, err := nd.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	if d.TLS == nil {
		return conn, nil
	}

	cfg := d.TLS.Clone()
	if cfg.ServerName == "" {
		cfg.ServerName = d.ServerName
	}
	if cfg.ServerName == "" {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			cfg.ServerName = host
		}
	}

	tlsConn := tls.Client(conn, cfg)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("TLS handshake with %s: %w", addr, err)
	}

	return tlsConn, nil
}
