package redis

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCreateTLSConfigNil(t *testing.T) {
	var cfg *TLSConfig

	got, err := cfg.CreateTLSConfig()
	if err != nil || got != nil {
		t.Fatalf("CreateTLSConfig(nil) = (%v,%v), want (nil,nil)", got, err)
	}
}

func TestCreateTLSConfigCAFromContent(t *testing.T) {
	pki := newTestPKI(t)

	cfg := &TLSConfig{CA: pki.CACert}
	got, err := cfg.CreateTLSConfig()
	if err != nil {
		t.Fatalf("CreateTLSConfig: %v", err)
	}
	if got == nil || got.RootCAs == nil {
		t.Fatal("expected RootCAs to be set from the inline PEM")
	}
	if got.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify must be false")
	}
}

func TestCreateTLSConfigCAFromFile(t *testing.T) {
	pki := newTestPKI(t)

	path := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(path, []byte(pki.CACert), 0o600); err != nil {
		t.Fatalf("writing CA: %v", err)
	}

	got, err := (&TLSConfig{CA: path}).CreateTLSConfig()
	if err != nil {
		t.Fatalf("CreateTLSConfig: %v", err)
	}
	if got.RootCAs == nil {
		t.Fatal("expected RootCAs to be set from the CA file")
	}
}

func TestCreateTLSConfigInvalidCA(t *testing.T) {
	_, err := (&TLSConfig{CA: "not a pem"}).CreateTLSConfig()
	if err == nil || !strings.Contains(err.Error(), "failed to parse CA") {
		t.Fatalf("err = %v, want a 'failed to parse CA' error", err)
	}
}

func TestCreateTLSConfigCertWithoutKey(t *testing.T) {
	pki := newTestPKI(t)

	if _, err := (&TLSConfig{Cert: pki.LeafCert}).CreateTLSConfig(); err == nil {
		t.Fatal("expected an error when only the cert is set")
	}
	if _, err := (&TLSConfig{Key: pki.LeafKey}).CreateTLSConfig(); err == nil {
		t.Fatal("expected an error when only the key is set")
	}
}

func TestCreateTLSConfigCertAndKeyFromContentAndFiles(t *testing.T) {
	pki := newTestPKI(t)

	// Inline PEM content.
	got, err := (&TLSConfig{Cert: pki.LeafCert, Key: pki.LeafKey}).CreateTLSConfig()
	if err != nil {
		t.Fatalf("CreateTLSConfig (content): %v", err)
	}
	if len(got.Certificates) != 1 {
		t.Fatalf("certificates = %d, want 1", len(got.Certificates))
	}

	// Files on disk.
	dir := t.TempDir()
	certPath := filepath.Join(dir, "client.crt")
	keyPath := filepath.Join(dir, "client.key")
	if err := os.WriteFile(certPath, []byte(pki.LeafCert), 0o600); err != nil {
		t.Fatalf("writing cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte(pki.LeafKey), 0o600); err != nil {
		t.Fatalf("writing key: %v", err)
	}

	got, err = (&TLSConfig{Cert: certPath, Key: keyPath}).CreateTLSConfig()
	if err != nil {
		t.Fatalf("CreateTLSConfig (files): %v", err)
	}
	if len(got.Certificates) != 1 {
		t.Fatalf("certificates = %d, want 1", len(got.Certificates))
	}
}

func TestCreateTLSConfigMissingFiles(t *testing.T) {
	_, err := (&TLSConfig{Cert: "/does/not/exist.crt", Key: "/does/not/exist.key"}).CreateTLSConfig()
	if err == nil || !strings.Contains(err.Error(), "cert file does not exist") {
		t.Fatalf("err = %v, want a 'cert file does not exist' error", err)
	}
}

func TestDialerWithoutTLS(t *testing.T) {
	fs := newFakeSentinel(t)

	conn, err := (&Dialer{DialTimeout: time.Second}).Dial(context.Background(), fs.addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = conn.Close()
}

func TestDialerWithTLS(t *testing.T) {
	pki := newTestPKI(t)
	server := newFakeRedis(t, pki.TLSConfig)

	cfg, err := (&TLSConfig{CA: pki.CACert}).CreateTLSConfig()
	if err != nil {
		t.Fatalf("CreateTLSConfig: %v", err)
	}

	conn, err := (&Dialer{TLS: cfg, DialTimeout: time.Second}).Dial(context.Background(), server.addr())
	if err != nil {
		t.Fatalf("Dial over TLS: %v", err)
	}
	defer conn.Close()

	rc := newRespConn(conn)
	if err := setDeadline(conn, time.Second, time.Second); err != nil {
		t.Fatalf("setDeadline: %v", err)
	}
	reply, err := rc.do("PING")
	if err != nil {
		t.Fatalf("PING over TLS: %v", err)
	}
	if reply.String() != "PONG" {
		t.Fatalf("PING reply = %q, want PONG", reply.String())
	}
}

func TestRespReadValue(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"+OK\r\n", "OK"},
		{"$5\r\nhello\r\n", "hello"},
		{"$-1\r\n", ""},
		{"*2\r\n$1\r\na\r\n$1\r\nb\r\n", "ab"},
	}

	for _, c := range cases {
		got, err := readValue(stringReader(c.raw))
		if err != nil {
			t.Fatalf("readValue(%q): %v", c.raw, err)
		}

		var sb strings.Builder
		for _, item := range got.Array() {
			sb.WriteString(item.String())
		}
		if sb.Len() == 0 {
			sb.WriteString(got.String())
		}
		if sb.String() != c.want {
			t.Errorf("readValue(%q) = %q, want %q", c.raw, sb.String(), c.want)
		}
	}

	if _, err := readValue(stringReader("*@bad\r\n")); err == nil {
		t.Error("expected an error for a malformed array header")
	}
}
