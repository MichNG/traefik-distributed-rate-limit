package redis

// Minimal, dependency-free Redis RESP2 primitives.
//
// The plugin runs inside Traefik's Yaegi interpreter, where only the Go
// standard library is importable. That is why this package re-implements the
// small subset of the protocol the plugin needs instead of using
// github.com/redis/go-redis (whose v9 generics are not interpretable).

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type respKind int

const (
	respSimple respKind = iota
	respError
	respInteger
	respBulk
	respArray
	respNil
)

// respValue is a decoded RESP2 reply.
type respValue struct {
	kind respKind
	str  string
	num  int64
	arr  []respValue
}

// Error implements the error interface so that a RESP error can be carried
// around as an error value.
func (v respValue) Error() string { return v.str }

// isError reports whether the reply is a RESP error.
func (v respValue) isError() bool { return v.kind == respError }

// String returns the bulk/simple string value, or "" for other kinds.
func (v respValue) String() string {
	if v.kind == respSimple || v.kind == respBulk {
		return v.str
	}
	return ""
}

// Int returns the integer value.
func (v respValue) Int() (int64, error) {
	if v.kind == respInteger {
		return v.num, nil
	}
	if v.kind == respBulk || v.kind == respSimple {
		return strconv.ParseInt(v.str, 10, 64)
	}
	return 0, fmt.Errorf("resp: not an integer: kind=%d", v.kind)
}

// Float returns the numeric value as a float64.
func (v respValue) Float() (float64, error) {
	if v.kind == respInteger {
		return float64(v.num), nil
	}
	if v.kind == respBulk || v.kind == respSimple {
		return strconv.ParseFloat(v.str, 64)
	}
	return 0, fmt.Errorf("resp: not a number: kind=%d", v.kind)
}

// Array returns the array elements, nil for a null array.
func (v respValue) Array() []respValue { return v.arr }

// Len returns the number of array items (0 for non-array replies).
func (v respValue) Len() int { return len(v.arr) }

// At returns the i-th array item, or an error when out of range.
func (v respValue) At(i int) (respValue, error) {
	if i < 0 || i >= len(v.arr) {
		return respValue{}, fmt.Errorf("resp: index %d out of range (len=%d)", i, len(v.arr))
	}
	return v.arr[i], nil
}

// writeCommand encodes a command as a RESP array of bulk strings.
func writeCommand(w io.Writer, args ...string) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := io.WriteString(w, sb.String()); err != nil {
		if len(args) > 0 {
			return fmt.Errorf("writing command %q: %w", args[0], err)
		}
		return fmt.Errorf("writing command: %w", err)
	}
	return nil
}

// readValue decodes a single RESP2 reply. Bulk strings are read by length so
// that binary payloads are safe.
func readValue(r *bufio.Reader) (respValue, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return respValue{}, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return respValue{}, errors.New("resp: empty reply line")
	}

	switch line[0] {
	case '+':
		return respValue{kind: respSimple, str: line[1:]}, nil
	case '-':
		return respValue{kind: respError, str: line[1:]}, nil
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return respValue{}, fmt.Errorf("resp: malformed integer %q: %w", line, err)
		}
		return respValue{kind: respInteger, num: n}, nil
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return respValue{}, fmt.Errorf("resp: malformed bulk length %q: %w", line, err)
		}
		if n < 0 {
			return respValue{kind: respNil}, nil
		}
		buf := make([]byte, n+2) // payload + CRLF
		if _, err := io.ReadFull(r, buf); err != nil {
			return respValue{}, fmt.Errorf("resp: reading bulk payload: %w", err)
		}
		return respValue{kind: respBulk, str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return respValue{}, fmt.Errorf("resp: malformed array length %q: %w", line, err)
		}
		if n < 0 {
			return respValue{kind: respNil}, nil
		}
		items := make([]respValue, 0, n)
		for i := 0; i < n; i++ {
			item, err := readValue(r)
			if err != nil {
				return respValue{}, fmt.Errorf("resp: reading array item %d: %w", i, err)
			}
			items = append(items, item)
		}
		return respValue{kind: respArray, arr: items}, nil
	default:
		return respValue{}, fmt.Errorf("resp: unsupported reply %q", line)
	}
}

// respConn is a connection plus its buffered reader. Keeping one reader per
// connection is mandatory: on a pub/sub connection, push messages may arrive
// in the same TCP segment as the SUBSCRIBE confirmation, and a per-call reader
// would silently swallow them.
type respConn struct {
	conn net.Conn
	rd   *bufio.Reader
}

func newRespConn(conn net.Conn) *respConn {
	return &respConn{conn: conn, rd: bufio.NewReader(conn)}
}

func (c *respConn) do(args ...string) (respValue, error) {
	if err := writeCommand(c.conn, args...); err != nil {
		return respValue{}, err
	}
	return readValue(c.rd)
}

func (c *respConn) close() { _ = c.conn.Close() }

// setDeadline applies write/read deadlines; a zero duration disables the
// corresponding deadline.
func setDeadline(conn net.Conn, write, read time.Duration) error {
	var deadline time.Time
	if write > 0 {
		deadline = time.Now().Add(write)
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}

	deadline = time.Time{}
	if read > 0 {
		deadline = time.Now().Add(read)
	}
	return conn.SetReadDeadline(deadline)
}
