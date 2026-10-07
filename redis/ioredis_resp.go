// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ioredisDefaultConnectTimeout = 10 * time.Second
	ioredisDefaultLoadingRetry   = 10 * time.Second
	ioredisLibraryVersion        = "5.11.1"
)

// IORedisRESPOptions configures the opt-in RESP2 compatibility transport.
type IORedisRESPOptions struct {
	Addr           string
	Username       string
	Password       string
	DB             int
	ConnectionName string

	TLSConfig      *tls.Config
	ConnectTimeout time.Duration
	SocketTimeout  time.Duration
	KeepAlive      time.Duration

	DisableTCPNoDelay   bool
	DisableTCPKeepAlive bool
	DisableClientInfo   bool
	DisableReadyCheck   bool
	MaxLoadingRetryTime time.Duration
	// clientInfoLibraryVersion is set by patch-specific compatibility profiles.
	clientInfoLibraryVersion string

	// DialContext overrides network dialing. nil uses net.Dialer.
	DialContext func(context.Context, string, string) (net.Conn, error)

	// readyWait allows deterministic package tests.
	readyWait func(context.Context, time.Duration) bool
}

// IORedisRESPTransportFactory creates ready standalone connections.
type IORedisRESPTransportFactory struct {
	options IORedisRESPOptions
}

// NewIORedisRESPTransportFactory validates options and returns a factory.
func NewIORedisRESPTransportFactory(options IORedisRESPOptions) (*IORedisRESPTransportFactory, error) {
	if strings.TrimSpace(options.Addr) == "" {
		return nil, errors.New("ioredis RESP address is required")
	}
	if options.DB < 0 {
		return nil, errors.New("ioredis RESP database must be non-negative")
	}
	if options.ConnectTimeout < 0 {
		return nil, errors.New("ioredis RESP connect timeout must be non-negative")
	}
	if options.SocketTimeout < 0 {
		return nil, errors.New("ioredis RESP socket timeout must be non-negative")
	}
	if options.KeepAlive < 0 {
		return nil, errors.New("ioredis RESP keepalive must be non-negative")
	}
	if options.MaxLoadingRetryTime < 0 {
		return nil, errors.New("ioredis RESP loading retry time must be non-negative")
	}
	if options.ConnectTimeout == 0 {
		options.ConnectTimeout = ioredisDefaultConnectTimeout
	}
	if options.MaxLoadingRetryTime == 0 {
		options.MaxLoadingRetryTime = ioredisDefaultLoadingRetry
	}
	if options.clientInfoLibraryVersion == "" {
		options.clientInfoLibraryVersion = ioredisLibraryVersion
	}
	if options.readyWait == nil {
		options.readyWait = waitIORedisRESPReady
	}
	return &IORedisRESPTransportFactory{options: options}, nil
}

// NewIORedisRESPCompatClient creates a client using the RESP2 transport.
func NewIORedisRESPCompatClient(options IORedisRESPOptions) (*IORedisCompatClient, error) {
	factory, err := NewIORedisRESPTransportFactory(options)
	if err != nil {
		return nil, err
	}
	return NewIORedisCompatClient(factory)
}

// NewIORedisRESPCompatClientReady waits for the RESP2 client to become ready.
func NewIORedisRESPCompatClientReady(ctx context.Context, options IORedisRESPOptions) (*IORedisCompatClient, error) {
	factory, err := NewIORedisRESPTransportFactory(options)
	if err != nil {
		return nil, err
	}
	return NewIORedisCompatClientReady(ctx, factory)
}

func (f *IORedisRESPTransportFactory) Connect(ctx context.Context) (IORedisTransport, error) {
	if f == nil {
		return nil, errors.New("ioredis RESP transport factory is not configured")
	}
	options := f.options
	dialCtx, cancel := context.WithTimeout(ctx, options.ConnectTimeout)
	defer cancel()

	dial := options.DialContext
	if dial == nil {
		dialer := &net.Dialer{Timeout: options.ConnectTimeout}
		dial = dialer.DialContext
	}
	connection, err := dial(dialCtx, "tcp", options.Addr)
	if err != nil {
		return nil, err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = connection.Close()
		}
	}()

	if tcp, ok := connection.(*net.TCPConn); ok {
		if !options.DisableTCPNoDelay {
			if err := tcp.SetNoDelay(true); err != nil {
				return nil, err
			}
		}
		if !options.DisableTCPKeepAlive {
			if err := tcp.SetKeepAlive(true); err != nil {
				return nil, err
			}
			if options.KeepAlive > 0 {
				if err := tcp.SetKeepAlivePeriod(options.KeepAlive); err != nil {
					return nil, err
				}
			}
		}
	}
	trackedConnection := &ioredisRESPCountingConn{Conn: connection}
	connection = trackedConnection

	if options.TLSConfig != nil {
		config := options.TLSConfig.Clone()
		if config.ServerName == "" && !config.InsecureSkipVerify {
			host, _, splitErr := net.SplitHostPort(options.Addr)
			if splitErr != nil {
				return nil, splitErr
			}
			config.ServerName = host
		}
		tlsConnection := tls.Client(connection, config)
		if err := tlsConnection.HandshakeContext(dialCtx); err != nil {
			return nil, err
		}
		connection = tlsConnection
	}

	transport := newIORedisRESPTransport(connection, options.SocketTimeout, trackedConnection)
	// ioredis clears connectTimeout on TCP/TLS connect. AUTH, SELECT, client
	// metadata and INFO therefore use the caller lifetime plus socket timeout,
	// not the connect timeout.
	if err := f.startup(ctx, transport); err != nil {
		_ = transport.Close()
		return nil, err
	}
	closeOnError = false
	return transport, nil
}

func (f *IORedisRESPTransportFactory) startup(ctx context.Context, transport *ioredisRESPTransport) error {
	options := f.options
	commands := make([][]string, 0, 5)
	authIndex := -1
	selectIndex := -1
	if options.Username != "" {
		authIndex = len(commands)
		commands = append(commands, []string{"auth", options.Username, options.Password})
	} else if options.Password != "" {
		authIndex = len(commands)
		commands = append(commands, []string{"auth", options.Password})
	}
	if options.DB != 0 {
		selectIndex = len(commands)
		commands = append(commands, []string{"select", strconv.Itoa(options.DB)})
	}
	if options.ConnectionName != "" {
		commands = append(commands, []string{"client", "setname", options.ConnectionName})
	}
	if !options.DisableClientInfo {
		// getPackageMeta() resolves LIB-VER on a microtask while LIB-NAME is
		// issued synchronously. The actual ioredis 5.11.1 wire order is NAME,
		// then VER even though event_handler.js builds the promises VER-first.
		commands = append(commands,
			[]string{"client", "SETINFO", "LIB-NAME", "ioredis"},
			[]string{"client", "SETINFO", "LIB-VER", options.clientInfoLibraryVersion},
		)
	}
	if len(commands) > 0 {
		exchange := transport.Exchange(ctx, commands)
		if exchange.Error != nil {
			return exchange.Error
		}
		if authIndex >= 0 && exchange.Replies[authIndex].Error != nil && !ioredisToleratesAuthError(exchange.Replies[authIndex].Error) {
			return exchange.Replies[authIndex].Error
		}
		// FIT.js rejects its initialization promise on a SELECT error before
		// ready. Failing the transport here preserves that startup boundary.
		if selectIndex >= 0 && exchange.Replies[selectIndex].Error != nil {
			return exchange.Replies[selectIndex].Error
		}
		// CLIENT SETNAME and SETINFO errors are intentionally ignored by
		// ioredis 5.11.1.
	}

	if options.DisableReadyCheck {
		return nil
	}
	for {
		exchange := transport.Exchange(ctx, [][]string{{"info"}})
		if exchange.Error != nil {
			return exchange.Error
		}
		reply := exchange.Replies[0]
		if reply.Error != nil {
			if strings.Contains(reply.Error.Error(), "NOPERM") {
				return nil
			}
			return reply.Error
		}
		info, ok := reply.Value.(string)
		if !ok {
			return nil
		}
		fields := parseIORedisINFO(info)
		if fields["loading"] == "" || fields["loading"] == "0" {
			return nil
		}
		delay := ioredisLoadingDelay(fields["loading_eta_seconds"], options.MaxLoadingRetryTime)
		if !options.readyWait(ctx, delay) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return errors.New("ioredis RESP ready wait stopped")
		}
	}
}

func ioredisToleratesAuthError(err error) bool {
	message := err.Error()
	return strings.Contains(message, "no password is set") ||
		strings.Contains(message, "without any password configured for the default user") ||
		strings.Contains(message, "wrong number of arguments for 'auth' command")
}

func parseIORedisINFO(value string) map[string]string {
	fields := make(map[string]string)
	for _, line := range strings.Split(value, "\r\n") {
		name, fieldValue, found := strings.Cut(line, ":")
		if found && fieldValue != "" {
			fields[name] = fieldValue
		}
	}
	return fields
}

func ioredisLoadingDelay(eta string, maximum time.Duration) time.Duration {
	seconds := float64(1)
	if eta != "" {
		if parsed, err := strconv.ParseFloat(eta, 64); err == nil {
			seconds = parsed
		} else {
			seconds = 0 // JavaScript setTimeout(NaN) schedules immediately.
		}
	}
	delay := time.Duration(seconds * float64(time.Second))
	if maximum > 0 && maximum < delay {
		return maximum
	}
	return delay
}

func waitIORedisRESPReady(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

type ioredisRESPRead struct {
	reply IORedisReply
	err   error
}

type ioredisRESPTransport struct {
	connection    net.Conn
	writeCounter  *ioredisRESPCountingConn
	socketTimeout time.Duration
	replies       chan ioredisRESPRead
	closed        chan struct{}
	closeOnce     sync.Once
	exchangeMu    sync.Mutex
	writeMu       sync.Mutex
	awaitingReply atomic.Bool
}

func newIORedisRESPTransport(connection net.Conn, socketTimeout time.Duration, writeCounter *ioredisRESPCountingConn) *ioredisRESPTransport {
	transport := &ioredisRESPTransport{
		connection:    connection,
		writeCounter:  writeCounter,
		socketTimeout: socketTimeout,
		replies:       make(chan ioredisRESPRead, 1),
		closed:        make(chan struct{}),
	}
	go transport.readLoop()
	return transport
}

func (t *ioredisRESPTransport) Closed() <-chan struct{} { return t.closed }

func (t *ioredisRESPTransport) Close() error {
	if t == nil {
		return nil
	}
	var closeErr error
	t.closeOnce.Do(func() {
		closeErr = t.connection.Close()
		close(t.closed)
	})
	return closeErr
}

func (t *ioredisRESPTransport) Exchange(ctx context.Context, commands [][]string) IORedisExchange {
	if t == nil || t.connection == nil {
		return IORedisExchange{WriteDisposition: IORedisNotWritten, Error: errors.New("ioredis RESP transport is not configured")}
	}
	if len(commands) == 0 {
		return IORedisExchange{WriteDisposition: IORedisNotWritten, Error: errors.New("ioredis RESP exchange is empty")}
	}

	t.exchangeMu.Lock()
	defer t.exchangeMu.Unlock()
	exchange := t.writeCommands(ctx, commands)
	if exchange.Error != nil {
		return exchange
	}
	defer t.finishReplyWait()
	for len(exchange.Replies) < len(commands) {
		reply, err := t.readReply(ctx)
		if err != nil {
			exchange.Error = err
			_ = t.Close()
			return exchange
		}
		exchange.Replies = append(exchange.Replies, reply)
	}
	return exchange
}

func (t *ioredisRESPTransport) writeCommands(_ context.Context, commands [][]string) IORedisExchange {
	if t == nil || t.connection == nil {
		return IORedisExchange{WriteDisposition: IORedisNotWritten, Error: errors.New("ioredis RESP transport is not configured")}
	}
	if len(commands) == 0 {
		return IORedisExchange{WriteDisposition: IORedisNotWritten, Error: errors.New("ioredis RESP exchange is empty")}
	}

	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	wire, err := encodeIORedisRESPCommands(commands)
	if err != nil {
		return IORedisExchange{WriteDisposition: IORedisNotWritten, Error: err}
	}
	exchange := IORedisExchange{BytesTotal: len(wire), WriteDisposition: IORedisNotWritten}
	networkStart := t.networkBytesWritten()

	written := 0
	for written < len(wire) {
		count, writeErr := t.connection.Write(wire[written:])
		if count > 0 {
			written += count
			exchange.BytesWritten = written
			exchange.MayHaveExecuted = true
			exchange.WriteDisposition = IORedisPartiallyWritten
		}
		if writeErr != nil {
			exchange.NetworkBytesWritten = t.networkBytesWritten() - networkStart
			if exchange.NetworkBytesWritten > 0 {
				exchange.MayHaveExecuted = true
				exchange.WriteDisposition = IORedisPartiallyWritten
			}
			exchange.Error = writeErr
			_ = t.Close()
			return exchange
		}
		if count == 0 {
			exchange.NetworkBytesWritten = t.networkBytesWritten() - networkStart
			if exchange.NetworkBytesWritten > 0 {
				exchange.MayHaveExecuted = true
				exchange.WriteDisposition = IORedisPartiallyWritten
			}
			exchange.Error = io.ErrNoProgress
			_ = t.Close()
			return exchange
		}
	}
	exchange.NetworkBytesWritten = t.networkBytesWritten() - networkStart
	exchange.WriteDisposition = IORedisFullyWritten
	return exchange
}

func (t *ioredisRESPTransport) readReply(ctx context.Context) (IORedisReply, error) {
	if t == nil || t.connection == nil {
		return IORedisReply{}, errors.New("ioredis RESP transport is not configured")
	}
	t.awaitingReply.Store(true)
	if t.socketTimeout > 0 {
		if err := t.connection.SetReadDeadline(time.Now().Add(t.socketTimeout)); err != nil {
			_ = t.Close()
			return IORedisReply{}, err
		}
	}
	select {
	case result := <-t.replies:
		if result.err != nil {
			return IORedisReply{}, t.normalizeReadError(result.err)
		}
		return result.reply, nil
	case <-ctx.Done():
		_ = t.Close()
		return IORedisReply{}, ctx.Err()
	case <-t.closed:
		// The reader publishes its precise error before closing. Prefer it if
		// already available, otherwise use the legacy close text.
		select {
		case result := <-t.replies:
			if result.err != nil {
				return IORedisReply{}, t.normalizeReadError(result.err)
			}
			return result.reply, nil
		default:
			return IORedisReply{}, IORedisConnectionClosedError{}
		}
	}
}

func (t *ioredisRESPTransport) finishReplyWait() {
	if t == nil || t.connection == nil {
		return
	}
	t.awaitingReply.Store(false)
	if t.socketTimeout > 0 {
		_ = t.connection.SetReadDeadline(time.Time{})
	}
}

func (t *ioredisRESPTransport) networkBytesWritten() int64 {
	if t.writeCounter == nil {
		return 0
	}
	return t.writeCounter.written.Load()
}

func (t *ioredisRESPTransport) normalizeReadError(err error) error {
	if err == nil {
		return IORedisConnectionClosedError{}
	}
	var networkError net.Error
	if t.socketTimeout > 0 && errors.As(err, &networkError) && networkError.Timeout() {
		return fmt.Errorf("Socket timeout. Expecting data, but didn't receive any in %dms.", t.socketTimeout.Milliseconds())
	}
	return err
}

func (t *ioredisRESPTransport) readLoop() {
	reader := bufio.NewReader(ioredisRESPActivityReader{transport: t})
	for {
		value, replyErr, err := readIORedisRESPValue(reader)
		if err != nil {
			select {
			case t.replies <- ioredisRESPRead{err: err}:
			case <-t.closed:
				return
			}
			_ = t.Close()
			return
		}
		select {
		case t.replies <- ioredisRESPRead{reply: IORedisReply{Value: value, Error: replyErr}}:
		case <-t.closed:
			return
		}
	}
}

type ioredisRESPActivityReader struct {
	transport *ioredisRESPTransport
}

type ioredisRESPCountingConn struct {
	net.Conn
	written atomic.Int64
}

func (c *ioredisRESPCountingConn) Write(buffer []byte) (int, error) {
	count, err := c.Conn.Write(buffer)
	c.written.Add(int64(count))
	return count, err
}

func (r ioredisRESPActivityReader) Read(buffer []byte) (int, error) {
	count, err := r.transport.connection.Read(buffer)
	if count > 0 && r.transport.socketTimeout > 0 && r.transport.awaitingReply.Load() {
		// ioredis resets socketTimeout on every data event, including a partial
		// RESP frame, rather than only after a complete reply is parsed.
		_ = r.transport.connection.SetReadDeadline(time.Now().Add(r.transport.socketTimeout))
	}
	return count, err
}

func encodeIORedisRESPCommands(commands [][]string) ([]byte, error) {
	var buffer bytes.Buffer
	for _, command := range commands {
		if len(command) == 0 || command[0] == "" {
			return nil, errors.New("ioredis RESP command is empty")
		}
		fmt.Fprintf(&buffer, "*%d\r\n", len(command))
		for _, argument := range command {
			fmt.Fprintf(&buffer, "$%d\r\n", len(argument))
			buffer.WriteString(argument)
			buffer.WriteString("\r\n")
		}
	}
	return buffer.Bytes(), nil
}

func readIORedisRESPValue(reader *bufio.Reader) (any, error, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, nil, err
	}
	switch prefix {
	case '+':
		line, err := readIORedisRESPLine(reader)
		return line, nil, err
	case '-':
		line, err := readIORedisRESPLine(reader)
		if err != nil {
			return nil, nil, err
		}
		return nil, errors.New(line), nil
	case ':':
		line, err := readIORedisRESPLine(reader)
		if err != nil {
			return nil, nil, err
		}
		integer, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid RESP integer %q: %w", line, err)
		}
		return integer, nil, nil
	case '$':
		line, err := readIORedisRESPLine(reader)
		if err != nil {
			return nil, nil, err
		}
		length, err := strconv.ParseInt(line, 10, 64)
		if err != nil || length < -1 {
			return nil, nil, fmt.Errorf("invalid RESP bulk length %q", line)
		}
		if length == -1 {
			return nil, nil, nil
		}
		if length > int64(^uint(0)>>1)-2 {
			return nil, nil, errors.New("RESP bulk length overflows int")
		}
		payload := make([]byte, int(length)+2)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, nil, err
		}
		if payload[len(payload)-2] != '\r' || payload[len(payload)-1] != '\n' {
			return nil, nil, errors.New("invalid RESP bulk terminator")
		}
		return string(payload[:len(payload)-2]), nil, nil
	case '*':
		line, err := readIORedisRESPLine(reader)
		if err != nil {
			return nil, nil, err
		}
		length, err := strconv.ParseInt(line, 10, 64)
		if err != nil || length < -1 {
			return nil, nil, fmt.Errorf("invalid RESP array length %q", line)
		}
		if length == -1 {
			return nil, nil, nil
		}
		if length > int64(^uint(0)>>1) {
			return nil, nil, errors.New("RESP array length overflows int")
		}
		values := make([]any, int(length))
		for index := range values {
			value, replyErr, err := readIORedisRESPValue(reader)
			if err != nil {
				return nil, nil, err
			}
			if replyErr != nil {
				values[index] = replyErr
			} else {
				values[index] = value
			}
		}
		return values, nil, nil
	default:
		return nil, nil, fmt.Errorf("unsupported RESP2 prefix %q", prefix)
	}
}

func readIORedisRESPLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2:] != "\r\n" {
		return "", errors.New("invalid RESP line terminator")
	}
	return line[:len(line)-2], nil
}
