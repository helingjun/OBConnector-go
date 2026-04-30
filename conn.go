package oceanbase

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

type Conn struct {
	netConn net.Conn
	packets *protocol.PacketConn
	cfg     *Config
	mu      sync.Mutex
	closed  bool
	bad     bool
	inTx    bool
}

type handshake struct {
	serverVersion string
	connectionID  uint32
	capabilities  uint32
	authPlugin    string
	authSeed      []byte
	status        uint16
}

func dialAndHandshake(ctx context.Context, cfg *Config) (*Conn, error) {
	var d net.Dialer
	if cfg.Timeout > 0 {
		d.Timeout = cfg.Timeout
	}
	netConn, err := d.DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	c := &Conn{
		netConn: netConn,
		packets: protocol.NewPacketConn(netConn),
		cfg:     cfg,
	}
	if err := c.withDeadline(ctx, func() error { return c.handshake() }); err != nil {
		_ = netConn.Close()
		return nil, err
	}
	for _, query := range cfg.InitSQL {
		c.tracef("init query: %s", query)
		if _, err := c.execLocked(ctx, query); err != nil {
			_ = netConn.Close()
			return nil, fmt.Errorf("init query %q failed: %w", query, err)
		}
	}
	return c, nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if query == "" {
		return nil, errors.New("oceanbase: empty statement")
	}
	return &Stmt{conn: c, query: query}, nil
}

func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.netConn == nil {
		return nil
	}
	c.packets.ResetSequence()
	_ = c.packets.WritePacket([]byte{protocol.ComQuit})
	return c.netConn.Close()
}

func (c *Conn) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && !c.bad
}

func (c *Conn) ResetSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkUsableLocked(); err != nil {
		return err
	}
	if !c.inTx {
		c.tracef("reset session: no active transaction")
		return nil
	}
	c.tracef("reset session: rollback active transaction")
	if _, err := c.execLocked(ctx, "rollback"); err != nil {
		return c.markBadIfConnErr(err)
	}
	c.inTx = false
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != driver.IsolationLevel(0) {
		return nil, errors.New("oceanbase: custom transaction isolation is not implemented")
	}
	if opts.ReadOnly {
		return nil, errors.New("oceanbase: read-only transactions are not implemented")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inTx {
		return nil, errors.New("oceanbase: transaction already active")
	}
	c.inTx = true
	return &Tx{conn: c}, nil
}

func (c *Conn) Ping(ctx context.Context) error {
	rows, err := c.QueryContext(ctx, "select 1 from dual", nil)
	if err != nil {
		return err
	}
	return rows.Close()
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	var err error
	query, err = interpolateParams(query, args)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if err := c.checkUsableLocked(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	rows, err := c.queryLocked(ctx, query)
	if err != nil {
		c.mu.Unlock()
		return nil, c.markBadIfConnErr(err)
	}
	if r, ok := rows.(*Rows); ok && r.streaming {
		r.release = c.mu.Unlock
	} else {
		c.mu.Unlock()
	}
	return rows, nil
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var err error
	query, err = interpolateParams(query, args)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkUsableLocked(); err != nil {
		return nil, err
	}
	res, err := c.execLocked(ctx, query)
	if err != nil {
		return nil, c.markBadIfConnErr(err)
	}
	return res, nil
}

func (c *Conn) CheckNamedValue(v *driver.NamedValue) error {
	return driver.ErrSkip
}

func (c *Conn) handshake() error {
	c.packets.ResetSequence()
	packet, err := c.packets.ReadPacket()
	if err != nil {
		return err
	}
	if len(packet) > 0 && packet[0] == protocol.ErrPacket {
		return parseServerError(packet)
	}

	hs, err := parseHandshake(packet)
	if err != nil {
		return err
	}
	c.tracef(
		"server handshake: version=%q connection_id=%d capability=0x%08x flags=%s status=0x%04x auth_plugin=%q seed_len=%d",
		hs.serverVersion,
		hs.connectionID,
		hs.capabilities,
		capabilityNames(hs.capabilities),
		hs.status,
		hs.authPlugin,
		len(hs.authSeed),
	)
	if hs.authPlugin == "" {
		hs.authPlugin = "mysql_native_password"
	}
	if hs.authPlugin != "mysql_native_password" {
		return fmt.Errorf("oceanbase: auth plugin %q is not implemented", hs.authPlugin)
	}

	response := c.buildHandshakeResponse(hs)
	c.tracef("client handshake response: payload_len=%d", len(response))
	if err := c.packets.WritePacket(response); err != nil {
		return err
	}

	authResult, err := c.packets.ReadPacket()
	if err != nil {
		return err
	}
	if len(authResult) == 0 {
		return io.ErrUnexpectedEOF
	}
	switch authResult[0] {
	case protocol.OKPacket:
		c.tracef("auth result: OK")
		return nil
	case protocol.ErrPacket:
		c.tracef("auth result: ERR")
		return parseServerError(authResult)
	default:
		return fmt.Errorf("oceanbase: unexpected auth response 0x%02x", authResult[0])
	}
}

func (c *Conn) buildHandshakeResponse(hs *handshake) []byte {
	baseCaps := protocol.ClientLongPassword |
		protocol.ClientLongFlag |
		protocol.ClientProtocol41 |
		protocol.ClientTransactions |
		protocol.ClientSecureConnection |
		protocol.ClientMultiResults |
		protocol.ClientPluginAuth |
		protocol.ClientPluginAuthLenencClientData |
		protocol.ClientConnectAttrs |
		protocol.ClientSessionTrack |
		protocol.ClientSupportOracleMode
	baseCaps |= presetCapabilities(c.cfg.Preset)
	caps := baseCaps
	if hs.capabilities&protocol.ClientProtocol41 == 0 {
		caps &^= protocol.ClientProtocol41
	}
	if c.cfg.Database != "" {
		caps |= protocol.ClientConnectWithDB
	}
	caps |= c.cfg.CapabilityAdd
	caps &^= c.cfg.CapabilityDrop
	c.tracef(
		"client capabilities: base=0x%08x add=0x%08x drop=0x%08x final=0x%08x flags=%s",
		baseCaps,
		c.cfg.CapabilityAdd,
		c.cfg.CapabilityDrop,
		caps,
		capabilityNames(caps),
	)

	out := make([]byte, 0, 128)
	out = binary.LittleEndian.AppendUint32(out, caps)
	out = binary.LittleEndian.AppendUint32(out, protocol.DefaultMaxPacketSize)
	out = append(out, c.cfg.Collation)
	out = append(out, make([]byte, 23)...)
	out = append(out, c.cfg.User...)
	out = append(out, 0x00)
	out = protocol.PutLengthEncodedString(out, string(protocol.NativePasswordAuth(c.cfg.Password, hs.authSeed)))
	if caps&protocol.ClientConnectWithDB != 0 {
		out = append(out, c.cfg.Database...)
		out = append(out, 0x00)
	}
	if caps&protocol.ClientPluginAuth != 0 {
		out = append(out, hs.authPlugin...)
		out = append(out, 0x00)
	}
	if caps&protocol.ClientConnectAttrs != 0 {
		attrs := c.connectionAttributes(hs)
		attrPayload := make([]byte, 0, 128)
		for _, kv := range attrs {
			c.tracef("client attr: %s=%q", kv[0], kv[1])
			attrPayload = protocol.PutLengthEncodedString(attrPayload, kv[0])
			attrPayload = protocol.PutLengthEncodedString(attrPayload, kv[1])
		}
		out = protocol.PutLengthEncodedInt(out, uint64(len(attrPayload)))
		out = append(out, attrPayload...)
	}
	return out
}

func (c *Conn) connectionAttributes(hs *handshake) [][2]string {
	attrMap := map[string]string{
		"_client_name":      "obconnector-go",
		"_client_version":   Version,
		"_os":               runtime.GOOS,
		"_platform":         runtime.GOARCH,
		"program_name":      os.Args[0],
		"ob_server_version": hs.serverVersion,
	}
	for k, v := range presetAttributes(c.cfg.Preset) {
		attrMap[k] = v
	}
	for k, v := range c.cfg.Attributes {
		attrMap[k] = v
	}

	keys := make([]string, 0, len(attrMap))
	for k := range attrMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	attrs := make([][2]string, 0, len(keys))
	for _, k := range keys {
		attrs = append(attrs, [2]string{k, attrMap[k]})
	}
	return attrs
}

func (c *Conn) queryLocked(ctx context.Context, query string) (driver.Rows, error) {
	var rows driver.Rows
	err := c.withDeadline(ctx, func() error {
		if err := c.writeQuery(query); err != nil {
			return err
		}
		r, err := c.readQueryResult()
		if err != nil {
			return err
		}
		rows = r
		return nil
	})
	return rows, err
}

func (c *Conn) execLocked(ctx context.Context, query string) (driver.Result, error) {
	var result driver.Result
	err := c.withDeadline(ctx, func() error {
		if err := c.writeQuery(query); err != nil {
			return err
		}
		first, err := c.packets.ReadPacket()
		if err != nil {
			return err
		}
		res, err := c.readResultFromFirstPacket(first)
		if err != nil {
			return err
		}
		result = res
		return nil
	})
	return result, err
}

func (c *Conn) writeQuery(query string) error {
	c.tracef("query: %s", query)
	c.packets.ResetSequence()
	return c.packets.WritePacket(append([]byte{protocol.ComQuery}, query...))
}

func (c *Conn) tracef(format string, args ...any) {
	if c == nil || c.cfg == nil || !c.cfg.Trace || c.cfg.TraceWriter == nil {
		return
	}
	_, _ = fmt.Fprintf(c.cfg.TraceWriter, "obconnector-go: "+format+"\n", args...)
}

func (c *Conn) checkUsableLocked() error {
	if c.closed || c.bad {
		return driver.ErrBadConn
	}
	return nil
}

func (c *Conn) markBadIfConnErr(err error) error {
	if err == nil {
		return nil
	}
	if isBadConnError(err) {
		c.bad = true
		return driver.ErrBadConn
	}
	return err
}

func (c *Conn) withDeadline(ctx context.Context, fn func() error) error {
	deadline, ok := ctx.Deadline()
	if !ok && c.cfg.Timeout > 0 {
		deadline = time.Now().Add(c.cfg.Timeout)
		ok = true
	}
	if ok {
		if err := c.netConn.SetDeadline(deadline); err != nil {
			return err
		}
		defer c.netConn.SetDeadline(time.Time{})
	}

	cancelled := make(chan struct{})
	if ctx.Done() != nil {
		defer close(cancelled)
		go func() {
			select {
			case <-ctx.Done():
				_ = c.netConn.SetDeadline(time.Now())
			case <-cancelled:
			}
		}()
	}

	err := fn()
	if ctxErr := ctx.Err(); ctxErr != nil {
		c.bad = true
		return ctxErr
	}
	return err
}

func parseHandshake(packet []byte) (*handshake, error) {
	if len(packet) < 34 {
		return nil, io.ErrUnexpectedEOF
	}
	if packet[0] != 10 {
		return nil, fmt.Errorf("unsupported protocol version %d", packet[0])
	}

	pos := 1
	serverVersion, used, err := readNullTerminated(packet[pos:])
	if err != nil {
		return nil, err
	}
	pos += used
	if len(packet) < pos+13 {
		return nil, io.ErrUnexpectedEOF
	}

	hs := &handshake{
		serverVersion: serverVersion,
		connectionID:  binary.LittleEndian.Uint32(packet[pos : pos+4]),
	}
	pos += 4
	seed1 := append([]byte(nil), packet[pos:pos+8]...)
	pos += 9
	hs.capabilities = uint32(binary.LittleEndian.Uint16(packet[pos : pos+2]))
	pos += 2
	if len(packet) <= pos {
		hs.authSeed = seed1
		return hs, nil
	}
	pos++ // character set
	hs.status = binary.LittleEndian.Uint16(packet[pos : pos+2])
	pos += 2
	hs.capabilities |= uint32(binary.LittleEndian.Uint16(packet[pos:pos+2])) << 16
	pos += 2

	authPluginDataLen := 0
	if hs.capabilities&protocol.ClientPluginAuth != 0 {
		authPluginDataLen = int(packet[pos])
	}
	pos++
	pos += 10

	seed2Len := 12
	if authPluginDataLen > 0 {
		seed2Len = authPluginDataLen - 8
		if seed2Len < 12 {
			seed2Len = 12
		}
	}
	if len(packet) < pos+seed2Len {
		seed2Len = len(packet) - pos
	}
	if seed2Len > 0 {
		seed2 := append([]byte(nil), packet[pos:pos+seed2Len]...)
		hs.authSeed = append(seed1, bytes.TrimRight(seed2, "\x00")...)
		pos += seed2Len
	} else {
		hs.authSeed = seed1
	}
	if hs.capabilities&protocol.ClientPluginAuth != 0 && pos < len(packet) {
		plugin, _, err := readNullTerminated(packet[pos:])
		if err == nil {
			hs.authPlugin = plugin
		}
	}
	return hs, nil
}

func readNullTerminated(src []byte) (string, int, error) {
	for i, b := range src {
		if b == 0x00 {
			return string(src[:i]), i + 1, nil
		}
	}
	return "", 0, io.ErrUnexpectedEOF
}

type result struct {
	affectedRows int64
	lastInsertID int64
}

func (r result) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r result) RowsAffected() (int64, error) { return r.affectedRows, nil }

func parseOK(packet []byte) (driver.Result, error) {
	if len(packet) == 0 || packet[0] != protocol.OKPacket {
		return nil, fmt.Errorf("not an OK packet")
	}
	pos := 1
	affected, used, _, err := protocol.ReadLengthEncodedInt(packet[pos:])
	if err != nil {
		return nil, err
	}
	pos += used
	lastID, _, _, err := protocol.ReadLengthEncodedInt(packet[pos:])
	if err != nil {
		return nil, err
	}
	return result{affectedRows: int64(affected), lastInsertID: int64(lastID)}, nil
}
