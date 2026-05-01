package oceanbase

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

type Rows struct {
	conn      *Conn
	columns   []string
	types     []byte
	values    [][]driver.Value
	pos       int
	streaming bool
	binary    bool
	done      bool
	closed    bool
	release   func()
}

func (r *Rows) Columns() []string {
	return append([]string(nil), r.columns...)
}

func (r *Rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.values = nil
	var err error
	if r.streaming && !r.done {
		err = r.drain()
	}
	r.finish()
	return err
}

func (r *Rows) finish() {
	r.done = true
	if r.release != nil {
		r.release()
		r.release = nil
	}
}

func (r *Rows) drain() error {
	for {
		packet, err := r.conn.packets.ReadPacket()
		if err != nil {
			_ = r.conn.markBadIfConnErr(err)
			return err
		}
		if isEOFOrOK(packet) {
			return nil
		}
		if len(packet) > 0 && packet[0] == protocol.ErrPacket {
			return parseServerError(packet)
		}
	}
}

func (r *Rows) nextStreaming(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	packet, err := r.conn.packets.ReadPacket()
	if err != nil {
		_ = r.conn.markBadIfConnErr(err)
		r.finish()
		return err
	}
	if isEOFOrOK(packet) {
		r.finish()
		return io.EOF
	}
	if len(packet) > 0 && packet[0] == protocol.ErrPacket {
		r.finish()
		return parseServerError(packet)
	}
	var row []driver.Value
	if r.binary {
		binaryRow, err := protocol.ParseBinaryRow(packet, len(r.columns), r.types)
		if err != nil {
			r.finish()
			return err
		}
		row = make([]driver.Value, len(binaryRow))
		for i, v := range binaryRow {
			row[i] = v
		}
	} else {
		textRow, err := parseTextRow(packet, len(r.columns), r.types)
		if err != nil {
			r.finish()
			return err
		}
		row = textRow
	}
	copy(dest, row)
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.streaming {
		return r.nextStreaming(dest)
	}
	if r.pos >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.pos])
	r.pos++
	return nil
}

func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.types) {
		return ""
	}
	return databaseTypeName(r.types[index])
}

func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.types) {
		return reflect.TypeOf("")
	}
	return scanType(r.types[index])
}

func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return true, true
}

func (c *Conn) readQueryResult() (driver.Rows, error) {
	first, err := c.packets.ReadPacket()
	if err != nil {
		return nil, err
	}
	if len(first) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	if first[0] == protocol.ErrPacket {
		return nil, parseServerError(first)
	}
	if first[0] == protocol.OKPacket {
		return &Rows{done: true}, nil
	}

	columnCount, _, _, err := protocol.ReadLengthEncodedInt(first)
	if err != nil {
		return nil, err
	}
	columns := make([]string, 0, columnCount)
	types := make([]byte, 0, columnCount)
	for i := uint64(0); i < columnCount; i++ {
		packet, err := c.packets.ReadPacket()
		if err != nil {
			return nil, err
		}
		name, typ, err := parseColumnDefinition(packet)
		if err != nil {
			return nil, err
		}
		columns = append(columns, name)
		types = append(types, typ)
	}
	if err := c.readEOFOrOK(); err != nil {
		return nil, err
	}

	return &Rows{conn: c, columns: columns, types: types, streaming: true}, nil
}

func (c *Conn) readResultFromFirstPacket(packet []byte) (driver.Result, error) {
	if len(packet) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	switch packet[0] {
	case protocol.OKPacket:
		return c.handleOK(packet)
	case protocol.ErrPacket:
		return nil, parseServerError(packet)
	default:
		// Exec can still be called with a SELECT. Drain the result set to keep the connection usable.
		if _, err := c.readQueryResultAfterColumnCount(packet); err != nil {
			return nil, err
		}
		return result{}, nil
	}
}

func (c *Conn) readQueryResultAfterColumnCount(first []byte) (driver.Rows, error) {
	columnCount, _, _, err := protocol.ReadLengthEncodedInt(first)
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < columnCount; i++ {
		if _, err := c.packets.ReadPacket(); err != nil {
			return nil, err
		}
	}
	if err := c.readEOFOrOK(); err != nil {
		return nil, err
	}
	for {
		packet, err := c.packets.ReadPacket()
		if err != nil {
			return nil, err
		}
		if isEOFOrOK(packet) {
			break
		}
	}
	return &Rows{}, nil
}

func (c *Conn) readEOFOrOK() error {
	packet, err := c.packets.ReadPacket()
	if err != nil {
		return err
	}
	if len(packet) > 0 && packet[0] == protocol.ErrPacket {
		return parseServerError(packet)
	}
	if !isEOFOrOK(packet) {
		return fmt.Errorf("expected EOF/OK packet, got 0x%02x", packet[0])
	}
	return nil
}

func isEOFOrOK(packet []byte) bool {
	return len(packet) > 0 && (packet[0] == protocol.EOFPacket || packet[0] == protocol.OKPacket) && len(packet) < 9
}

func parseColumnDefinition(packet []byte) (name string, typ byte, err error) {
	pos := 0
	for i := 0; i < 4; i++ {
		_, used, _, err := protocol.ReadLengthEncodedString(packet[pos:])
		if err != nil {
			return "", 0, err
		}
		pos += used
	}
	nameBytes, used, _, err := protocol.ReadLengthEncodedString(packet[pos:])
	if err != nil {
		return "", 0, err
	}
	pos += used
	_, used, _, err = protocol.ReadLengthEncodedString(packet[pos:])
	if err != nil {
		return "", 0, err
	}
	pos += used
	if len(packet) < pos+12 {
		return string(nameBytes), 0, io.ErrUnexpectedEOF
	}
	return string(nameBytes), packet[pos+5], nil
}

func parseTextRow(packet []byte, columnCount int, types []byte) ([]driver.Value, error) {
	row := make([]driver.Value, columnCount)
	pos := 0
	for i := 0; i < columnCount; i++ {
		if pos >= len(packet) {
			return nil, io.ErrUnexpectedEOF
		}
		if packet[pos] == protocol.NullColumn {
			row[i] = nil
			pos++
			continue
		}
		raw, used, _, err := protocol.ReadLengthEncodedString(packet[pos:])
		if err != nil {
			return nil, err
		}
		pos += used
		row[i] = textValue(raw, types[i])
	}
	return row, nil
}

func textValue(raw []byte, typ byte) driver.Value {
	s := string(raw)
	switch typ {
	case protocol.ColumnTypeTiny, protocol.ColumnTypeShort, protocol.ColumnTypeLong, protocol.ColumnTypeInt24, protocol.ColumnTypeLongLong:
		if val, err := strconv.ParseInt(s, 10, 64); err == nil {
			return val
		}
	case protocol.ColumnTypeFloat, protocol.ColumnTypeDouble:
		if val, err := strconv.ParseFloat(s, 64); err == nil {
			return val
		}
	case protocol.ColumnTypeDate, protocol.ColumnTypeDateTime, protocol.ColumnTypeTimestamp:
		formats := []string{
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, f := range formats {
			if t, err := time.ParseInLocation(f, s, time.UTC); err == nil {
				return t
			}
		}
	}
	return s
}

func databaseTypeName(typ byte) string {
	switch typ {
	case protocol.ColumnTypeDecimal:
		return "DECIMAL"
	case protocol.ColumnTypeTiny:
		return "TINYINT"
	case protocol.ColumnTypeShort:
		return "SMALLINT"
	case protocol.ColumnTypeLong:
		return "INT"
	case protocol.ColumnTypeFloat:
		return "FLOAT"
	case protocol.ColumnTypeDouble:
		return "DOUBLE"
	case protocol.ColumnTypeNull:
		return "NULL"
	case protocol.ColumnTypeTimestamp:
		return "TIMESTAMP"
	case protocol.ColumnTypeLongLong:
		return "BIGINT"
	case protocol.ColumnTypeInt24:
		return "MEDIUMINT"
	case protocol.ColumnTypeDate:
		return "DATE"
	case protocol.ColumnTypeTime:
		return "TIME"
	case protocol.ColumnTypeDateTime:
		return "DATETIME"
	case protocol.ColumnTypeYear:
		return "YEAR"
	case protocol.ColumnTypeVarChar:
		return "VARCHAR"
	case protocol.ColumnTypeBit:
		return "BIT"
	case protocol.ColumnTypeJSON:
		return "JSON"
	case protocol.ColumnTypeNewDecimal:
		return "NEWDECIMAL"
	case protocol.ColumnTypeEnum:
		return "ENUM"
	case protocol.ColumnTypeSet:
		return "SET"
	case protocol.ColumnTypeTinyBlob:
		return "TINYBLOB"
	case protocol.ColumnTypeMediumBlob:
		return "MEDIUMBLOB"
	case protocol.ColumnTypeLongBlob:
		return "LONGBLOB"
	case protocol.ColumnTypeBlob:
		return "BLOB"
	case protocol.ColumnTypeVarString:
		return "VAR_STRING"
	case protocol.ColumnTypeString:
		return "STRING"
	case protocol.ColumnTypeGeometry:
		return "GEOMETRY"
	default:
		return fmt.Sprintf("TYPE_%02X", typ)
	}
}

func scanType(typ byte) reflect.Type {
	switch typ {
	case protocol.ColumnTypeTiny, protocol.ColumnTypeShort, protocol.ColumnTypeLong, protocol.ColumnTypeInt24, protocol.ColumnTypeLongLong:
		return reflect.TypeOf(int64(0))
	case protocol.ColumnTypeFloat, protocol.ColumnTypeDouble:
		return reflect.TypeOf(float64(0))
	case protocol.ColumnTypeDate, protocol.ColumnTypeDateTime, protocol.ColumnTypeTimestamp:
		return reflect.TypeOf(time.Time{})
	case protocol.ColumnTypeTinyBlob,
		protocol.ColumnTypeMediumBlob,
		protocol.ColumnTypeLongBlob,
		protocol.ColumnTypeBlob,
		protocol.ColumnTypeGeometry:
		return reflect.TypeOf([]byte{})
	default:
		return reflect.TypeOf("")
	}
}
