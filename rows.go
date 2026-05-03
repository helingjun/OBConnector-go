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
	const maxDrainRows = 10000
	for i := 0; i < maxDrainRows; i++ {
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
	r.conn.bad = true
	return fmt.Errorf("oceanbase: drain() exceeded %d rows, possible protocol desync", maxDrainRows)
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
		res, _, err := c.handleOK(packet)
		return res, err
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
	if len(packet) == 0 {
		return false
	}
	switch packet[0] {
	case protocol.ErrPacket:
		return false
	case protocol.OKPacket:
		// OK packet: 0x00 + affected_rows(lenenc) + last_insert_id(lenenc) + [status(2) + ...]
		// Try to parse affected_rows. If parsing succeeds, it's an OK packet (not a row).
		// A bare 0x00 is a valid minimal OK packet.
		_, used, _, err := protocol.ReadLengthEncodedInt(packet[1:])
		if err != nil {
			return false
		}
		// Try to parse last_insert_id — an OK packet always has this field.
		_, _, _, err = protocol.ReadLengthEncodedInt(packet[1+used:])
		return err == nil
	case protocol.EOFPacket:
		// EOF packet: 0xFE + status(2) + warnings(2) = 5 bytes minimum in MySQL 5.7+
		// Contextually, 0xFE always means EOF after the column-definition or row-data phase.
		return true
	}
	return false
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
		typ := protocol.ColumnTypeVarString
		if i < len(types) {
			typ = types[i]
		}
		row[i] = textValue(raw, typ)
	}
	return row, nil
}

func textValue(raw []byte, typ byte) driver.Value {
	s := string(raw)
	switch typ {
	case protocol.ColumnTypeTiny, protocol.ColumnTypeShort, protocol.ColumnTypeLong, protocol.ColumnTypeInt24:
		// Note: ColumnTypeLong is also ColumnTypeOracleNumber (3).
		// In Oracle mode, we prefer string to preserve precision.
		// However, without knowing the mode here, we might have a conflict.
		// For now, let's assume if it can be parsed as int64, it's fine,
		// but Oracle NUMBER often exceeds int64.
		if val, err := strconv.ParseInt(s, 10, 64); err == nil {
			return val
		}
		return s
	case protocol.ColumnTypeLongLong:
		if val, err := strconv.ParseInt(s, 10, 64); err == nil {
			return val
		}
		return s
	case protocol.ColumnTypeOracleNumberFloat:
		return s
	case protocol.ColumnTypeFloat, protocol.ColumnTypeDouble:
		// ColumnTypeFloat is also ColumnTypeOracleBinaryFloat (4)
		// ColumnTypeDouble is also ColumnTypeOracleBinaryDouble (5)
		if val, err := strconv.ParseFloat(s, 64); err == nil {
			return val
		}
	case protocol.ColumnTypeDecimal, protocol.ColumnTypeNewDecimal:
		// Always return string for Decimal to preserve precision
		return s
	case protocol.ColumnTypeDate, protocol.ColumnTypeDateTime, protocol.ColumnTypeTimestamp,
		protocol.ColumnTypeOracleTimestampNano, protocol.ColumnTypeOracleTimestampTZ, protocol.ColumnTypeOracleTimestampLTZ:
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
	case protocol.ColumnTypeTime:
		// TIME as string (e.g. "HH:MM:SS" or "HH:MM:SS.ffffff")
		return s
	case protocol.ColumnTypeYear:
		if val, err := strconv.ParseInt(s, 10, 64); err == nil {
			return val
		}
		return s
	case protocol.ColumnTypeBit:
		return raw
	case protocol.ColumnTypeTinyBlob, protocol.ColumnTypeMediumBlob,
		protocol.ColumnTypeLongBlob, protocol.ColumnTypeBlob,
		protocol.ColumnTypeOracleRaw, protocol.ColumnTypeOracleBlob, protocol.ColumnTypeOracleClob:
		return raw
	case protocol.ColumnTypeOracleRowID, protocol.ColumnTypeOracleIntervalYM,
		protocol.ColumnTypeOracleIntervalDS, protocol.ColumnTypeOracleNVarChar2, protocol.ColumnTypeOracleNChar:
		return s
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
		// Also ColumnTypeOracleNumber. Usually INT in MySQL.
		return "INT"
	case protocol.ColumnTypeFloat:
		// Also ColumnTypeOracleBinaryFloat
		return "FLOAT"
	case protocol.ColumnTypeDouble:
		// Also ColumnTypeOracleBinaryDouble
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
	case protocol.ColumnTypeOracleTimestampTZ:
		return "TIMESTAMP WITH TIME ZONE"
	case protocol.ColumnTypeOracleTimestampLTZ:
		return "TIMESTAMP WITH LOCAL TIME ZONE"
	case protocol.ColumnTypeOracleRaw:
		return "RAW"
	case protocol.ColumnTypeOracleRowID:
		return "ROWID"
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
	case protocol.ColumnTypeDecimal, protocol.ColumnTypeNewDecimal:
		return reflect.TypeOf("")
	case protocol.ColumnTypeDate, protocol.ColumnTypeDateTime, protocol.ColumnTypeTimestamp:
		return reflect.TypeOf(time.Time{})
	case protocol.ColumnTypeTinyBlob,
		protocol.ColumnTypeMediumBlob,
		protocol.ColumnTypeLongBlob,
		protocol.ColumnTypeBlob,
		protocol.ColumnTypeGeometry,
		protocol.ColumnTypeOracleRaw,
		protocol.ColumnTypeOracleBlob,
		protocol.ColumnTypeOracleClob:
		return reflect.TypeOf([]byte{})
	default:
		return reflect.TypeOf("")
	}
}
