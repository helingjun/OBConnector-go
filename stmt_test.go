package oceanbase

import (
	"bytes"
	"testing"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

func TestStmtNumInput(t *testing.T) {
	stmt := &Stmt{paramCount: 1}
	if stmt.NumInput() != 1 {
		t.Fatalf("NumInput = %d", stmt.NumInput())
	}
}

func TestStmtClose(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{packets: protocol.NewPacketConn(&buf)} // closed/bad check will return nil
	stmt := &Stmt{conn: conn, closed: false}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if !stmt.closed {
		t.Fatal("stmt should be closed")
	}
}
