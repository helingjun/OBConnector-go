package oceanbase

import (
	"context"
	"database/sql/driver"
	"testing"
)

func TestPrepareContext(t *testing.T) {
	conn := &Conn{}
	stmt, err := conn.PrepareContext(context.Background(), "select ?, '?' from dual")
	if err != nil {
		t.Fatal(err)
	}
	if stmt.NumInput() != 1 {
		t.Fatalf("NumInput = %d", stmt.NumInput())
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.(driver.StmtQueryContext).QueryContext(context.Background(), nil); err == nil {
		t.Fatal("query on closed statement should fail")
	}
}

func TestPrepareRejectsEmptyStatement(t *testing.T) {
	conn := &Conn{}
	if _, err := conn.PrepareContext(context.Background(), ""); err == nil {
		t.Fatal("empty statement should fail")
	}
}
