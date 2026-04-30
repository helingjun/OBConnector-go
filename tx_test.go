package oceanbase

import (
	"context"
	"database/sql/driver"
	"testing"
)

func TestBeginTxMarksTransactionActive(t *testing.T) {
	conn := &Conn{}
	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if tx == nil {
		t.Fatal("tx is nil")
	}
	if !conn.inTx {
		t.Fatal("connection should be marked in transaction")
	}
	if _, err := conn.BeginTx(context.Background(), driver.TxOptions{}); err == nil {
		t.Fatal("second transaction should fail")
	}
}

func TestBeginTxRejectsUnsupportedOptions(t *testing.T) {
	conn := &Conn{}
	if _, err := conn.BeginTx(context.Background(), driver.TxOptions{ReadOnly: true}); err == nil {
		t.Fatal("read-only transaction should fail")
	}
	if _, err := conn.BeginTx(context.Background(), driver.TxOptions{Isolation: driver.IsolationLevel(1)}); err == nil {
		t.Fatal("custom isolation transaction should fail")
	}
}
