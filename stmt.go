package oceanbase

import (
	"context"
	"database/sql/driver"
	"errors"
)

type Stmt struct {
	conn   *Conn
	query  string
	closed bool
}

func (s *Stmt) Close() error {
	s.closed = true
	return nil
}

func (s *Stmt) NumInput() int {
	return countPlaceholders(s.query)
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamed(args))
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamed(args))
}

func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.closed {
		return nil, errors.New("oceanbase: statement is closed")
	}
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.closed {
		return nil, errors.New("oceanbase: statement is closed")
	}
	return s.conn.QueryContext(ctx, s.query, args)
}
