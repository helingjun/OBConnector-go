package oceanbase

import (
	"io"
	"net"
	"os"
	"testing"
)

func TestParseServerError(t *testing.T) {
	tests := []struct {
		name   string
		packet []byte
		check  func(t *testing.T, err error)
	}{
		{
			"standard_error",
			[]byte{0xff, 0xE8, 0x03, '#', '4', '2', 'S', '0', '2', 't', 'e', 's', 't'},
			func(t *testing.T, err error) {
				if err == nil {
					t.Fatal("expected error")
				}
				se, ok := err.(*ServerError)
				if !ok {
					t.Fatalf("expected *ServerError, got %T", err)
				}
				if se.Number != 1000 {
					t.Errorf("Number = %d, want 1000", se.Number)
				}
				if se.SQLState != "42S02" {
					t.Errorf("SQLState = %q, want 42S02", se.SQLState)
				}
				if se.Message != "test" {
					t.Errorf("Message = %q, want test", se.Message)
				}
			},
		},
		{
			"no_sqlstate",
			[]byte{0xff, 0x6E, 0x03, 'e', 'r', 'r', ' ', 'm', 's', 'g'},
			func(t *testing.T, err error) {
				se := err.(*ServerError)
				if se.Number != 878 {
					t.Errorf("Number = %d, want 878", se.Number)
				}
				if se.SQLState != "" {
					t.Errorf("SQLState = %q, want empty", se.SQLState)
				}
				if se.Message != "err msg" {
					t.Errorf("Message = %q, want 'err msg'", se.Message)
				}
			},
		},
		{
			"ora_format", // e.g. ORA-00942
			[]byte{0xff, 0xAE, 0x03, '#', '4', '2', 'S', '0', '2', 'O', 'R', 'A', '-', '0', '0', '9', '4', '2'},
			func(t *testing.T, err error) {
				se := err.(*ServerError)
				if se.Number != 942 {
					t.Errorf("Number = %d, want 942", se.Number)
				}
			},
		},
		{
			"malformed_short_packet",
			[]byte{0xff},
			func(t *testing.T, err error) {
				if err == nil {
					t.Fatal("expected error for short packet")
				}
			},
		},
		{
			"not_error_packet",
			[]byte{0x00, 0x01, 0x02, 'h', 'i'},
			func(t *testing.T, err error) {
				if err == nil {
					t.Fatal("expected error for non-0xff packet")
				}
			},
		},
		{
			"empty_packet",
			nil,
			func(t *testing.T, err error) {
				if err == nil {
					t.Fatal("expected error for nil packet")
				}
			},
		},
		{
			"sqlstate_without_hash",
			[]byte{0xff, 0x01, 0x00, 'A', 'B', 'C', 'D', 'E'},
			func(t *testing.T, err error) {
				se := err.(*ServerError)
				if se.SQLState != "" {
					t.Errorf("SQLState = %q, want empty (no # prefix)", se.SQLState)
				}
			},
		},
		{
			"message_with_extra_spaces",
			[]byte{0xff, 0x04, 0x00, '#', '4', '2', 'S', '0', '2', ' ', 'm', 's', 'g'},
			func(t *testing.T, err error) {
				se := err.(*ServerError)
				if se.Message != " msg" {
					t.Errorf("Message = %q, want ' msg'", se.Message)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseServerError(tt.packet)
			tt.check(t, err)
		})
	}
}

func TestServerErrorFormat(t *testing.T) {
	e1 := &ServerError{Number: 942, Message: "table does not exist"}
	msg1 := e1.Error()
	if msg1 != "oceanbase: error 942: table does not exist" {
		t.Errorf("got %q", msg1)
	}

	e2 := &ServerError{Number: 1062, SQLState: "23000", Message: "Duplicate entry"}
	msg2 := e2.Error()
	if msg2 != "oceanbase: error 1062 (23000): Duplicate entry" {
		t.Errorf("got %q", msg2)
	}
}

func TestIsBadConnError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"io.EOF", io.EOF, true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, true},
		{"net.ErrClosed", net.ErrClosed, true},
		{"os.ErrDeadlineExceeded", os.ErrDeadlineExceeded, true},
		{"timeout", &testNetErr{timeout: true}, true},
		{"temporary", &testNetErr{timeout: false, temporary: true}, true},
		{"other", &testNetErr{timeout: false, temporary: false}, true}, // Any net.Error is bad
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isBadConnError(tt.err)
			if got != tt.want {
				t.Errorf("isBadConnError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

type testNetErr struct {
	timeout   bool
	temporary bool
}

func (e *testNetErr) Error() string   { return "net error" }
func (e *testNetErr) Timeout() bool   { return e.timeout }
func (e *testNetErr) Temporary() bool { return e.temporary }
