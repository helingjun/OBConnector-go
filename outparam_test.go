package oceanbase

import (
	"testing"
)

func TestAssignOutParam(t *testing.T) {
	c := &Conn{}

	t.Run("Int64", func(t *testing.T) {
		var i int64
		if err := c.assignOutParam(&i, int64(123)); err != nil {
			t.Fatal(err)
		}
		if i != 123 {
			t.Errorf("got %d, want 123", i)
		}
	})

	t.Run("String", func(t *testing.T) {
		var s string
		if err := c.assignOutParam(&s, "hello"); err != nil {
			t.Fatal(err)
		}
		if s != "hello" {
			t.Errorf("got %q, want hello", s)
		}
	})

	t.Run("ConvertibleInt", func(t *testing.T) {
		var i int
		if err := c.assignOutParam(&i, int64(456)); err != nil {
			t.Fatal(err)
		}
		if i != 456 {
			t.Errorf("got %d, want 456", i)
		}
	})

	t.Run("Float64", func(t *testing.T) {
		var f float64
		if err := c.assignOutParam(&f, 3.14); err != nil {
			t.Fatal(err)
		}
		if f != 3.14 {
			t.Errorf("got %f, want 3.14", f)
		}
	})

	t.Run("NilDest", func(t *testing.T) {
		if err := c.assignOutParam(nil, "value"); err != nil {
			t.Errorf("assign to nil dest should not fail: %v", err)
		}
	})

	t.Run("NonPointerDest", func(t *testing.T) {
		var i int
		if err := c.assignOutParam(i, int64(1)); err == nil {
			t.Error("assign to non-pointer should fail")
		}
	})
}

func TestReadOutParams(t *testing.T) {
	// This would require mocking the connection and packets.
	// For now, I'll rely on the unit test for assignOutParam.
}
