package main

import (
	"strings"
	"testing"
)

func TestApplyExperimentParamsOpaqueDSN(t *testing.T) {
	dsn, err := applyExperimentParams("oceanbase:u:p@127.0.0.1:2883/db?TIMEOUT=5", true, "", "", "", "oboracle", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "TIMEOUT=5&") {
		t.Fatalf("original query not preserved: %s", dsn)
	}
	if !strings.Contains(dsn, "trace=true") {
		t.Fatalf("trace not appended: %s", dsn)
	}
	if !strings.Contains(dsn, "preset=oboracle") {
		t.Fatalf("preset not appended: %s", dsn)
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		value any
		want  string
	}{
		{nil, "NULL"},
		{[]byte("abc"), "abc"},
		{int64(42), "42"},
		{"hello", "hello"},
	}
	for _, tt := range tests {
		if got := formatValue(tt.value); got != tt.want {
			t.Fatalf("formatValue(%#v) = %q, want %q", tt.value, got, tt.want)
		}
	}
}

func TestSmokeTableName(t *testing.T) {
	generated, err := smokeTableName("")
	if err != nil {
		t.Fatal(err)
	}
	if !validIdentifier.MatchString(generated) {
		t.Fatalf("generated invalid table name %q", generated)
	}

	named, err := smokeTableName("my_table_1")
	if err != nil {
		t.Fatal(err)
	}
	if named != "MY_TABLE_1" {
		t.Fatalf("named table = %q", named)
	}

	if _, err := smokeTableName("1bad"); err == nil {
		t.Fatal("invalid table name should fail")
	}
}

func TestIsMissingTableError(t *testing.T) {
	if !isMissingTableError(errString("oceanbase: error 942 (42S02): ORA-00942: table does not exist")) {
		t.Fatal("ORA-00942 should be recognized")
	}
	if isMissingTableError(errString("some other error")) {
		t.Fatal("unrelated error should not be recognized")
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestResultValue(t *testing.T) {
	if got := resultValue(3, nil); got != "3" {
		t.Fatalf("resultValue = %q", got)
	}
	if got := resultValue(0, errString("unsupported")); got != "unknown" {
		t.Fatalf("resultValue error = %q", got)
	}
}
