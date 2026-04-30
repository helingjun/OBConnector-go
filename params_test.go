package oceanbase

import (
	"database/sql/driver"
	"testing"
	"time"
)

func TestInterpolateParams(t *testing.T) {
	when := time.Date(2026, 4, 30, 11, 55, 1, 123456000, time.UTC)
	got, err := interpolateParams(
		"select ?, ?, ?, ?, ?, ? from dual",
		[]driver.NamedValue{
			{Ordinal: 1, Value: int64(42)},
			{Ordinal: 2, Value: "O'Reilly"},
			{Ordinal: 3, Value: nil},
			{Ordinal: 4, Value: true},
			{Ordinal: 5, Value: []byte{0x0a, 0xff}},
			{Ordinal: 6, Value: when},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := "select 42, 'O''Reilly', NULL, 1, hextoraw('0AFF'), timestamp '2026-04-30 11:55:01.123456' from dual"
	if got != want {
		t.Fatalf("query = %q, want %q", got, want)
	}
}

func TestInterpolateParamsSkipsQuotesAndComments(t *testing.T) {
	got, err := interpolateParams(
		"select '?' as q, \"?\" as ident, ? as v -- ? comment\nfrom dual /* ? block */",
		[]driver.NamedValue{{Ordinal: 1, Value: "ok"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := "select '?' as q, \"?\" as ident, 'ok' as v -- ? comment\nfrom dual /* ? block */"
	if got != want {
		t.Fatalf("query = %q, want %q", got, want)
	}
}

func TestInterpolateParamsArgumentCount(t *testing.T) {
	if _, err := interpolateParams("select ? from dual", nil); err == nil {
		t.Fatal("not enough args should fail")
	}
	if _, err := interpolateParams("select 1 from dual", []driver.NamedValue{{Ordinal: 1, Value: int64(1)}}); err == nil {
		t.Fatal("too many args should fail")
	}
}

func TestCountPlaceholders(t *testing.T) {
	query := "select ?, '?', \"?\" from dual -- ?\nwhere x = ? /* ? */"
	if got := countPlaceholders(query); got != 2 {
		t.Fatalf("countPlaceholders = %d, want 2", got)
	}
}

func TestLiteralRejectsNamedParams(t *testing.T) {
	if _, err := literal(driver.NamedValue{Name: "x", Value: int64(1)}); err == nil {
		t.Fatal("named params should fail")
	}
}
