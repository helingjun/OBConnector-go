package protocol

import (
	"bytes"
	"testing"
)

func TestNativePasswordAuth(t *testing.T) {
	// Empty password -> nil
	if got := NativePasswordAuth("", []byte("seed1234567890")); got != nil {
		t.Fatal("empty password should return nil")
	}

	// Deterministic output
	seed := []byte("abcdefghijklmnopqrst")
	r1 := NativePasswordAuth("password", seed)
	r2 := NativePasswordAuth("password", seed)
	if !bytes.Equal(r1, r2) {
		t.Fatal("same inputs should produce same output")
	}

	// Different seed -> different output
	seed2 := []byte("bcdefghijklmnopqrstu")
	r3 := NativePasswordAuth("password", seed2)
	if bytes.Equal(r1, r3) {
		t.Fatal("different seeds should produce different output")
	}

	// Different password -> different output
	r4 := NativePasswordAuth("otherpass", seed)
	if bytes.Equal(r1, r4) {
		t.Fatal("different passwords should produce different output")
	}

	// Output length should be 20 (SHA1 length)
	if len(r1) != 20 {
		t.Fatalf("output length = %d, want 20", len(r1))
	}
}

func TestNativePasswordAuthZeroSeed(t *testing.T) {
	// Even with zero seed, should produce deterministic 20-byte output
	seed := make([]byte, 20)
	result := NativePasswordAuth("test", seed)
	if len(result) != 20 {
		t.Fatalf("output length = %d, want 20", len(result))
	}
}

func TestNativePasswordAuthShortSeed(t *testing.T) {
	// Short seed should not panic
	seed := []byte("short")
	result := NativePasswordAuth("test", seed)
	if len(result) != 20 {
		t.Fatalf("output length = %d, want 20", len(result))
	}
}

func TestCachingSha2PasswordAuth(t *testing.T) {
	// Empty password -> nil
	if got := CachingSha2PasswordAuth("", []byte("seed1234567890")); got != nil {
		t.Fatal("empty password should return nil")
	}

	// Deterministic output
	seed := []byte("abcdefghijklmnopqrst")
	r1 := CachingSha2PasswordAuth("password", seed)
	r2 := CachingSha2PasswordAuth("password", seed)
	if !bytes.Equal(r1, r2) {
		t.Fatal("same inputs should produce same output")
	}

	// Output length should be 32 (SHA256 length)
	if len(r1) != 32 {
		t.Fatalf("output length = %d, want 32", len(r1))
	}
}

func TestBothAuthConsistency(t *testing.T) {
	// Both algorithms should produce non-empty results for the same input
	seed := []byte("abcdefghijklmnopqrst")
	native := NativePasswordAuth("mypassword", seed)
	sha2 := CachingSha2PasswordAuth("mypassword", seed)
	if native == nil || sha2 == nil {
		t.Fatal("both should produce non-nil results")
	}
	if bytes.Equal(native, sha2) {
		t.Fatal("native and sha2 should produce different results")
	}
}
