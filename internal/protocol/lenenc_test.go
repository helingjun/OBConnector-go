package protocol

import (
	"io"
	"testing"
)

func TestReadLengthEncodedInt(t *testing.T) {
	tests := []struct {
		name  string
		data  []byte
		value uint64
		used  int
		isNil bool
		err   bool
	}{
		{"empty", nil, 0, 0, false, true},
		{"zero", []byte{0x00}, 0, 1, false, false},
		{"one", []byte{0x01}, 1, 1, false, false},
		{"max_1byte", []byte{0xFA}, 250, 1, false, false},
		{"null", []byte{0xFB}, 0, 1, true, false},
		{"2byte", []byte{0xFC, 0x01, 0x02}, 0x0201, 3, false, false},
		{"3byte", []byte{0xFD, 0x01, 0x02, 0x03}, 0x030201, 4, false, false},
		{"8byte", []byte{0xFE, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 0x0807060504030201, 9, false, false},
		{"2byte_truncated", []byte{0xFC}, 0, 0, false, true},
		{"3byte_truncated", []byte{0xFD, 0x01}, 0, 0, false, true},
		{"8byte_truncated", []byte{0xFE, 0x01, 0x02, 0x03}, 0, 0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, used, isNil, err := ReadLengthEncodedInt(tt.data)
			if tt.err && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != tt.value {
				t.Errorf("value = %d, want %d", val, tt.value)
			}
			if used != tt.used {
				t.Errorf("used = %d, want %d", used, tt.used)
			}
			if isNil != tt.isNil {
				t.Errorf("isNil = %v, want %v", isNil, tt.isNil)
			}
		})
	}
}

func TestPutLengthEncodedInt(t *testing.T) {
	tests := []struct {
		name string
		val  uint64
		want []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"one", 1, []byte{0x01}},
		{"max_1byte", 250, []byte{0xFA}},
		{"2byte_min", 251, []byte{0xFC, 0xFB, 0x00}},
		{"2byte_max", 0xFFFF, []byte{0xFC, 0xFF, 0xFF}},
		{"3byte_min", 0x10000, []byte{0xFD, 0x00, 0x00, 0x01}},
		{"8byte", 0x0102030405060708, []byte{0xFE, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PutLengthEncodedInt(nil, tt.val)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("byte %d = 0x%02x, want 0x%02x", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestReadLengthEncodedString(t *testing.T) {
	tests := []struct {
		name  string
		data  []byte
		value string
		used  int
		isNil bool
		err   bool
	}{
		{"empty", nil, "", 0, false, true},
		{"null", []byte{0xFB}, "", 1, true, false},
		{"empty_string", []byte{0x00}, "", 1, false, false},
		{"short", []byte{0x03, 'a', 'b', 'c'}, "abc", 4, false, false},
		{"truncated", []byte{0x05, 'a'}, "", 0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, used, isNil, err := ReadLengthEncodedString(tt.data)
			if tt.err && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(val) != tt.value {
				t.Errorf("value = %q, want %q", string(val), tt.value)
			}
			if used != tt.used {
				t.Errorf("used = %d, want %d", used, tt.used)
			}
			if isNil != tt.isNil {
				t.Errorf("isNil = %v, want %v", isNil, tt.isNil)
			}
		})
	}
}

func TestPutLengthEncodedString(t *testing.T) {
	got := PutLengthEncodedString(nil, "hello")
	if len(got) != 6 || string(got[1:]) != "hello" || got[0] != 0x05 {
		t.Fatalf("PutLengthEncodedString = %v (len=%d)", got, len(got))
	}

	// Empty string
	got = PutLengthEncodedString(nil, "")
	if len(got) != 1 || got[0] != 0x00 {
		t.Fatalf("empty string encoded = %v", got)
	}

	// Long string (251+ bytes)
	longStr := string(make([]byte, 300))
	got = PutLengthEncodedString(nil, longStr)
	if got[0] != 0xFC {
		t.Fatalf("long string prefix = 0x%02x, want 0xFC", got[0])
	}
	if len(got) != 303 {
		t.Fatalf("long string encoded length = %d, want 303", len(got))
	}
}

func TestPutLengthEncodedIntEndToEnd(t *testing.T) {
	values := []uint64{0, 1, 250, 251, 0xFFFF, 0x10000, 0xFFFFFF, 0x1000000, 0xFFFFFFFFFFFFFF}
	for _, v := range values {
		encoded := PutLengthEncodedInt(nil, v)
		decoded, used, _, err := ReadLengthEncodedInt(encoded)
		if err != nil {
			t.Fatalf("roundtrip failed for %d: %v", v, err)
		}
		if decoded != v {
			t.Fatalf("roundtrip: got %d, want %d", decoded, v)
		}
		if used != len(encoded) {
			t.Fatalf("roundtrip: consumed %d, encoded %d", used, len(encoded))
		}
	}
}

func TestPutLengthEncodedStringEndToEnd(t *testing.T) {
	values := []string{"", "a", "hello", string(make([]byte, 255)), string(make([]byte, 70000))}
	for _, v := range values {
		encoded := PutLengthEncodedString(nil, v)
		decoded, used, _, err := ReadLengthEncodedString(encoded)
		if err != nil {
			t.Fatalf("roundtrip failed for len %d: %v", len(v), err)
		}
		if string(decoded) != v {
			t.Fatalf("roundtrip: got %q, want %q", string(decoded), v)
		}
		if used != len(encoded) {
			t.Fatalf("roundtrip: consumed %d, encoded %d", used, len(encoded))
		}
	}
}

func TestReadLengthEncodedIntEdgeCases(t *testing.T) {
	// io.EOF for truncated
	_, _, _, err := ReadLengthEncodedInt(nil)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}

	// Just the type byte but no content for multi-byte
	_, _, _, err = ReadLengthEncodedInt([]byte{0xFC})
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestReadLengthEncodedStringEdgeCases(t *testing.T) {
	// io.EOF for nil
	_, _, _, err := ReadLengthEncodedString(nil)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}
