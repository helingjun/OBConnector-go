package protocol

import (
	"testing"
)

func TestOB20Header(t *testing.T) {
	h := OB20Header{
		MagicNum:     OB20MagicNum,
		Version:      OB20Version,
		ConnectionID: 12345,
		RequestID:    67890,
		PacketSeq:    1,
		PayloadLen:   100,
		Flag:         OB20FlagNone,
	}

	var buf [OB20HeaderLen]byte
	h.Encode(buf[:])

	var h2 OB20Header
	if !h2.Decode(buf[:]) {
		t.Fatal("failed to decode OB 2.0 header")
	}

	if h.MagicNum != h2.MagicNum || h.Version != h2.Version || h.ConnectionID != h2.ConnectionID ||
		h.RequestID != h2.RequestID || h.PacketSeq != h2.PacketSeq || h.PayloadLen != h2.PayloadLen ||
		h.Flag != h2.Flag {
		t.Errorf("decoded header mismatch: %+v vs %+v", h, h2)
	}
}

func TestCRC16(t *testing.T) {
	data := []byte("hello world")
	crc := CRC16(data)
	if crc == 0 {
		t.Error("CRC16 should not be 0")
	}
}
