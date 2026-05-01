package protocol

import (
	"reflect"
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
		Reserved:     0,
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

func TestOB20ExtraInfo(t *testing.T) {
	info := OB20ExtraInfo{
		Type: OB20ExtraInfoTypePartitionID,
		Data: []byte{0x01, 0x02, 0x03, 0x04},
	}
	buf := make([]byte, info.TotalLen())
	n := info.Encode(buf)
	if n != 10 {
		t.Errorf("expected length 10, got %d", n)
	}

	infos, err := ParseOB20ExtraInfo(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Type != info.Type || !reflect.DeepEqual(infos[0].Data, info.Data) {
		t.Errorf("decoded info mismatch: %+v", infos)
	}
}

func TestCRC16(t *testing.T) {
	data := []byte("hello world")
	crc := CRC16(data)
	if crc == 0 {
		t.Error("CRC16 should not be 0")
	}
}
