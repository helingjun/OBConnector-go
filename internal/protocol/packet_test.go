package protocol

import (
	"bytes"
	"io"
	"testing"
)

type mockRW struct {
	*bytes.Buffer
}

func newMockRW() *mockRW {
	return &mockRW{Buffer: new(bytes.Buffer)}
}

func (m *mockRW) Read(p []byte) (int, error) {
	return m.Buffer.Read(p)
}

func (m *mockRW) Write(p []byte) (int, error) {
	return m.Buffer.Write(p)
}

// writeAndRead writes a packet (with ResetSequence before the write, and again
// before the read to simulate a fresh server response), then reads back.
func writeAndRead(pc *PacketConn, data []byte) ([]byte, error) {
	pc.ResetSequence()
	if err := pc.WritePacket(data); err != nil {
		return nil, err
	}
	pc.ResetSequence()
	return pc.ReadPacket()
}

func TestBasicReadWritePacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	data, err := writeAndRead(pc, []byte("hello"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("got %q, want %q", string(data), "hello")
	}
}

func TestPacketSequenceMismatch(t *testing.T) {
	pc := NewPacketConn(newMockRW())

	// Manually write a packet with wrong sequence number
	header := []byte{0x04, 0x00, 0x00, 0xFF} // len=4, seq=255
	pc.rw.Write(header)
	pc.rw.Write([]byte("test"))

	_, err := pc.ReadPacket()
	if err == nil {
		t.Fatal("expected sequence mismatch error")
	}
}

func TestPacketSequenceResetBeforeWrite(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	// Write seq=0, reset, write seq=0 again — buffer has two seq=0 packets
	_ = pc.WritePacket([]byte("pkt1"))
	pc.ResetSequence()
	_ = pc.WritePacket([]byte("pkt2"))

	// Reset sequence before read so c.seq matches the seq=0 in the packets
	pc.ResetSequence()

	// ReadPacket picks "pkt1" (first in buffer) since both have seq=0
	data, err := pc.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "pkt1" {
		t.Fatalf("got %q, want %q", string(data), "pkt1")
	}

	// Reading again gives "pkt2" — seq after first read is 1, but
	// pkt2 also has seq=0, so we need another reset.
	// This is expected: ResetSequence is used between requests.
	pc.ResetSequence()
	data, err = pc.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "pkt2" {
		t.Fatalf("got %q, want %q", string(data), "pkt2")
	}
}

func TestOB20ReadWritePacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)
	pc.EnableOB20(1, OB20MagicNum)
	pc.NextRequest()

	data, err := writeAndRead(pc, []byte("ob20 payload"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if string(data) != "ob20 payload" {
		t.Fatalf("got %q, want %q", string(data), "ob20 payload")
	}
}

func TestOB20ExtraInfoPacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)
	pc.EnableOB20(1, OB20MagicNum)
	pc.NextRequest()

	pc.AddExtraInfo(OB20ExtraInfoTypePartitionID, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
	data, err := writeAndRead(pc, []byte("data with partition info"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if string(data) != "data with partition info" {
		t.Fatalf("got %q, want %q", string(data), "data with partition info")
	}
}

func TestLargePacketSplit(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	largeData := make([]byte, maxPayloadLen+100)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	data, err := writeAndRead(pc, largeData)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(data) != len(largeData) {
		t.Fatalf("len = %d, want %d", len(data), len(largeData))
	}
	for i := range largeData {
		if data[i] != largeData[i] {
			t.Fatalf("byte %d differs: got 0x%02x, want 0x%02x", i, data[i], largeData[i])
		}
	}
}

func TestOB20LargePacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)
	pc.EnableOB20(1, OB20MagicNum)
	pc.NextRequest()

	largeData := make([]byte, maxPayloadLen+100)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	data, err := writeAndRead(pc, largeData)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(data) != len(largeData) {
		t.Fatalf("len = %d, want %d", len(data), len(largeData))
	}
	for i := range largeData {
		if data[i] != largeData[i] {
			t.Fatalf("byte %d differs: got 0x%02x, want 0x%02x", i, data[i], largeData[i])
		}
	}
}

func TestReadPacketTruncated(t *testing.T) {
	mock := newMockRW()
	// Write only 2 bytes of the 4-byte header
	mock.Write([]byte{0x05, 0x00})

	pc := NewPacketConn(mock)
	_, err := pc.ReadPacket()
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestMultipleSequentialWriteAndRead(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	for i := 0; i < 10; i++ {
		data, err := writeAndRead(pc, []byte{byte('A' + i)})
		if err != nil {
			t.Fatalf("iteration %d error: %v", i, err)
		}
		if len(data) != 1 || data[0] != byte('A'+i) {
			t.Fatalf("iteration %d: got %v, want %c", i, data, 'A'+i)
		}
	}
}

func TestReadPacketExactMaxPayload(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	data := make([]byte, maxPayloadLen)
	for i := range data {
		data[i] = byte(i)
	}

	read, err := writeAndRead(pc, data)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(read) != maxPayloadLen {
		t.Fatalf("len = %d, want %d", len(read), maxPayloadLen)
	}
}

func TestClearExtraInfo(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)
	pc.EnableOB20(1, OB20MagicNum)
	pc.NextRequest()

	pc.AddExtraInfo(OB20ExtraInfoTypePartitionID, []byte{0x01})
	pc.ClearExtraInfo()
	pc.AddExtraInfo(OB20ExtraInfoTypePartitionID, []byte{0x02})

	data, err := writeAndRead(pc, []byte("test"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if string(data) != "test" {
		t.Fatalf("got %q, want %q", string(data), "test")
	}
}

func TestNextRequestIncrement(t *testing.T) {
	pc := NewPacketConn(newMockRW())
	pc.EnableOB20(1, OB20MagicNum)

	id1 := pc.requestID
	pc.NextRequest()
	if pc.requestID != id1+1 {
		t.Fatalf("requestID: %d -> %d, expected increment", id1, pc.requestID)
	}
}

func TestEmptyPacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	data, err := writeAndRead(pc, nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("len = %d, want 0", len(data))
	}
}

func TestPacketIdentity(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	payloads := [][]byte{
		{},
		{0x00},
		{0xFF, 0xFE, 0xFD},
		bytes.Repeat([]byte{0xAB}, 1000),
	}

	for idx, payload := range payloads {
		data, err := writeAndRead(pc, payload)
		if err != nil {
			t.Fatalf("payload %d error: %v", idx, err)
		}
		if !bytes.Equal(data, payload) {
			t.Fatalf("payload %d: length mismatch: got %d, want %d", idx, len(data), len(payload))
			t.Fatalf("payload %d: got %v, want %v", idx, data, payload)
		}
	}
}

