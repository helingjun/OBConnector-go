package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

const maxPayloadLen = 1<<24 - 1

type PacketConn struct {
	rw  io.ReadWriter
	seq byte
}

func NewPacketConn(rw io.ReadWriter) *PacketConn {
	return &PacketConn{rw: rw}
}

func (c *PacketConn) ResetSequence() {
	c.seq = 0
}

func (c *PacketConn) ReadPacket() ([]byte, error) {
	var out []byte
	for {
		var header [4]byte
		if _, err := io.ReadFull(c.rw, header[:]); err != nil {
			return nil, err
		}

		payloadLen := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		gotSeq := header[3]
		if gotSeq != c.seq {
			return nil, fmt.Errorf("unexpected packet sequence: got %d, want %d", gotSeq, c.seq)
		}
		c.seq++

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(c.rw, payload); err != nil {
			return nil, err
		}
		out = append(out, payload...)
		if payloadLen < maxPayloadLen {
			return out, nil
		}
	}
}

func (c *PacketConn) WritePacket(payload []byte) error {
	for {
		chunkLen := len(payload)
		if chunkLen > maxPayloadLen {
			chunkLen = maxPayloadLen
		}

		var header [4]byte
		header[0] = byte(chunkLen)
		header[1] = byte(chunkLen >> 8)
		header[2] = byte(chunkLen >> 16)
		header[3] = c.seq
		c.seq++

		if _, err := c.rw.Write(header[:]); err != nil {
			return err
		}
		if _, err := c.rw.Write(payload[:chunkLen]); err != nil {
			return err
		}

		payload = payload[chunkLen:]
		if chunkLen < maxPayloadLen {
			return nil
		}
		if len(payload) == 0 {
			return c.writeEmptyContinuation()
		}
	}
}

func (c *PacketConn) writeEmptyContinuation() error {
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(c.seq)<<24)
	c.seq++
	_, err := c.rw.Write(header[:])
	return err
}
