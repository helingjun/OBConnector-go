package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

const maxPayloadLen = 1<<24 - 1

var bufPool = sync.Pool{
	New: func() any {
		return make([]byte, 4096)
	},
}

func getBuf(size int) []byte {
	if size > 4096 {
		return make([]byte, size)
	}
	buf := bufPool.Get().([]byte)
	return buf[:size]
}

func putBuf(buf []byte) {
	if cap(buf) == 4096 {
		bufPool.Put(buf[:4096])
	}
}

type PacketConn struct {
	rw           io.ReadWriter
	seq          byte
	ob20         bool
	connectionID uint32
	requestID    uint32
}

func NewPacketConn(rw io.ReadWriter) *PacketConn {
	return &PacketConn{rw: rw}
}

func (c *PacketConn) ResetSequence() {
	c.seq = 0
}

func (c *PacketConn) EnableOB20(connectionID uint32) {
	c.ob20 = true
	c.connectionID = connectionID
}

func (c *PacketConn) NextRequest() {
	c.requestID++
}

func (c *PacketConn) ReadPacket() ([]byte, error) {
	var out []byte
	for {
		if c.ob20 {
			var obHeader [OB20HeaderLen]byte
			if _, err := io.ReadFull(c.rw, obHeader[:]); err != nil {
				return nil, err
			}
			var h OB20Header
			if !h.Decode(obHeader[:]) {
				return nil, fmt.Errorf("invalid OB 2.0 header")
			}
			// In OB 2.0, the payload is the entire MySQL packet (header + payload)
			// We allocate the payload as before, as it's passed back to the user
			mysqlPacket := make([]byte, h.PayloadLen)
			if _, err := io.ReadFull(c.rw, mysqlPacket); err != nil {
				return nil, err
			}
			var obTrailer [4]byte
			if _, err := io.ReadFull(c.rw, obTrailer[:]); err != nil {
				return nil, err
			}
			expectedChecksum := binary.BigEndian.Uint32(obTrailer[:])
			if OB20PayloadChecksum(mysqlPacket) != expectedChecksum {
				return nil, fmt.Errorf("invalid OB 2.0 payload checksum")
			}

			// Extract the MySQL payload from the MySQL packet
			if len(mysqlPacket) < 4 {
				return nil, io.ErrUnexpectedEOF
			}
			payloadLen := int(mysqlPacket[0]) | int(mysqlPacket[1])<<8 | int(mysqlPacket[2])<<16
			gotSeq := mysqlPacket[3]
			if gotSeq != c.seq {
				return nil, fmt.Errorf("unexpected packet sequence: got %d, want %d", gotSeq, c.seq)
			}
			c.seq++
			out = append(out, mysqlPacket[4:]...)
			if payloadLen < maxPayloadLen {
				return out, nil
			}
		} else {
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
}

func (c *PacketConn) WritePacket(payload []byte) error {
	for {
		chunkLen := len(payload)
		if chunkLen > maxPayloadLen {
			chunkLen = maxPayloadLen
		}

		// Use pooled buffer for the write payload
		writeBuf := getBuf(4 + chunkLen)
		writeBuf[0] = byte(chunkLen)
		writeBuf[1] = byte(chunkLen >> 8)
		writeBuf[2] = byte(chunkLen >> 16)
		writeBuf[3] = c.seq
		c.seq++
		copy(writeBuf[4:], payload[:chunkLen])

		if c.ob20 {
			var obHeaderBuf [OB20HeaderLen]byte
			h := OB20Header{
				MagicNum:     OB20MagicNum,
				Version:      OB20Version,
				ConnectionID: c.connectionID,
				RequestID:    c.requestID,
				PacketSeq:    writeBuf[3],
				PayloadLen:   uint32(len(writeBuf)),
			}
			h.Encode(obHeaderBuf[:])
			if _, err := c.rw.Write(obHeaderBuf[:]); err != nil {
				putBuf(writeBuf)
				return err
			}
			if _, err := c.rw.Write(writeBuf); err != nil {
				putBuf(writeBuf)
				return err
			}
			var obTrailer [4]byte
			binary.BigEndian.PutUint32(obTrailer[:], OB20PayloadChecksum(writeBuf))
			if _, err := c.rw.Write(obTrailer[:]); err != nil {
				putBuf(writeBuf)
				return err
			}
		} else {
			if _, err := c.rw.Write(writeBuf); err != nil {
				putBuf(writeBuf)
				return err
			}
		}
		putBuf(writeBuf)

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
	mysqlHeader := getBuf(4)
	defer putBuf(mysqlHeader)
	mysqlHeader[0] = 0
	mysqlHeader[1] = 0
	mysqlHeader[2] = 0
	mysqlHeader[3] = c.seq
	c.seq++

	if c.ob20 {
		var obHeaderBuf [OB20HeaderLen]byte
		h := OB20Header{
			MagicNum:     OB20MagicNum,
			Version:      OB20Version,
			ConnectionID: c.connectionID,
			RequestID:    c.requestID,
			PacketSeq:    mysqlHeader[3],
			PayloadLen:   uint32(len(mysqlHeader)),
		}
		h.Encode(obHeaderBuf[:])
		if _, err := c.rw.Write(obHeaderBuf[:]); err != nil {
			return err
		}
		if _, err := c.rw.Write(mysqlHeader); err != nil {
			return err
		}
		var obTrailer [4]byte
		binary.BigEndian.PutUint32(obTrailer[:], OB20PayloadChecksum(mysqlHeader))
		_, err := c.rw.Write(obTrailer[:])
		return err
	}

	_, err := c.rw.Write(mysqlHeader)
	return err
}
