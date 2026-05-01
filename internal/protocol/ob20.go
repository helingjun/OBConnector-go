package protocol

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	OB20MagicNum uint16 = 0x20AB
	OB20Version  uint8  = 1
	OB20HeaderLen uint8 = 24
)

const (
	OB20FlagNone     uint32 = 0
	OB20FlagSSL      uint32 = 1 << 0
	OB20FlagCompress uint32 = 1 << 1
)

type OB20Header struct {
	MagicNum     uint16
	Version      uint8
	ConnectionID uint32
	RequestID    uint32
	PacketSeq    uint8
	PayloadLen   uint32
	Flag         uint32
	Reserved     uint16
	HeaderCRC    uint16
}

func (h *OB20Header) Encode(buf []byte) {
	binary.BigEndian.PutUint16(buf[0:2], h.MagicNum)
	buf[2] = h.Version
	binary.BigEndian.PutUint32(buf[3:7], h.ConnectionID)
	binary.BigEndian.PutUint32(buf[7:11], h.RequestID)
	buf[11] = h.PacketSeq
	// PayloadLen is 4 bytes? Or 3 bytes + something? 
	// The search said 24 bytes total. Let's re-verify the offsets.
	// 0-1: Magic (2)
	// 2: Version (1)
	// 3-6: ConnID (4)
	// 7-10: RequestID (4)
	// 11: Seq (1)
	// 12-15: PayloadLen (4)
	// 16-19: Flag (4)
	// 20-21: Reserved (2)
	// 22-23: Checksum (2)
	binary.BigEndian.PutUint32(buf[12:16], h.PayloadLen)
	binary.BigEndian.PutUint32(buf[16:20], h.Flag)
	binary.BigEndian.PutUint16(buf[20:22], h.Reserved)
	h.HeaderCRC = CRC16(buf[0:22])
	binary.BigEndian.PutUint16(buf[22:24], h.HeaderCRC)
}

func (h *OB20Header) Decode(buf []byte) bool {
	if len(buf) < int(OB20HeaderLen) {
		return false
	}
	h.MagicNum = binary.BigEndian.Uint16(buf[0:2])
	if h.MagicNum != OB20MagicNum {
		return false
	}
	h.Version = buf[2]
	h.ConnectionID = binary.BigEndian.Uint32(buf[3:7])
	h.RequestID = binary.BigEndian.Uint32(buf[7:11])
	h.PacketSeq = buf[11]
	h.PayloadLen = binary.BigEndian.Uint32(buf[12:16])
	h.Flag = binary.BigEndian.Uint32(buf[16:20])
	h.Reserved = binary.BigEndian.Uint16(buf[20:22])
	h.HeaderCRC = binary.BigEndian.Uint16(buf[22:24])
	return h.HeaderCRC == CRC16(buf[0:22])
}

// CRC16 CCITT (0x1021)
func CRC16(data []byte) uint16 {
	var crc uint16 = 0
	for _, b := range data {
		crc = (crc << 8) ^ crc16Table[byte(crc>>8)^b]
	}
	return crc
}

var crc16Table = func() [256]uint16 {
	var table [256]uint16
	for i := 0; i < 256; i++ {
		var crc uint16 = uint16(i) << 8
		for j := 0; j < 8; j++ {
			if (crc & 0x8000) != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		table[i] = crc
	}
	return table
}()

func OB20PayloadChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
