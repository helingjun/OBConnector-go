package protocol

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	OB20MagicNum uint16 = 0x20AB
	OB20Version  uint16 = 20
	OB20HeaderLen uint8 = 24
)

const (
	OB20FlagNone      uint32 = 0
	OB20FlagExtraInfo uint32 = 1 << 0
	OB20FlagSSL       uint32 = 1 << 1
	OB20FlagCompress  uint32 = 1 << 2
)

type OB20Header struct {
	MagicNum     uint16
	Version      uint16
	ConnectionID uint32
	RequestID    uint32
	PacketSeq    uint8
	PayloadLen   uint32
	Flag         uint32
	Reserved     uint8
	HeaderCRC    uint16
}

func (h *OB20Header) Encode(buf []byte) {
	binary.BigEndian.PutUint16(buf[0:2], h.MagicNum)
	binary.BigEndian.PutUint16(buf[2:4], h.Version)
	binary.BigEndian.PutUint32(buf[4:8], h.ConnectionID)
	binary.BigEndian.PutUint32(buf[8:12], h.RequestID)
	buf[12] = h.PacketSeq
	// PayloadLen (4)
	binary.BigEndian.PutUint32(buf[13:17], h.PayloadLen)
	// Flag (4)
	binary.BigEndian.PutUint32(buf[17:21], h.Flag)
	// Reserved (1)
	buf[21] = h.Reserved
	// HeaderCRC (2) at 22-23
	h.HeaderCRC = CRC16(buf[0:22])
	binary.BigEndian.PutUint16(buf[22:24], h.HeaderCRC)
}

func (h *OB20Header) Decode(buf []byte) bool {
	if len(buf) < int(OB20HeaderLen) {
		return false
	}
	h.MagicNum = binary.BigEndian.Uint16(buf[0:2])
	h.Version = binary.BigEndian.Uint16(buf[2:4])
	h.ConnectionID = binary.BigEndian.Uint32(buf[4:8])
	h.RequestID = binary.BigEndian.Uint32(buf[8:12])
	h.PacketSeq = buf[12]
	h.PayloadLen = binary.BigEndian.Uint32(buf[13:17])
	h.Flag = binary.BigEndian.Uint32(buf[17:21])
	h.Reserved = buf[21]
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

type OB20ExtraInfo struct {
	Type uint16
	Data []byte
}

func (e *OB20ExtraInfo) Encode(buf []byte) int {
	binary.BigEndian.PutUint16(buf[0:2], e.Type)
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(e.Data)))
	copy(buf[6:], e.Data)
	return 6 + len(e.Data)
}

func (e *OB20ExtraInfo) TotalLen() int {
	return 6 + len(e.Data)
}

func ParseOB20ExtraInfo(data []byte) ([]OB20ExtraInfo, error) {
	var infos []OB20ExtraInfo
	pos := 0
	for pos < len(data) {
		if len(data[pos:]) < 4 {
			break
		}
		typ := binary.BigEndian.Uint16(data[pos : pos+2])
		length := int(binary.BigEndian.Uint32(data[pos+2 : pos+6]))
		pos += 6
		if len(data[pos:]) < length {
			return nil, io.ErrUnexpectedEOF
		}
		infos = append(infos, OB20ExtraInfo{
			Type: typ,
			Data: data[pos : pos+length],
		})
		pos += length
	}
	return infos, nil
}

const (
	OB20ExtraInfoTypeTraceID     uint16 = 2001
	OB20ExtraInfoTypeSessVar     uint16 = 2002
	OB20ExtraInfoTypeFullTrace   uint16 = 2003
	OB20ExtraInfoTypeTableID     uint16 = 2004
	OB20ExtraInfoTypePartitionID uint16 = 2005 // Adjusted to follow JDBC pattern
)
