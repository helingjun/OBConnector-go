package protocol

const (
	ClientLongPassword               uint32 = 1 << 0
	ClientFoundRows                  uint32 = 1 << 1
	ClientLongFlag                   uint32 = 1 << 2
	ClientConnectWithDB              uint32 = 1 << 3
	ClientNoSchema                   uint32 = 1 << 4
	ClientCompress                   uint32 = 1 << 5
	ClientODBC                       uint32 = 1 << 6
	ClientLocalFiles                 uint32 = 1 << 7
	ClientIgnoreSpace                uint32 = 1 << 8
	ClientProtocol41                 uint32 = 1 << 9
	ClientInteractive                uint32 = 1 << 10
	ClientSSL                        uint32 = 1 << 11
	ClientIgnoreSigpipe              uint32 = 1 << 12
	ClientTransactions               uint32 = 1 << 13
	ClientReserved                   uint32 = 1 << 14
	ClientSecureConnection           uint32 = 1 << 15
	ClientMultiStatements            uint32 = 1 << 16
	ClientMultiResults               uint32 = 1 << 17
	ClientPSMultiResults             uint32 = 1 << 18
	ClientPluginAuth                 uint32 = 1 << 19
	ClientConnectAttrs               uint32 = 1 << 20
	ClientPluginAuthLenencClientData uint32 = 1 << 21
	ClientCanHandleExpiredPasswords  uint32 = 1 << 22
	ClientSessionTrack               uint32 = 1 << 23
	ClientDeprecateEOF               uint32 = 1 << 24
	ClientSupportOracleMode          uint32 = 1 << 27
	ClientReturnHiddenRowID          uint32 = 1 << 28
	ClientUseLOBLocator              uint32 = 1 << 29
	DefaultMaxPacketSize             uint32 = 1 << 24
	DefaultCollationUTF8MB4GeneralCI byte   = 45
	DefaultCollationUTF8MB4Bin       byte   = 46
	ComQuit                          byte   = 0x01
	ComQuery                         byte   = 0x03
	ComStmtPrepare                   byte   = 0x16
	ComStmtExecute                   byte   = 0x17
	ComStmtClose                     byte   = 0x19
	ComStmtReset                     byte   = 0x1a
	OKPacket                         byte   = 0x00
	ErrPacket                        byte   = 0xff
	EOFPacket                        byte   = 0xfe
	NullColumn                       byte   = 0xfb
)

const (
	ServerSessionStateChanged uint16 = 0x4000
)

const (
	ColumnTypeDecimal    byte = 0x00
	ColumnTypeTiny       byte = 0x01
	ColumnTypeShort      byte = 0x02
	ColumnTypeLong       byte = 0x03
	ColumnTypeFloat      byte = 0x04
	ColumnTypeDouble     byte = 0x05
	ColumnTypeNull       byte = 0x06
	ColumnTypeTimestamp  byte = 0x07
	ColumnTypeLongLong   byte = 0x08
	ColumnTypeInt24      byte = 0x09
	ColumnTypeDate       byte = 0x0a
	ColumnTypeTime       byte = 0x0b
	ColumnTypeDateTime   byte = 0x0c
	ColumnTypeYear       byte = 0x0d
	ColumnTypeVarChar    byte = 0x0f
	ColumnTypeBit        byte = 0x10
	ColumnTypeJSON       byte = 0xf5
	ColumnTypeNewDecimal byte = 0xf6
	ColumnTypeEnum       byte = 0xf7
	ColumnTypeSet        byte = 0xf8
	ColumnTypeTinyBlob   byte = 0xf9
	ColumnTypeMediumBlob byte = 0xfa
	ColumnTypeLongBlob   byte = 0xfb
	ColumnTypeBlob       byte = 0xfc
	ColumnTypeVarString  byte = 0xfd
	ColumnTypeString     byte = 0xfe
	ColumnTypeGeometry   byte = 0xff
)
