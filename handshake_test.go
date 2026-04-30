package oceanbase

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

func TestHandshakeResponseIncludesOceanBaseOracleExtensions(t *testing.T) {
	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "sys@tenant#cluster",
		Password:   "test-password",
		Timeout:    time.Second,
		Attributes: map[string]string{},
	}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	conn := &Conn{cfg: cfg}
	hs := &handshake{
		serverVersion: "5.6.25",
		connectionID:  42,
		capabilities: protocol.ClientLongPassword |
			protocol.ClientLongFlag |
			protocol.ClientProtocol41 |
			protocol.ClientTransactions |
			protocol.ClientSecureConnection |
			protocol.ClientMultiResults |
			protocol.ClientPluginAuth |
			protocol.ClientConnectAttrs |
			protocol.ClientSessionTrack,
		authPlugin: "mysql_native_password",
		authSeed:   []byte("12345678901234567890"),
	}

	response := conn.buildHandshakeResponse(hs)
	caps := binary.LittleEndian.Uint32(response[:4])
	if caps&protocol.ClientSupportOracleMode == 0 {
		t.Fatalf("CLIENT_SUPPORT_ORACLE_MODE missing from %#x", caps)
	}
	if caps&protocol.ClientSessionTrack == 0 {
		t.Fatalf("CLIENT_SESSION_TRACK missing from %#x", caps)
	}

	requiredAttrs := [][]byte{
		[]byte("__mysql_client_type"),
		[]byte("__ob_libobclient"),
		[]byte("__ob_client_name"),
		[]byte("OceanBase Connector/C"),
		[]byte("__proxy_capability_flag"),
		[]byte("__ob_client_attribute_capability_flag"),
	}
	for _, attr := range requiredAttrs {
		if !bytes.Contains(response, attr) {
			t.Fatalf("handshake response missing attr fragment %q", attr)
		}
	}
}
