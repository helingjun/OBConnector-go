package protocol

import "crypto/sha1"

func NativePasswordAuth(password string, seed []byte) []byte {
	if password == "" {
		return nil
	}

	hash1 := sha1.Sum([]byte(password))
	hash2 := sha1.Sum(hash1[:])

	h := sha1.New()
	_, _ = h.Write(seed)
	_, _ = h.Write(hash2[:])
	hash3 := h.Sum(nil)

	out := make([]byte, len(hash1))
	for i := range out {
		out[i] = hash1[i] ^ hash3[i]
	}
	return out
}
