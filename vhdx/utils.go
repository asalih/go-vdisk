package vhdx

import (
	"github.com/google/uuid"
)

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func divmod(numerator, denominator int64) (quotient, remainder int64) {
	return numerator / denominator, numerator % denominator
}

func min32(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func newUUIDFromBytesLE(bytesLe []byte) uuid.UUID {
	var uuid uuid.UUID
	copy(uuid[0:4], reverseBytes(bytesLe[0:4]))
	copy(uuid[4:6], reverseBytes(bytesLe[4:6]))
	copy(uuid[6:8], reverseBytes(bytesLe[6:8]))
	copy(uuid[8:16], bytesLe[8:16])
	return uuid
}

// reverseBytes reverses a slice of bytes.
func reverseBytes(b []byte) []byte {
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	return b
}
