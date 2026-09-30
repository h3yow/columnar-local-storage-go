package columnstore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"testing"
)

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

func u32Bytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func readU32(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }

func mustGob(t *testing.T, v any) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
