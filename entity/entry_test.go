package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifyRecordCRC(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func([]byte)
		wantOK  bool
	}{
		{
			name:   "valid_roundtrip",
			mutate: func([]byte) {},
			wantOK: true,
		},
		{
			name: "flip_payload_byte",
			mutate: func(b []byte) {
				if len(b) > MetaSize {
					b[len(b)-1] ^= 0xFF
				}
			},
			wantOK: false,
		},
		{
			name: "flip_crc_byte",
			mutate: func(b []byte) {
				b[0] ^= 0xFF
			},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEntryWithData([]byte("k"), []byte("v"))
			buf := e.Encode()
			tt.mutate(buf)
			assert.Equal(t, tt.wantOK, VerifyRecordCRC(buf))
		})
	}
}

func TestNewTombstoneEntry_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{name: "short", key: []byte("k")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewTombstoneEntry(tt.key)
			require.Equal(t, uint8(DeleteFlag), e.Meta.Flag)
			require.Equal(t, uint32(len(tt.key)), e.Meta.KeySize)
			require.Equal(t, uint32(0), e.Meta.ValueSize)
			buf := e.Encode()
			require.True(t, VerifyRecordCRC(buf))
			require.Equal(t, int64(len(buf)), e.Size())
		})
	}
}
