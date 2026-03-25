package tstorage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_blobEncoder_encodePoint_decodePoint(t *testing.T) {
	tests := []struct {
		name  string
		input []*DataPoint
		want  []*DataPoint
	}{
		{
			name: "single blob point",
			input: []*DataPoint{
				{Timestamp: 1600000000, Payload: []byte("hello world")},
			},
			want: []*DataPoint{
				{Timestamp: 1600000000, Payload: []byte("hello world")},
			},
		},
		{
			name: "multiple blob points",
			input: []*DataPoint{
				{Timestamp: 1600000000, Payload: []byte{0x01, 0x02, 0x03}},
				{Timestamp: 1600000060, Payload: []byte{0xAA, 0xBB}},
				{Timestamp: 1600000120, Payload: []byte("a longer payload with more bytes")},
			},
			want: []*DataPoint{
				{Timestamp: 1600000000, Payload: []byte{0x01, 0x02, 0x03}},
				{Timestamp: 1600000060, Payload: []byte{0xAA, 0xBB}},
				{Timestamp: 1600000120, Payload: []byte("a longer payload with more bytes")},
			},
		},
		{
			name: "empty payload",
			input: []*DataPoint{
				{Timestamp: 1600000000, Payload: []byte{}},
			},
			want: []*DataPoint{
				{Timestamp: 1600000000, Payload: []byte{}},
			},
		},
		{
			name: "large binary payload",
			input: func() []*DataPoint {
				payload := make([]byte, 4096)
				for i := range payload {
					payload[i] = byte(i % 256)
				}
				return []*DataPoint{
					{Timestamp: 1600000000, Payload: payload},
				}
			}(),
			want: func() []*DataPoint {
				payload := make([]byte, 4096)
				for i := range payload {
					payload[i] = byte(i % 256)
				}
				return []*DataPoint{
					{Timestamp: 1600000000, Payload: payload},
				}
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			encoder := newBlobSeriesEncoder(&buf)
			for _, point := range tt.input {
				err := encoder.encodePoint(point)
				require.NoError(t, err)
			}
			err := encoder.flush()
			require.NoError(t, err)

			decoder, err := newBlobSeriesDecoder(&buf)
			require.NoError(t, err)
			got := make([]*DataPoint, 0, len(tt.input))
			for range tt.input {
				p := &DataPoint{}
				err := decoder.decodePoint(p)
				require.NoError(t, err)
				got = append(got, p)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_blobEncoder_does_not_affect_numeric(t *testing.T) {
	var buf bytes.Buffer
	encoder := newSeriesEncoder(&buf)
	points := []*DataPoint{
		{Timestamp: 1600000000, Value: 0.1},
		{Timestamp: 1600000060, Value: 1.1},
	}
	for _, p := range points {
		require.NoError(t, encoder.encodePoint(p))
	}
	require.NoError(t, encoder.flush())

	decoder, err := newSeriesDecoder(&buf)
	require.NoError(t, err)
	for _, want := range points {
		got := &DataPoint{}
		require.NoError(t, decoder.decodePoint(got))
		assert.Equal(t, want.Timestamp, got.Timestamp)
		assert.Equal(t, want.Value, got.Value)
		assert.Nil(t, got.Payload)
	}
}
