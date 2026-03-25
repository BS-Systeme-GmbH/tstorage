package tstorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// blobEncoder encodes data points that carry arbitrary binary payloads.
// Each point is stored as: varint(timestamp) + uvarint(payload_len) + payload_bytes.
type blobEncoder struct {
	w   io.Writer
	buf []byte
}

func newBlobSeriesEncoder(w io.Writer) seriesEncoder {
	return &blobEncoder{w: w}
}

func (e *blobEncoder) encodePoint(point *DataPoint) error {
	tmp := make([]byte, binary.MaxVarintLen64)

	n := binary.PutVarint(tmp, point.Timestamp)
	e.buf = append(e.buf, tmp[:n]...)

	payload := point.Payload
	if payload == nil {
		payload = []byte{}
	}
	n = binary.PutUvarint(tmp, uint64(len(payload)))
	e.buf = append(e.buf, tmp[:n]...)

	e.buf = append(e.buf, payload...)
	return nil
}

func (e *blobEncoder) flush() error {
	if len(e.buf) == 0 {
		return nil
	}
	_, err := e.w.Write(e.buf)
	if err != nil {
		return fmt.Errorf("failed to flush blob data: %w", err)
	}
	e.buf = e.buf[:0]
	return nil
}

// blobDecoder decodes data points produced by blobEncoder.
type blobDecoder struct {
	r *bytes.Reader
}

func newBlobSeriesDecoder(r io.Reader) (seriesDecoder, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read blob data: %w", err)
	}
	return &blobDecoder{r: bytes.NewReader(b)}, nil
}

func (d *blobDecoder) decodePoint(dst *DataPoint) error {
	ts, err := binary.ReadVarint(d.r)
	if err != nil {
		return fmt.Errorf("failed to read blob timestamp: %w", err)
	}
	dst.Timestamp = ts

	payloadLen, err := binary.ReadUvarint(d.r)
	if err != nil {
		return fmt.Errorf("failed to read blob payload length: %w", err)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(d.r, payload); err != nil {
		return fmt.Errorf("failed to read blob payload: %w", err)
	}
	dst.Payload = payload
	return nil
}
