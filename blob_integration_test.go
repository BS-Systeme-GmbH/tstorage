package tstorage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_blob_memory_roundtrip(t *testing.T) {
	storage, err := NewStorage(
		WithTimestampPrecision(Seconds),
	)
	require.NoError(t, err)
	defer storage.Close()

	err = storage.InsertRows([]Row{
		{Metric: "sensor1", DataPoint: DataPoint{Timestamp: 1600000000, Payload: []byte("data-a")}},
		{Metric: "sensor1", DataPoint: DataPoint{Timestamp: 1600000001, Payload: []byte("data-b")}},
		{Metric: "sensor1", DataPoint: DataPoint{Timestamp: 1600000002, Payload: []byte{0x00, 0xFF}}},
	})
	require.NoError(t, err)

	points, err := storage.Select("sensor1", nil, 1600000000, 1600000003)
	require.NoError(t, err)
	require.Len(t, points, 3)
	assert.Equal(t, []byte("data-a"), points[0].Payload)
	assert.Equal(t, []byte("data-b"), points[1].Payload)
	assert.Equal(t, []byte{0x00, 0xFF}, points[2].Payload)
}

func Test_blob_disk_roundtrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tstorage-blob-disk")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	storage, err := NewStorage(
		WithDataPath(tmpDir),
		WithPartitionDuration(100*time.Second),
		WithTimestampPrecision(Seconds),
	)
	require.NoError(t, err)

	for ts := int64(1600000000); ts < 1600000010; ts++ {
		err := storage.InsertRows([]Row{
			{Metric: "blob-metric", DataPoint: DataPoint{Timestamp: ts, Payload: []byte{byte(ts & 0xFF), 0xAA}}},
		})
		require.NoError(t, err)
	}

	require.NoError(t, storage.Close())

	storage2, err := NewStorage(
		WithDataPath(tmpDir),
		WithPartitionDuration(100*time.Second),
		WithTimestampPrecision(Seconds),
	)
	require.NoError(t, err)
	defer storage2.Close()

	points, err := storage2.Select("blob-metric", nil, 1600000000, 1600000010)
	require.NoError(t, err)
	require.Len(t, points, 10)
	for i, p := range points {
		ts := int64(1600000000) + int64(i)
		assert.Equal(t, ts, p.Timestamp)
		assert.Equal(t, []byte{byte(ts & 0xFF), 0xAA}, p.Payload)
	}
}

func Test_blob_wal_recovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tstorage-blob-wal")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	walPath := filepath.Join(tmpDir, "wal")

	wal, err := newDiskWAL(walPath, 4096)
	require.NoError(t, err)

	rows := []Row{
		{Metric: "blob-wal", DataPoint: DataPoint{Timestamp: 100, Payload: []byte("first")}},
		{Metric: "blob-wal", DataPoint: DataPoint{Timestamp: 200, Payload: []byte("second")}},
	}
	require.NoError(t, wal.append(operationInsert, rows))
	require.NoError(t, wal.flush())

	reader, err := newDiskWALReader(walPath)
	require.NoError(t, err)
	require.NoError(t, reader.readAll())

	require.Len(t, reader.rowsToInsert, 2)
	assert.Equal(t, "blob-wal", reader.rowsToInsert[0].Metric)
	assert.Equal(t, int64(100), reader.rowsToInsert[0].Timestamp)
	assert.Equal(t, []byte("first"), reader.rowsToInsert[0].Payload)
	assert.Equal(t, int64(200), reader.rowsToInsert[1].Timestamp)
	assert.Equal(t, []byte("second"), reader.rowsToInsert[1].Payload)
}

func Test_blob_wal_mixed_numeric_and_blob_metrics(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tstorage-blob-wal-mixed")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	walPath := filepath.Join(tmpDir, "wal")

	wal, err := newDiskWAL(walPath, 4096)
	require.NoError(t, err)

	rows := []Row{
		{Metric: "numeric-metric", DataPoint: DataPoint{Timestamp: 100, Value: 42.5}},
		{Metric: "blob-metric", DataPoint: DataPoint{Timestamp: 100, Payload: []byte("payload")}},
		{Metric: "numeric-metric", DataPoint: DataPoint{Timestamp: 200, Value: 43.5}},
	}
	require.NoError(t, wal.append(operationInsert, rows))
	require.NoError(t, wal.flush())

	reader, err := newDiskWALReader(walPath)
	require.NoError(t, err)
	require.NoError(t, reader.readAll())

	require.Len(t, reader.rowsToInsert, 3)

	assert.Equal(t, 42.5, reader.rowsToInsert[0].Value)
	assert.Nil(t, reader.rowsToInsert[0].Payload)

	assert.Equal(t, []byte("payload"), reader.rowsToInsert[1].Payload)
	assert.Equal(t, float64(0), reader.rowsToInsert[1].Value)

	assert.Equal(t, 43.5, reader.rowsToInsert[2].Value)
	assert.Nil(t, reader.rowsToInsert[2].Payload)
}

func Test_blob_mixed_series_kind_rejected(t *testing.T) {
	storage, err := NewStorage(
		WithTimestampPrecision(Seconds),
	)
	require.NoError(t, err)
	defer storage.Close()

	err = storage.InsertRows([]Row{
		{Metric: "m1", DataPoint: DataPoint{Timestamp: 1, Value: 1.0}},
	})
	require.NoError(t, err)

	err = storage.InsertRows([]Row{
		{Metric: "m1", DataPoint: DataPoint{Timestamp: 2, Payload: []byte("x")}},
	})
	assert.ErrorIs(t, err, ErrMixedSeriesKind)
}

func Test_blob_and_numeric_coexist_different_metrics(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tstorage-blob-coexist")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	storage, err := NewStorage(
		WithDataPath(tmpDir),
		WithPartitionDuration(100*time.Second),
		WithTimestampPrecision(Seconds),
	)
	require.NoError(t, err)

	for ts := int64(1600000000); ts < 1600000005; ts++ {
		require.NoError(t, storage.InsertRows([]Row{
			{Metric: "temperature", DataPoint: DataPoint{Timestamp: ts, Value: 21.5}},
		}))
		require.NoError(t, storage.InsertRows([]Row{
			{Metric: "raw-frame", DataPoint: DataPoint{Timestamp: ts, Payload: []byte{0xDE, 0xAD, byte(ts & 0xFF)}}},
		}))
	}

	require.NoError(t, storage.Close())

	storage2, err := NewStorage(
		WithDataPath(tmpDir),
		WithPartitionDuration(100*time.Second),
		WithTimestampPrecision(Seconds),
	)
	require.NoError(t, err)
	defer storage2.Close()

	numericPts, err := storage2.Select("temperature", nil, 1600000000, 1600000005)
	require.NoError(t, err)
	require.Len(t, numericPts, 5)
	for _, p := range numericPts {
		assert.Equal(t, 21.5, p.Value)
		assert.Nil(t, p.Payload)
	}

	blobPts, err := storage2.Select("raw-frame", nil, 1600000000, 1600000005)
	require.NoError(t, err)
	require.Len(t, blobPts, 5)
	for i, p := range blobPts {
		ts := int64(1600000000) + int64(i)
		assert.Equal(t, []byte{0xDE, 0xAD, byte(ts & 0xFF)}, p.Payload)
	}
}
