package storage

import "context"

type Record struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

type Header struct {
	Key   string
	Value []byte
}

type StorageEngine interface {
	Append(ctx context.Context, topic string, partition int32, records []Record) (baseOffset int64, err error)
	Read(ctx context.Context, topic string, partition int32, startOffset int64, maxBytes int) ([]Record, error)
	HighWatermark(topic string, partition int32) (int64, error)
	LogStartOffset(topic string, partition int32) (int64, error)
	CreatePartition(topic string, partition int32) error
	DeletePartition(topic string, partition int32) error
}
