package event

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/onflow/flow-go/model/flow"
)

// Usage:
//
//	for iter.First(); iter.Valid(); iter.Next() {
//		events, err := iter.ValueAndErr()
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("%+v\n", events)
//	}
//
//	if err := iter.Close(); err != nil {
//		log.Fatal(err)
//	}

type Iterator interface {
	First() bool
	Next() bool
	Valid() bool
	ValueAndErr() ([]flow.Event, error)
	Close() error
}

var _ Iterator = &iteratorWrapper{}

type iteratorWrapper struct {
	key     []byte
	storage *Storage
	iter    *pebble.Iterator
}

func (w *iteratorWrapper) First() bool {
	return w.iter.SeekPrefixGE(w.key)
}

func (w *iteratorWrapper) Next() bool {
	return w.iter.Next()
}

func (w *iteratorWrapper) Valid() bool {
	return w.iter.Valid()
}

func (w *iteratorWrapper) ValueAndErr() ([]flow.Event, error) {
	key := w.iter.Key()
	if len(key) < uint64Len {
		return nil, fmt.Errorf("invalid secondary key len: %d: %x", len(key), key)
	}
	height := binary.BigEndian.Uint64(key[len(key)-uint64Len:])

	binaryRefs, err := w.iter.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("failed to get compact refs: %w", err)
	}

	events, err := w.storage.getEventsByReference(height, binaryRefs)
	if err != nil {
		return nil, fmt.Errorf("failed to get events: %d, %x: %w",
			height, binaryRefs, err)
	}

	return events, nil
}

func (w *iteratorWrapper) Close() error {
	return w.iter.Close()
}
