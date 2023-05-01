package payload

import (
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.com/onflow/flow-archive/service/storage2"
	"github.com/onflow/flow-go/model/flow"
)

const (
	// Size of the blockHeight encoded in the key.
	HeightSize = 8
)

type PayloadStorage struct {
	db *pebble.DB
}

// NewPayloadStorage creates a pebble-backed payload storage.
// The reason we use a separate storage for payloads we need a Comparer with a custom Split function.
//
// It needs to access the last available payload with height less or equal to the requested height.
// This means all point-lookups are range scans.
func NewPayloadStorage(dbPath string, cacheSize int64) (*PayloadStorage, error) {
	comparer := *pebble.DefaultComparer
	comparer.Split = func(a []byte) int {
		return len(a) - HeightSize
	}
	comparer.Name = "flow.PlayloadHeightComparer"

	// TODO(rbtz): cache metrics
	cache := pebble.NewCache(cacheSize)
	defer cache.Unref()

	opts := storage2.DefaultPebbleOptions(cache, &comparer)
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	return &PayloadStorage{
		db: db,
	}, nil
}

// Get returns the payload for the given height and RegisterID.
func (s *PayloadStorage) Get(
	height uint64,
	reg flow.RegisterID,
) ([]byte, error) {
	iter := s.db.NewIter(&pebble.IterOptions{
		UseL6Filters: true,
	})
	defer iter.Close()

	encoded := newLookupKey(height, reg).Bytes()
	ok := iter.SeekPrefixGE(encoded)
	if !ok {
		return nil, &ErrNotFound{
			RegisterID: reg,
			Height:     height,
		}
	}

	binaryValue, err := iter.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("failed to get value: %w", err)
	}

	return binaryValue, nil
}

// SetBatch sets the given entries in a batch.
func (s *PayloadStorage) SetBatch(
	height uint64,
	entries flow.RegisterEntries,
) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, entry := range entries {
		encoded := newLookupKey(height, entry.Key).Bytes()

		err := batch.Set(encoded, entry.Value, nil)
		if err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}

// Close closes the storage.
func (s *PayloadStorage) Close() error {
	return s.db.Close()
}
