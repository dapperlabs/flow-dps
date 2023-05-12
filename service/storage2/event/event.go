package event

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"go.uber.org/multierr"

	// TODO: CCF
	"github.com/onflow/flow-go/model/encoding/cbor"

	"github.com/onflow/flow-archive/service/storage2/config"
	"github.com/onflow/flow-go/model/flow"
)

type Storage struct {
	db         *pebble.DB
	marshaller *cbor.Marshaler
}

// NewStorage creates a pebble-backed payload storage.
// The reason we use a separate storage for payloads we need a Comparer with a custom Split function.
//
// It needs to access the last available payload with height less or equal to the requested height.
// This means all point-lookups are range scans.
func NewStorage(dbPath string, cache *pebble.Cache) (*Storage, error) {
	opts := config.DefaultPebbleOptions(cache, config.NewMVCCComparer())
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	return &Storage{
		db:         db,
		marshaller: cbor.NewMarshaler(),
	}, nil
}

func (s *Storage) StreamEventsBySecondaryIndex(
	startHeight uint64,
	key string,
) (Iterator, error) {
	secondaryKey := newSecondaryKey(startHeight, key)
	iter := s.db.NewIter(&pebble.IterOptions{
		UseL6Filters: true,
	})

	return &iteratorWrapper{key: secondaryKey, storage: s, iter: iter}, nil
}

func (s *Storage) getEventByReference(
	reference pkeyReference,
) (flow.Event, error) {
	pKey := newPrimaryKey(reference)
	eventBytes, closer, err := s.db.Get(pKey.Bytes())
	if err != nil {
		return flow.Event{}, fmt.Errorf("failed to get event: %x: %w", pKey.Bytes(), err)
	}
	defer multierr.AppendInvoke(&err, multierr.Close(closer))

	var event flow.Event
	err = s.marshaller.Unmarshal(eventBytes, &event)
	if err != nil {
		return flow.Event{}, fmt.Errorf("failed to decode event: %x: %w", pKey.Bytes(), err)
	}
	return event, nil
}

func (s *Storage) getEventsByReference(
	height uint64,
	binaryRefs []byte,
) ([]flow.Event, error) {
	compactRefs, err := newCompactReferencesFromBytes(binaryRefs)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal refs: %x: %w", binaryRefs, err)
	}

	events := make([]flow.Event, 0, len(compactRefs))
	for _, compactRef := range compactRefs {
		ref := pkeyReference{height: height, compactPkeyReference: compactRef}
		event, err := s.getEventByReference(ref)
		if err != nil {
			return nil, fmt.Errorf("failed to get event: %d/%d/%d: %w",
				ref.height, ref.transactionIndex, ref.eventIndex, err)
		}
		events = append(events, event)
	}

	return events, nil
}

func (s *Storage) getEventsBySecondaryKey(
	height uint64,
	secondaryKey []byte,
) ([]flow.Event, error) {
	binaryRefs, closer, err := s.db.Get(secondaryKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return []flow.Event{}, nil
		}
		return nil, fmt.Errorf("failed to get secondary key: %s: %w", secondaryKey, err)
	}
	defer multierr.AppendInvoke(&err, multierr.Close(closer))

	events, err := s.getEventsByReference(height, binaryRefs)
	if err != nil {
		return nil, fmt.Errorf("failed to get event: %s/%d, %x: %w",
			secondaryKey, height, binaryRefs, err)
	}

	return events, nil
}

func (s *Storage) GetEventsBySecondaryIndex(
	height uint64,
	key string,
) ([]flow.Event, error) {
	secondaryKey := newSecondaryKey(height, key)
	events, err := s.getEventsBySecondaryKey(height, secondaryKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get events by secondary key: %s: %d: %w",
			key, height, err)
	}

	return events, nil
}

// BatchSetEvents sets the given entries in a batch.
func (s *Storage) BatchSetEvents(
	height uint64,
	events []flow.Event,
) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	indices := make(map[string][]compactPkeyReference)
	for _, event := range events {
		reference := pkeyReference{
			height: height,
			compactPkeyReference: compactPkeyReference{
				transactionIndex: event.TransactionIndex,
				eventIndex:       event.EventIndex,
			},
		}
		key := newPrimaryKey(reference)
		eventBytes, err := s.marshaller.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		// save event by primary key
		err = batch.Set(key.Bytes(), eventBytes, nil)
		if err != nil {
			return fmt.Errorf("failed to set key: %w", err)
		}

		// group secondary indices by key
		keyIndices, err := key.SecondaryIndices(event.Type)
		if err != nil {
			return fmt.Errorf("failed to create secondary indices: %w", err)
		}
		for _, secondaryKey := range keyIndices.keys {
			if _, ok := indices[secondaryKey]; !ok {
				indices[secondaryKey] = make([]compactPkeyReference, 0, 1)
			}
			indices[secondaryKey] = append(indices[secondaryKey], keyIndices.value)
		}
	}

	// save secondary indices
	for secondaryKey, value := range indices {
		err := batch.Set([]byte(secondaryKey), compactPkeyReferences(value).Bytes(), nil)
		if err != nil {
			return fmt.Errorf("failed to set secondary key: %w", err)
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}

// Close the storage.
func (s *Storage) Close() error {
	return s.db.Close()
}
