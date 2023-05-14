package index

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/sync/semaphore"

	"github.com/onflow/flow-go/ledger"
	"github.com/onflow/flow-go/ledger/complete/wal"
	"github.com/onflow/flow-go/model/flow"

	"github.com/onflow/flow-archive/models/archive"
	"github.com/onflow/flow-archive/models/convert"
	"github.com/onflow/flow-archive/util"
)

// Writer implements the `index.Writer` interface to write indexing data to
// an underlying Badger database.
type Writer struct {
	sync.RWMutex
	db   *badger.DB
	cfg  Config
	tx   *badger.Txn
	sema *semaphore.Weighted
	err  chan error

	// Old badger-based index. Eventually all methods from here would migrate to lib2.
	lib archive.WriteLibrary
	// New pebble-based index.  Would likely consist of multiple separate pebble databases underneath.
	lib2 archive.WriteLibrary2

	done  chan struct{}   // signals when no more new operations will be added
	mutex *sync.Mutex     // guards the current transaction against concurrent access
	wg    *sync.WaitGroup // keeps track of when the flush goroutine should exit
}

// NewWriter creates a new index writer that writes new indexing data to the
// given Badger database.
func NewWriter(
	db *badger.DB,
	lib archive.WriteLibrary,
	lib2 archive.WriteLibrary2,
	options ...func(*Config),
) *Writer {

	cfg := DefaultConfig
	for _, option := range options {
		option(&cfg)
	}

	w := Writer{
		db:   db,
		cfg:  cfg,
		tx:   db.NewTransaction(true),
		sema: semaphore.NewWeighted(int64(cfg.ConcurrentTransactions)),
		err:  make(chan error, cfg.ConcurrentTransactions),

		lib:  lib,
		lib2: lib2,

		done:  make(chan struct{}),
		mutex: &sync.Mutex{},
		wg:    &sync.WaitGroup{},
	}

	// No flush interval means that flushing is disabled, and we only commit
	// badger transactions that are full. This optimizes throughput of writing
	// to the database, but creates latency if transactions don't fill up fast
	// enough to be committed at maximum size.
	if cfg.FlushInterval > 0 {
		w.wg.Add(1)
		go w.flush()
	}

	return &w
}

// First indexes the height of the first finalized block.
func (w *Writer) First(height uint64) error {
	return w.apply(w.lib.SaveFirst(height))
}

// Last indexes the height of the last finalized block.
func (w *Writer) Last(height uint64) error {
	return w.apply(w.lib.SaveLast(height))
}

// Height indexes the height for the given block ID.
func (w *Writer) Height(blockID flow.Identifier, height uint64) error {
	return w.apply(w.lib.IndexHeightForBlock(blockID, height))
}

// Commit indexes the given commitment of the execution state as it was after
// the execution of the finalized block at the given height.
func (w *Writer) Commit(height uint64, commit flow.StateCommitment) error {
	return w.apply(w.lib.SaveCommit(height, commit))
}

// Header indexes the given header of a finalized block at the given height.
func (w *Writer) Header(height uint64, header *flow.Header) error {
	return w.apply(w.lib.SaveHeader(height, header))
}

// batch atomically writes a set of entries to the database.
func (w *Writer) batch(height uint64, entries flow.RegisterEntries) error {
	batch := util.NewBatch(w.db)

	writeBatch := batch.GetWriter()
	defer writeBatch.Cancel()

	err := w.lib2.BatchSetPayload(height, entries)
	if err != nil {
		return fmt.Errorf("could not batch write registers to database at height %v: %w", height, err)
	}

	return nil
}

// Payloads indexes the given payloads, which should represent a trie update
// of the execution state contained within the finalized block at the given
// height.
func (w *Writer) Payloads(height uint64, payloads []*ledger.Payload) error {
	// Convert the payloads to register entries by extracting and converting register IDs.
	entries := make(flow.RegisterEntries, 0, len(payloads))
	for _, p := range payloads {
		key, err := p.Key()
		if err != nil {
			return fmt.Errorf("could not get key from register payload: %w", err)
		}

		registerID, err := convert.KeyToRegisterID(key)
		if err != nil {
			return fmt.Errorf("could not get register ID from key: %w", err)
		}

		entries = append(entries, flow.RegisterEntry{
			Key:   registerID,
			Value: p.Value(),
		})
	}

	return w.batch(height, entries)
}

// Registers writes the given registers in a batch to database
func (w *Writer) Registers(height uint64, registers []*wal.LeafNode) error {
	payloads := make([]*ledger.Payload, 0, len(registers))
	for _, register := range registers {
		payloads = append(payloads, register.Payload)
	}
	return w.Payloads(height, payloads)
}

// Collections indexes the collections at the given height.
func (w *Writer) Collections(height uint64, collections []*flow.LightCollection) error {

	ops := make([]func(*badger.Txn) error, 0, 2*len(collections)+1)

	collIDs := make([]flow.Identifier, 0, len(collections))
	for _, collection := range collections {
		collID := collection.ID()
		collIDs = append(collIDs, collID)
		ops = append(ops, w.lib.SaveCollection(collection))
		ops = append(ops, w.lib.IndexTransactionsForCollection(collID, collection.Transactions))
	}

	ops = append(ops, w.lib.IndexCollectionsForHeight(height, collIDs))

	return w.apply(ops...)
}

// Guarantees indexes the guarantees at the given height.
func (w *Writer) Guarantees(_ uint64, guarantees []*flow.CollectionGuarantee) error {

	ops := make([]func(*badger.Txn) error, 0, len(guarantees))
	for _, guarantee := range guarantees {
		ops = append(ops, w.lib.SaveGuarantee(guarantee))
	}

	return w.apply(ops...)
}

// Transactions indexes the transactions at the given height.
func (w *Writer) Transactions(height uint64, transactions []*flow.TransactionBody) error {

	ops := make([]func(*badger.Txn) error, 0, 2*len(transactions)+1)

	txIDs := make([]flow.Identifier, 0, len(transactions))
	for _, transaction := range transactions {
		txID := transaction.ID()
		txIDs = append(txIDs, txID)
		ops = append(ops, w.lib.SaveTransaction(transaction))
		ops = append(ops, w.lib.IndexHeightForTransaction(txID, height))
	}

	ops = append(ops, w.lib.IndexTransactionsForHeight(height, txIDs))

	return w.apply(ops...)
}

// Results indexes the transaction results at the given height.
func (w *Writer) Results(results []*flow.TransactionResult) error {

	ops := make([]func(*badger.Txn) error, 0, len(results))

	for _, result := range results {
		ops = append(ops, w.lib.SaveResult(result))
	}

	return w.apply(ops...)
}

// Events indexes the events, which should represent all events of the finalized
// block at the given height.
func (w *Writer) Events(height uint64, events []flow.Event) error {

	buckets := make(map[flow.EventType][]flow.Event)
	for _, event := range events {
		buckets[event.Type] = append(buckets[event.Type], event)
	}

	ops := make([]func(*badger.Txn) error, 0, len(buckets))

	for typ, set := range buckets {
		ops = append(ops, w.lib.SaveEvents(height, typ, set))
	}

	return w.apply(ops...)
}

// Seals indexes the seals, which should represent all seals in the finalized
// block at the given height.
func (w *Writer) Seals(height uint64, seals []*flow.Seal) error {

	ops := make([]func(*badger.Txn) error, 0, len(seals)+1)

	sealIDs := make([]flow.Identifier, 0, len(seals))
	for _, seal := range seals {
		sealID := seal.ID()
		sealIDs = append(sealIDs, sealID)
		ops = append(ops, w.lib.SaveSeal(seal))
	}

	ops = append(ops, w.lib.IndexSealsForHeight(height, sealIDs))

	return w.apply(ops...)
}

func (w *Writer) apply(ops ...func(*badger.Txn) error) error {

	// Before applying an additional operation to the transaction we are
	// currently building, we want to see if there was an error committing any
	// previous transaction.
	select {
	case err := <-w.err:
		return fmt.Errorf("could not commit transaction: %w", err)
	default:
		// skip
	}

	// If we had no error in a previous transaction, we try applying the
	// operation to the current transaction. If the transaction is already too
	// big, we simply commit it with our callback and start a new transaction.
	// Transaction creation is guarded by a semaphore that limits it to the
	// configured number of inflight transactions.
	for _, op := range ops {
		w.mutex.Lock()
		err := op(w.tx)
		if errors.Is(err, badger.ErrTxnTooBig) {
			_ = w.sema.Acquire(context.Background(), 1)
			w.tx.CommitWith(w.committed)
			w.tx = w.db.NewTransaction(true)
			err = op(w.tx)
		}
		w.mutex.Unlock()
		if err != nil {
			return fmt.Errorf("could not apply operation: %w", err)
		}
	}

	return nil
}

func (w *Writer) committed(err error) {

	// When a transaction is fully committed, we get the result in this
	// callback. In case of an error, we pipe it to the apply function through
	// the error channel.
	if err != nil {
		w.err <- err
	}

	// Releasing one resource on the semaphore will free up one slot for
	// inflight transactions.
	w.sema.Release(1)
}

// Close closes the writer and commits the pending transaction, if there is one.
func (w *Writer) Close() error {

	// Shut down the ticker that makes sure we commit after a certain time
	// without new operations, then drain the tick channel.
	close(w.done)
	w.wg.Wait()

	// The first transaction we created did not claim a slot on the semaphore.
	// This makes sense, because we only want to limit in-flight (committing)
	// transactions. The currently building transaction is not in-progress.
	// However, we still need to make sure that the currently building
	// transaction is properly committed. We assume that we are no longer
	// applying new operations when we call `Close`, so we can explicitly do so
	// here, without using the callback.
	err := w.tx.Commit()
	if err != nil {
		return fmt.Errorf("could not commit final transaction: %w", err)
	}

	// Once we acquire all semaphore resources, it means all transactions have
	// been committed. We can now close the error channel and drain any
	// remaining errors.
	_ = w.sema.Acquire(context.Background(), int64(w.cfg.ConcurrentTransactions))
	close(w.err)
	var merr *multierror.Error
	for err := range w.err {
		merr = multierror.Append(merr, err)
	}

	return merr.ErrorOrNil()
}

func (w *Writer) flush() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {

		case <-ticker.C:
			w.mutex.Lock()
			_ = w.sema.Acquire(context.Background(), 1)
			w.tx.CommitWith(w.committed)
			w.tx = w.db.NewTransaction(true)
			w.mutex.Unlock()

		case <-w.done:
			return
		}
	}
}
