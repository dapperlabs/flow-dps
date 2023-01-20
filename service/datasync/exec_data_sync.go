package datasync

import (
	"context"
	"errors"
	"fmt"
	"github.com/onflow/flow-go/consensus/hotstuff/model"
	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/engine/execution/computation/computer/uploader"
	"github.com/onflow/flow-go/ledger"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module/mempool/entity"
	"github.com/onflow/flow/protobuf/go/flow/entities"
	execData "github.com/onflow/flow/protobuf/go/flow/executiondata"
	"github.com/optakt/flow-dps/models/dps"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"math"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"github.com/fxamacker/cbor/v2"

	"github.com/onflow/flow-archive/models/archive"
)

// ExecDataSync is a component to retrieve execution data from a trusted access node API instead of the GCPStreamer
type ExecDataSync struct {
	log         zerolog.Logger
	decoder     cbor.DecMode
	execDataApi execData.ExecutionDataAPIClient
	queue       *archive.SafeDeque // queue of block identifiers for next downloads
	buffer      *archive.SafeDeque // queue of downloaded execution data records
	limit       uint               // buffer size limit for downloaded records
	busy        uint32             // used as a guard to avoid concurrent polling
}

// NewExecDataSync returns a new Exec data sync object using the given AN client and options.
func NewExecDataSync(log zerolog.Logger, execDataAddr string, client execData.ExecutionDataAPIClient, options ...Option) *ExecDataSync {

	cfg := DefaultConfig
	for _, option := range options {
		option(&cfg)
	}

	decOptions := cbor.DecOptions{
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
		MaxArrayElements:  math.MaxUint32,
	}
	decoder, err := decOptions.DecMode()
	if err != nil {
		panic(err)
	}

	var exeDataApi execData.ExecutionDataAPIClient
	if client == nil {
		exeDataApi = getAPIClient(execDataAddr)
	} else {
		exeDataApi = client
	}

	e := ExecDataSync{
		log:         log.With().Str("component", "exec_data_sync").Logger(),
		decoder:     decoder,
		execDataApi: exeDataApi,
		queue:       archive.NewDeque(),
		buffer:      archive.NewDeque(),
		limit:       cfg.BufferSize,
		busy:        0,
	}

	for _, blockID := range cfg.CatchupBlocks {
		e.queue.PushFront(blockID)
		e.log.Debug().Hex("block", blockID[:]).Msg("execution record queued for catch-up")
	}

	return &e
}

func getAPIClient(addr string) execData.ExecutionDataAPIClient {
	// connect to Archive-Access instance
	MaxGRPCMessageSize := 1024 * 1024 * 20 // 20MB
	conn, err := grpc.Dial(addr, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxGRPCMessageSize)))
	if err != nil {
		panic(fmt.Sprintf("unable to create connection to node: %s", addr))
	}
	return execData.NewExecutionDataAPIClient(conn)
}

// OnBlockFinalized is a callback for the Flow consensus follower. It is called
// each time a block is finalized by the Flow consensus algorithm.
func (e *ExecDataSync) OnBlockFinalized(block *model.Block) {
	blockID := block.BlockID
	// We push the block ID to the front of the queue; the streamer will try to
	// download the blocks in a FIFO manner.
	e.queue.PushFront(blockID)

	e.log.Debug().Hex("block", blockID[:]).Msg("execution record queued for download")
}

// Next returns the next available block data. It returns an ErrUnavailable if no block
// data is available at the moment.
func (e *ExecDataSync) Next() (*uploader.BlockData, error) {

	// If we are not polling already, we want to start polling in the
	// background. This will try to fill the buffer up until its limit is
	// reached. It basically means that the cloud streamer will always be
	// downloading if something is available and the execution tracker is asking
	// for the next record.
	go e.poll()

	// If we have nothing in the buffer, we can return the unavailable error,
	// which will cause the mapper logic to go into a wait state and retry a bit
	// later.
	if e.buffer.Len() == 0 {
		e.log.Debug().Msg("buffer empty, no execution record available")
		return nil, dps.ErrUnavailable
	}

	// If we have a record in the buffer, we will just return it. The buffer is
	// concurrency safe, so there is no problem with popping from the back while
	// the poll is pushing new items in the front.
	record := e.buffer.PopBack()
	return record.(*uploader.BlockData), nil
}

func (e *ExecDataSync) poll() {

	// We only call `Next()` sequentially, so there is no need to guard it from
	// concurrent access. However, when the buffer is not empty, we might still
	// be polling for new data in the background when the next call happens. We
	// thus need to ensure that only one poll is executed at the same time. We
	// do this with a simple flag that is set atomically to work like a
	// `TryLock()` on a mutex, which is unfortunately not available in Go, see:
	// https://github.com/golang/go/issues/6123
	if !atomic.CompareAndSwapUint32(&e.busy, 0, 1) {
		return
	}
	defer atomic.StoreUint32(&e.busy, 0)

	// At this point, we try to pull new files from the cloud.
	err := e.getExecData()
	if errors.Is(err, storage.ErrObjectNotExist) {
		e.log.Debug().Msg("next execution record not available, download stopped")
		return
	}
	if err != nil {
		e.log.Error().Err(err).Msg("could not download execution records")
		return
	}
}

func (e *ExecDataSync) getExecData() error {

	for {

		// We only want to retrieve and process execData blocks until the buffer is full.
		if uint(e.buffer.Len()) >= e.limit {
			e.log.Debug().Uint("limit", e.limit).Msg("buffer full, stopping execution record pull")
			return nil
		}

		// We only want to retrieve and process files for blocks that have already
		// been finalized, in the order that they have been finalized. This
		// causes some latency, as we don't download until after a block is
		// finalized, even if the data is available before. However, it seems to
		// be the only way to make sure trie updates are delivered to the mapper
		// in the right order without changing the way uploads work.
		if uint(e.queue.Len()) == 0 {
			e.log.Debug().Msg("queue empty, stopping execution record download")
			return nil
		}

		// Get the name of the file based on the block ID. The file n
		blockID := e.queue.PopBack().(flow.Identifier)
		record, err := e.pullData(context.Background(), blockID)
		if err != nil {
			e.queue.PushBack(blockID)
			return fmt.Errorf("could not pull execution record (name: %s): %w", blockID, err)
		}

		e.log.Debug().
			Hex("blockID", blockID[:]).
			Msg("pushing execution record into buffer")

		e.buffer.PushFront(record)
	}
}

func (e *ExecDataSync) pullData(ctx context.Context, blockID flow.Identifier) (*uploader.BlockData, error) {
	// query AN from cached connection
	req := &execData.GetExecutionDataByBlockIDRequest{BlockId: blockID[:]}
	res, err := e.execDataApi.GetExecutionDataByBlockID(ctx, req)
	if err != nil {
		return nil, err
	}
	return convertToBlockData(res.BlockExecutionData)
}

func convertToBlockData(data *entities.BlockExecutionData) (*uploader.BlockData, error) {
	// convert to block exec data defined in flow-go
	bed, err := convert.MessageToBlockExecutionData(data, flow.Mainnet.Chain())
	if err != nil {
		return nil, err
	}

	collections := make([]*entity.CompleteCollection, 0)
	events := make([]*flow.Event, 0)
	trieUpdates := make([]*ledger.TrieUpdate, 0)

	for _, ced := range bed.ChunkExecutionDatas {
		// combine the collections in the chunks
		collectionToAdd := entity.CompleteCollection{
			Guarantee:    nil,
			Transactions: ced.Collection.Transactions,
		}
		collections = append(collections, &collectionToAdd)

		// combine the events in the chunks
		eventsToAdd := make([]*flow.Event, len(ced.Events))
		for i := range ced.Events {
			eventsToAdd[i] = &ced.Events[i]
		}
		events = append(events, eventsToAdd...)

		// combine the trie updates in the chunks
		trieUpdates = append(trieUpdates, ced.TrieUpdate)
	}
	blockData := uploader.BlockData{
		Block:                nil,
		Collections:          collections,
		TxResults:            nil,
		Events:               events,
		TrieUpdates:          trieUpdates,
		FinalStateCommitment: flow.StateCommitment{},
	}
	return &blockData, nil
}
