package datasync

import (
	"context"
	"errors"
	"fmt"
	"github.com/onflow/flow-go/consensus/hotstuff/model"
	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module/executiondatasync/execution_data"
	execData "github.com/onflow/flow/protobuf/go/flow/executiondata"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
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
	blocks      *archive.SafeDeque // blocks of block identifiers for next downloads
	records     *archive.SafeDeque // blocks of downloaded execution data records
	limit       uint               // records size limit for downloaded records
	busy        uint32             // used as a guard to avoid concurrent polling
	chain       flow.Chain
}

// NewExecDataSync returns a new Exec data sync object using the given AN client and options.
func NewExecDataSync(log zerolog.Logger, client execData.ExecutionDataAPIClient, chain flow.Chain, options ...Option) *ExecDataSync {

	cfg := DefaultConfig
	for _, option := range options {
		option(&cfg)
	}

	e := ExecDataSync{
		log:         log.With().Str("component", "exec_data_sync").Logger(),
		execDataApi: client,
		blocks:      archive.NewDeque(),
		records:     archive.NewDeque(),
		limit:       cfg.BufferSize,
		busy:        0,
		chain:       chain,
	}

	if client == nil {
		e.log.Error().Msg("could not access exec data api client")
	}

	for _, blockID := range cfg.CatchupBlocks {
		e.blocks.PushFront(blockID)
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
	// We push the block ID to the front of the blocks; the streamer will try to
	// download the blocks in a FIFO manner.
	e.blocks.PushFront(blockID)

	e.log.Debug().Hex("block", blockID[:]).Msg("execution record queued for download")
}

// Next returns the next available block data. It returns an ErrUnavailable if no block
// data is available at the moment.
func (e *ExecDataSync) Next() (*execution_data.BlockExecutionData, error) {

	// If we are not polling already, we want to start polling in the
	// background. This will try to fill the records up until its limit is
	// reached. It basically means that the cloud streamer will always be
	// downloading if something is available and the execution tracker is asking
	// for the next record.
	go e.poll()

	// If we have nothing in the records, we can return the unavailable error,
	// which will cause the mapper logic to go into a wait state and retry a bit
	// later.
	if e.records.Len() == 0 {
		e.log.Debug().Msg("records empty, no execution record available")
		return nil, archive.ErrUnavailable
	}

	// If we have a record in the records, we will just return it. The records is
	// concurrency safe, so there is no problem with popping from the back while
	// the poll is pushing new items in the front.
	record := e.records.PopBack()
	return record.(*execution_data.BlockExecutionData), nil
}

func (e *ExecDataSync) poll() {

	// We only call `Next()` sequentially, so there is no need to guard it from
	// concurrent access. However, when the records is not empty, we might still
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

		// We only want to retrieve and process execData blocks until the records is full.
		if uint(e.records.Len()) >= e.limit {
			e.log.Debug().Uint("limit", e.limit).Msg("records full, stopping execution record pull")
			return nil
		}

		// We only want to retrieve and process files for blocks that have already
		// been finalized, in the order that they have been finalized. This
		// causes some latency, as we don't download until after a block is
		// finalized, even if the data is available before. However, it seems to
		// be the only way to make sure trie updates are delivered to the mapper
		// in the right order without changing the way uploads work.
		if e.blocks.Len() == 0 {
			e.log.Debug().Msg("blocks empty, stopping execution record download")
			return nil
		}

		// Get the name of the file based on the block ID. The file n
		blockID := e.blocks.PopBack().(flow.Identifier)
		record, err := e.pullData(context.Background(), blockID)
		if err != nil {
			e.blocks.PushBack(blockID)
			return fmt.Errorf("could not pull execution record (name: %s): %w", blockID, err)
		}

		e.log.Debug().
			Hex("blockID", blockID[:]).
			Msg("pushing execution record into records")

		e.records.PushFront(record)
	}
}

func (e *ExecDataSync) pullData(ctx context.Context, blockID flow.Identifier) (*execution_data.BlockExecutionData, error) {
	// query AN from cached connection
	req := &execData.GetExecutionDataByBlockIDRequest{BlockId: blockID[:]}
	res, err := e.execDataApi.GetExecutionDataByBlockID(ctx, req)
	if err != nil {
		return nil, err
	}
	blockExecData, err := convert.MessageToBlockExecutionData(res.BlockExecutionData, e.chain)
	if err != nil {
		return nil, err
	}
	return blockExecData, nil
}
