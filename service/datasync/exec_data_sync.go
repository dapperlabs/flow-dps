package datasync

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/onflow/flow-go/consensus/hotstuff/model"
	"github.com/onflow/flow-go/engine"
	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module/component"
	"github.com/onflow/flow-go/module/executiondatasync/execution_data"
	"github.com/onflow/flow-go/module/irrecoverable"
	execData "github.com/onflow/flow/protobuf/go/flow/executiondata"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-archive/models/archive"
)

// ExecDataSync is a component to retrieve execution data from a trusted access node API instead of the GCPStreamer
type ExecDataSync struct {
	log         zerolog.Logger
	execDataApi execData.ExecutionDataAPIClient
	blocks      *archive.SafeDeque // queue of block identifiers for next downloads
	records     *archive.SafeDeque // queue of downloaded execution data records
	limit       uint               // records size limit for downloaded records
	chain       flow.Chain
	*component.ComponentManager
	pollNotifier engine.Notifier
}

// NewExecDataSync returns a new Exec data sync object using the given AN client and options.
func NewExecDataSync(log zerolog.Logger, client execData.ExecutionDataAPIClient, chain flow.Chain, options ...Option) *ExecDataSync {

	cfg := DefaultConfig
	for _, option := range options {
		option(&cfg)
	}

	e := ExecDataSync{
		log:          log.With().Str("component", "exec_data_sync").Logger(),
		execDataApi:  client,
		blocks:       archive.NewDeque(),
		records:      archive.NewDeque(),
		limit:        cfg.BufferSize,
		chain:        chain,
		pollNotifier: engine.NewNotifier(),
	}

	if client == nil {
		e.log.Error().Msg("could not access exec data api client")
	}

	e.ComponentManager = component.NewComponentManagerBuilder().
		AddWorker(e.poll).
		Build()

	for _, blockID := range cfg.CatchupBlocks {
		e.blocks.PushFront(blockID)
		e.log.Debug().Hex("block", blockID[:]).Msg("execution record queued for catch-up")
	}

	return &e
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

	e.pollNotifier.Notify()

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

func (e *ExecDataSync) poll(ctx irrecoverable.SignalerContext, ready component.ReadyFunc) {
	ready()
	notifier := e.pollNotifier.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case <-notifier:
			err := e.getExecData(ctx)
			if errors.Is(err, storage.ErrObjectNotExist) {
				e.log.Debug().Msg("next execution record not available, download stopped")
				return
			}
			if err != nil {
				e.log.Error().Err(err).Msg("could not download execution records")
				return
			}
		}

	}
}

func (e *ExecDataSync) getExecData(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		// We only want to retrieve and process execData blocks until the records is full.
		if uint(e.records.Len()) >= e.limit {
			e.log.Debug().Uint("limit", e.limit).Msg("records full, stopping execution record pull")
			return nil
		}

		// Get the name of the file based on the block ID. The file n
		blockID := e.blocks.PopBack().(flow.Identifier)
		record, err := e.pullData(ctx, blockID)
		if err != nil {
			e.blocks.PushBack(blockID)
			return fmt.Errorf("could not pull execution record (name: %s): %w", blockID, err)
		}

		e.log.Debug().
			Hex("blockID", blockID[:]).
			Msg("pushing execution record into records")

		e.records.PushFront(record)
		return nil
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
