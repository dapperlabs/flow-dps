package stream

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"github.com/hashicorp/go-multierror"
	"github.com/onflow/flow-archive/models/archive"
	"github.com/onflow/flow-go/consensus/hotstuff/model"
	"github.com/onflow/flow-go/engine/execution/ingestion/uploader"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow/protobuf/go/flow/access"
	execData "github.com/onflow/flow/protobuf/go/flow/executiondata"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ExecDataStreamer struct {
	log       zerolog.Logger
	execApi   execData.ExecutionDataAPIClient
	accessApi access.AccessAPIClient
	queue     *archive.SafeDeque // queue of block identifiers for next downloads
	buffer    *archive.SafeDeque // queue of downloaded execution data records
	limit     uint               // buffer size limit for downloaded records
	busy      uint32             // used as a guard to avoid concurrent polling
	ctx       context.Context
	chain     flow.ChainID
}

func NewExecDataStreamer(log zerolog.Logger, accessAddr string, msgSize int, options ...Option) *ExecDataStreamer {
	cfg := DefaultConfig
	for _, option := range options {
		option(&cfg)
	}

	ctx := context.Background()

	// initialize clients
	opts := []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(msgSize)),
		grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.Dial(accessAddr, opts...)
	if err != nil {
		log.Error()
	}
	execDataAPI := execData.NewExecutionDataAPIClient(conn)
	accessAPI := access.NewAccessAPIClient(conn)

	// get chainID from network params
	params, err := accessAPI.GetNetworkParameters(ctx, &access.GetNetworkParametersRequest{})
	if err != nil {
		log.Error().Err(err).Msg("unable to get network params")
	}
	chain := flow.ChainID(params.ChainId)

	return &ExecDataStreamer{
		log:       log,
		execApi:   execDataAPI,
		accessApi: accessAPI,
		queue:     archive.NewDeque(),
		buffer:    archive.NewDeque(),
		limit:     cfg.BufferSize,
		busy:      0,
		ctx:       ctx,
		chain:     chain,
	}
}

// OnBlockFinalized is a callback for the Flow consensus follower. It is called
// each time a block is finalized by the Flow consensus algorithm.
func (e *ExecDataStreamer) OnBlockFinalized(block *model.Block) {
	blockID := block.BlockID
	// We push the block ID to the front of the queue; the streamer will try to
	// download the blocks in a FIFO manner.
	e.queue.PushFront(blockID)

	e.log.Debug().Hex("block", blockID[:]).Msg("execution record queued for download")
}

// Next returns the next available block data. It returns an ErrUnavailable if no block
// data is available at the moment.
func (e *ExecDataStreamer) Next() (*uploader.BlockData, error) {
	// same implementation as GCPStreamer
	go e.poll()
	if e.buffer.Len() == 0 {
		e.log.Debug().Msg("buffer empty, no execution record available")
		return nil, archive.ErrUnavailable
	}
	record := e.buffer.PopBack()
	return record.(*uploader.BlockData), nil
}

func (e *ExecDataStreamer) poll() {
	// same implementation as GCPStreamer
	if !atomic.CompareAndSwapUint32(&e.busy, 0, 1) {
		return
	}
	defer atomic.StoreUint32(&e.busy, 0)
	err := e.pullExecData()
	if errors.Is(err, storage.ErrObjectNotExist) {
		e.log.Debug().Msg("next execution record not available, download stopped")
		return
	}
	if err != nil {
		e.log.Error().Err(err).Msg("could not download execution records")
		return
	}
}

func (e ExecDataStreamer) pullExecData() error {
	for {
		// same implementation as GCPStreamer
		if uint(e.buffer.Len()) >= e.limit {
			e.log.Debug().Uint("limit", e.limit).Msg("buffer full, stopping execution record download")
			return nil
		}

		if uint(e.queue.Len()) == 0 {
			e.log.Debug().Msg("queue empty, stopping execution record download")
			return nil
		}
		blockID := e.queue.PopBack().(flow.Identifier)
		record, err := e.getUploaderBlockData(blockID)
		if err != nil {
			e.queue.PushBack(blockID)
			return fmt.Errorf("could not pull execution record (name: %s): %w", blockID, err)
		}

		e.log.Info().
			Str("name", blockID.String()).
			Uint64("height", record.Block.Header.Height).
			Hex("block", blockID[:]).
			Msg("pushing execution record into buffer")
		e.buffer.PushFront(record)
	}
}

func (e *ExecDataStreamer) getUploaderBlockData(blockID flow.Identifier) (*uploader.BlockData, error) {
	// TODO : get rid of uploader.BlockData use
	// we currently have to query additional data from ANs to keep a 1:1 match of the data from the GCP strea
	var errs *multierror.Error
	// get block
	br := &access.GetBlockByIDRequest{Id: blockID[:]}
	b, err := e.accessApi.GetBlockByID(e.ctx, br)
	if err != nil {
		errs = multierror.Append(errs, fmt.Errorf("failed to get block data for blockID (%s): %w", blockID, err))
	}

	// get transactions
	txr := &access.GetTransactionsByBlockIDRequest{BlockId: blockID[:]}
	tx, err := e.accessApi.GetTransactionResultsByBlockID(e.ctx, txr)
	if err != nil {
		errs = multierror.Append(errs, fmt.Errorf("failed to get transaction results for blockID (%s): %w", blockID, err))
	}
	// get exec data
	exr := &execData.GetExecutionDataByBlockIDRequest{BlockId: blockID[:]}
	ex, err := e.execApi.GetExecutionDataByBlockID(e.ctx, exr)
	if err != nil {
		errs = multierror.Append(errs, fmt.Errorf("failed to get execution data for blockID (%s): %w", blockID, err))
	}
	err = errs.ErrorOrNil()
	if err != nil {
		return nil, err
	}
	return e.aggregateToBlockData(b, tx, ex)
}

// function to aggregate response data tp uploader.BlockData
// TODO: remove this function when we use the streaming API
func (e *ExecDataStreamer) aggregateToBlockData(
	bl *access.BlockResponse,
	tx *access.TransactionResultsResponse,
	ex *execData.GetExecutionDataByBlockIDResponse,
) (*uploader.BlockData, error) {
	return nil, nil
}
