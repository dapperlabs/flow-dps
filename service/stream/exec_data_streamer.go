package stream

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"github.com/onflow/flow-archive/models/archive"
	"github.com/onflow/flow-go/consensus/hotstuff/model"
	"github.com/onflow/flow-go/engine/execution/computation/computer/uploader"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/utils/grpcutils"
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
}

func NewExecDataStreamer(log zerolog.Logger, accessAddr string, options ...Option) *ExecDataStreamer {
	cfg := DefaultConfig
	for _, option := range options {
		option(&cfg)
	}

	ctx := context.Background()

	// initialize clients
	opts := []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcutils.DefaultMaxMsgSize)),
		grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.Dial(accessAddr, opts...)
	if err != nil {
		panic(fmt.Sprintf("unable to create connection to node: %s", accessAddr))
	}
	execDataAPI := execData.NewExecutionDataAPIClient(conn)
	accessAPI := access.NewAccessAPIClient(conn)

	// get chainID from network params

	return &ExecDataStreamer{
		log:       log,
		execApi:   execDataAPI,
		accessApi: accessAPI,
		queue:     archive.NewDeque(),
		buffer:    archive.NewDeque(),
		limit:     cfg.BufferSize,
		busy:      0,
		ctx:       ctx,
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

		e.log.Debug().
			Str("name", blockID.String()).
			Uint64("height", record.Block.Header.Height).
			Hex("block", blockID[:]).
			Msg("pushing execution record into buffer")

		e.buffer.PushFront(record)
	}
}

func (e *ExecDataStreamer) getUploaderBlockData(blockID flow.Identifier) (*uploader.BlockData, error) {
	// TODO  get block from storage, as it's already populated by consensus follower

	// get transactions from AN
	txr := &access.GetTransactionsByBlockIDRequest{BlockId: blockID[:]}
	tx, err := e.accessApi.GetTransactionResultsByBlockID(e.ctx, txr)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction results for blockID (%s): %w", blockID, err)
	}
	// get exec data
	exr := &execData.GetExecutionDataByBlockIDRequest{BlockId: blockID[:]}
	ex, err := e.execApi.GetExecutionDataByBlockID(e.ctx, exr)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution data for blockID (%s): %w", blockID, err)
	}
	// TODO aggregate to *uploader.BlockData
	return &uploader.BlockData{}, nil
}
