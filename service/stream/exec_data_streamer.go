package stream

import (
	"fmt"

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
}

func NewExecDataStreamer(log zerolog.Logger, accessAddr string, options ...Option) *ExecDataStreamer {
	cfg := DefaultConfig
	for _, option := range options {
		option(&cfg)
	}

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
	go e.poll()
	if e.buffer.Len() == 0 {
		e.log.Debug().Msg("buffer empty, no execution record available")
		return nil, archive.ErrUnavailable
	}
	record := e.buffer.PopBack()
	return record.(*uploader.BlockData), nil
}

func (e *ExecDataStreamer) getUploaderBlockData(blockID *flow.Identifier) (*uploader.BlockData, error) {
	// get block

	// get transactions

	// get exec data

	// convert to *uploader.BlockData

}
