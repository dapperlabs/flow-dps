package datasync

import (
	"context"
	"github.com/fxamacker/cbor/v2"
	"github.com/onflow/flow-archive/models/archive"
	"github.com/onflow/flow-archive/testing/mocks"
	"github.com/onflow/flow/protobuf/go/flow/entities"
	execData "github.com/onflow/flow/protobuf/go/flow/executiondata"
	"github.com/optakt/flow-dps/models/dps"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"testing"
)

func TestNewExecDataSync(t *testing.T) {
	log := zerolog.Nop()
	accessNode := "mock.access-node.url"
	limit := uint(42)
	blockIDs := mocks.GenericBlockIDs(4)

	bed := entities.BlockExecutionData{
		BlockId:            nil,
		ChunkExecutionData: nil,
	}
	mockResponse := execData.GetExecutionDataByBlockIDResponse{
		BlockExecutionData: &bed,
	}

	var mockApiClient execData.ExecutionDataAPIClient
	mockApiClient = MockExecAPIClient{
		response: &mockResponse,
	}

	streamer := NewExecDataSync(
		log,
		accessNode,
		mockApiClient,
		WithBufferSize(limit),
		WithCatchupBlocks(blockIDs),
	)

	require.NotNil(t, streamer)
	assert.NotZero(t, streamer.log)
	assert.Equal(t, limit, streamer.limit)
	assert.NotNil(t, streamer.queue)
	assert.NotNil(t, streamer.buffer)

	for streamer.queue.Len() > 0 {
		assert.Contains(t, blockIDs, streamer.queue.PopFront())
	}
}

func TestGCPStreamer_OnBlockFinalized(t *testing.T) {
	block := mocks.GenericBlock
	queue := archive.NewDeque()

	streamer := &ExecDataSync{
		log:   zerolog.Nop(),
		queue: queue,
	}

	streamer.OnBlockFinalized(block)

	require.Equal(t, 1, queue.Len())
	assert.Equal(t, queue.PopFront(), block.BlockID)
}

func TestGCPStreamer_Next(t *testing.T) {
	record := mocks.GenericRecord()
	_, err := cbor.Marshal(record)
	require.NoError(t, err)

	decOptions := cbor.DecOptions{ExtraReturnErrors: cbor.ExtraDecErrorUnknownField}
	decoder, err := decOptions.DecMode()
	require.NoError(t, err)

	bed := entities.BlockExecutionData{
		BlockId:            nil,
		ChunkExecutionData: nil,
	}
	mockResponse := execData.GetExecutionDataByBlockIDResponse{
		BlockExecutionData: &bed,
	}

	var mockApiClient execData.ExecutionDataAPIClient
	mockApiClient = MockExecAPIClient{
		response: &mockResponse,
	}

	t.Run("returns available record if buffer not empty", func(t *testing.T) {

		require.NoError(t, err)

		streamer := &ExecDataSync{
			log:         zerolog.Nop(),
			decoder:     decoder,
			execDataApi: mockApiClient,
			queue:       archive.NewDeque(),
			buffer:      archive.NewDeque(),
			limit:       999,
		}

		streamer.buffer.PushFront(record)

		got, err := streamer.Next()

		require.NoError(t, err)
		assert.Equal(t, record, got)
	})

	t.Run("returns unavailable when no block data in buffer", func(t *testing.T) {
		streamer := &ExecDataSync{
			log:         zerolog.Nop(),
			decoder:     decoder,
			execDataApi: mockApiClient,
			queue:       archive.NewDeque(),
			buffer:      archive.NewDeque(),
			limit:       999,
		}

		_, err = streamer.Next()

		require.Error(t, err)
		assert.ErrorIs(t, err, dps.ErrUnavailable)
	})

	t.Run("gets exec data from queue if it is available", func(t *testing.T) {
		streamer := &ExecDataSync{
			log:         zerolog.Nop(),
			decoder:     decoder,
			execDataApi: mockApiClient,
			queue:       archive.NewDeque(),
			buffer:      archive.NewDeque(),
			limit:       999,
		}

		streamer.queue.PushFront(record.Block.ID())

		_, err = streamer.Next()

		require.Error(t, err)
		assert.ErrorIs(t, err, dps.ErrUnavailable)

		//select {
		//case <-time.After(100 * time.Millisecond):
		//	t.Fatal("GCP Streamer did not attempt to download record from bucket")
		//}
	})
}

// TODO: Replace this with Mockery
type MockExecAPIClient struct {
	response *execData.GetExecutionDataByBlockIDResponse
}

func (m MockExecAPIClient) GetExecutionDataByBlockID(_ context.Context, _ *execData.GetExecutionDataByBlockIDRequest, _ ...grpc.CallOption) (*execData.GetExecutionDataByBlockIDResponse, error) {
	return m.response, nil
}

func (m MockExecAPIClient) SetMockResponse(execData execData.GetExecutionDataByBlockIDResponse) {
	m.response = &execData
}
