package datasync

import (
	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/model/flow"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/onflow/flow-archive/models/archive"
	"github.com/onflow/flow-archive/testing/mocks"
	"github.com/onflow/flow/protobuf/go/flow/entities"
	execData "github.com/onflow/flow/protobuf/go/flow/executiondata"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	var mockApiClient execData.ExecutionDataAPIClient = mocks.MockExecAPIClient{
		Response: &mockResponse,
	}

	streamer := NewExecDataSync(
		log,
		accessNode,
		mockApiClient,
		flow.Testnet.Chain(),
		WithBufferSize(limit),
		WithCatchupBlocks(blockIDs),
	)

	require.NotNil(t, streamer)
	assert.NotZero(t, streamer.log)
	assert.Equal(t, limit, streamer.limit)
	assert.NotNil(t, streamer.blocks)
	assert.NotNil(t, streamer.records)

	for streamer.blocks.Len() > 0 {
		assert.Contains(t, blockIDs, streamer.blocks.PopFront())
	}
}

func TestStreamer_OnBlockFinalized(t *testing.T) {
	block := mocks.GenericBlock
	queue := archive.NewDeque()

	streamer := &ExecDataSync{
		log:    zerolog.Nop(),
		blocks: queue,
	}

	streamer.OnBlockFinalized(block)

	require.Equal(t, 1, queue.Len())
	assert.Equal(t, queue.PopFront(), block.BlockID)
}

func TestStreamer_Next(t *testing.T) {
	record := mocks.GenericRecord()
	_, err := cbor.Marshal(record)
	require.NoError(t, err)

	bed, err := convert.BlockExecutionDataToMessage(record)
	require.NoError(t, err)

	mockResponse := execData.GetExecutionDataByBlockIDResponse{
		BlockExecutionData: bed,
	}

	var mockApiClient execData.ExecutionDataAPIClient = mocks.MockExecAPIClient{
		Response: &mockResponse,
	}

	t.Run("returns available record if buffer not empty", func(t *testing.T) {

		require.NoError(t, err)

		streamer := &ExecDataSync{
			log:         zerolog.Nop(),
			execDataApi: mockApiClient,
			blocks:      archive.NewDeque(),
			records:     archive.NewDeque(),
			limit:       999,
			chain:       flow.Testnet.Chain(),
		}

		streamer.records.PushFront(record)

		got, err := streamer.Next()

		require.NoError(t, err)
		assert.Equal(t, record, got)
	})

	t.Run("returns unavailable when no block data in buffer", func(t *testing.T) {
		streamer := &ExecDataSync{
			log:         zerolog.Nop(),
			execDataApi: mockApiClient,
			blocks:      archive.NewDeque(),
			records:     archive.NewDeque(),
			limit:       999,
		}

		_, err = streamer.Next()

		require.Error(t, err)
		assert.ErrorIs(t, err, archive.ErrUnavailable)
	})

	t.Run("gets exec data from queue if it is available", func(t *testing.T) {
		streamer := &ExecDataSync{
			log:         zerolog.Nop(),
			execDataApi: mockApiClient,
			blocks:      archive.NewDeque(),
			records:     archive.NewDeque(),
			limit:       999,
		}

		streamer.blocks.PushFront(record.BlockID)

		_, err = streamer.Next()

		require.Error(t, err)
		assert.ErrorIs(t, err, archive.ErrUnavailable)

		//time.Sleep(100 * time.Millisecond)
		//assert.Failf(t, "GCP Streamer did not attempt to download record from bucket", "asda")
	})
}
