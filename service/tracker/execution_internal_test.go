// Copyright 2021 Optakt Labs OÜ
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package tracker

import (
	"github.com/onflow/flow-archive/models/archive"
	"github.com/onflow/flow-go/module/executiondatasync/execution_data"
	"testing"

	"github.com/dgraph-io/badger/v2"
	"github.com/gammazero/deque"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/storage/badger/operation"

	"github.com/onflow/flow-archive/testing/helpers"
	"github.com/onflow/flow-archive/testing/mocks"
)

func TestNewExecution(t *testing.T) {
	header := mocks.GenericHeader
	blockID := header.ID()
	seal := mocks.GenericSeal(0)

	t.Run("nominal case", func(t *testing.T) {
		log := zerolog.Nop()
		stream := mocks.BaselineRecordStreamer(t)

		db := helpers.InMemoryDB(t)
		require.NoError(t, db.Update(operation.InsertRootHeight(header.Height)))
		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, blockID)))
		require.NoError(t, db.Update(operation.InsertHeader(blockID, header)))
		require.NoError(t, db.Update(operation.IndexLatestSealAtBlock(blockID, seal.ID())))
		require.NoError(t, db.Update(operation.InsertSeal(seal.ID(), seal)))

		exec, err := NewExecution(log, db, stream)

		require.NoError(t, err)
		assert.Equal(t, stream, exec.stream)
		assert.NotNil(t, exec.queue)
		assert.NotEmpty(t, exec.records)
	})

	t.Run("handles missing root height", func(t *testing.T) {
		log := zerolog.Nop()
		stream := mocks.BaselineRecordStreamer(t)

		db := helpers.InMemoryDB(t)
		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, blockID)))
		require.NoError(t, db.Update(operation.InsertHeader(blockID, header)))
		require.NoError(t, db.Update(operation.IndexLatestSealAtBlock(blockID, seal.ID())))
		require.NoError(t, db.Update(operation.InsertSeal(seal.ID(), seal)))

		_, err := NewExecution(log, db, stream)

		assert.Error(t, err)
	})

	t.Run("handles missing block height", func(t *testing.T) {
		log := zerolog.Nop()
		stream := mocks.BaselineRecordStreamer(t)

		db := helpers.InMemoryDB(t)
		require.NoError(t, db.Update(operation.InsertRootHeight(header.Height)))
		// Do not insert block height.
		//require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, blockID)))
		require.NoError(t, db.Update(operation.InsertHeader(blockID, header)))
		require.NoError(t, db.Update(operation.IndexLatestSealAtBlock(blockID, seal.ID())))
		require.NoError(t, db.Update(operation.InsertSeal(seal.ID(), seal)))

		_, err := NewExecution(log, db, stream)

		assert.Error(t, err)
	})

	t.Run("handles missing header", func(t *testing.T) {
		log := zerolog.Nop()
		stream := mocks.BaselineRecordStreamer(t)

		db := helpers.InMemoryDB(t)
		require.NoError(t, db.Update(operation.InsertRootHeight(header.Height)))
		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, blockID)))
		require.NoError(t, db.Update(operation.IndexLatestSealAtBlock(blockID, seal.ID())))
		require.NoError(t, db.Update(operation.InsertSeal(seal.ID(), seal)))

		_, err := NewExecution(log, db, stream)

		assert.Error(t, err)
	})

	t.Run("handles missing seal index", func(t *testing.T) {
		log := zerolog.Nop()
		stream := mocks.BaselineRecordStreamer(t)

		db := helpers.InMemoryDB(t)
		require.NoError(t, db.Update(operation.InsertRootHeight(header.Height)))
		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, blockID)))
		require.NoError(t, db.Update(operation.InsertHeader(blockID, header)))
		require.NoError(t, db.Update(operation.InsertSeal(seal.ID(), seal)))

		_, err := NewExecution(log, db, stream)

		assert.Error(t, err)
	})

	t.Run("handles missing seal", func(t *testing.T) {
		log := zerolog.Nop()
		stream := mocks.BaselineRecordStreamer(t)

		db := helpers.InMemoryDB(t)
		require.NoError(t, db.Update(operation.InsertRootHeight(header.Height)))
		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, blockID)))
		require.NoError(t, db.Update(operation.InsertHeader(blockID, header)))
		require.NoError(t, db.Update(operation.IndexLatestSealAtBlock(blockID, seal.ID())))

		_, err := NewExecution(log, db, stream)

		assert.Error(t, err)
	})
}

func TestExecution_Purge(t *testing.T) {
	blockIDs := mocks.GenericBlockIDs(4)
	blocks := []*execution_data.BlockExecutionData{
		{BlockID: blockIDs[0], ChunkExecutionDatas: nil},
		{BlockID: blockIDs[1], ChunkExecutionDatas: nil},
		{BlockID: blockIDs[2], ChunkExecutionDatas: nil},
		{BlockID: blockIDs[3], ChunkExecutionDatas: nil},
	}
	blockHeights := []uint64{4, 5, 6, 7}
	records := []*archive.BlockExecutionDataRecord{
		{ExecutionData: blocks[0], Height: blockHeights[0]},
		{ExecutionData: blocks[1], Height: blockHeights[1]},
		{ExecutionData: blocks[2], Height: blockHeights[2]},
		{ExecutionData: blocks[3], Height: blockHeights[3]},
	}

	tests := []struct {
		name      string
		threshold uint64
		before    map[flow.Identifier]*archive.BlockExecutionDataRecord
		after     map[flow.Identifier]*archive.BlockExecutionDataRecord
	}{
		{
			name: "threshold is at lowest height",

			threshold: blockHeights[0],
			before: map[flow.Identifier]*archive.BlockExecutionDataRecord{
				blockIDs[0]: records[0],
				blockIDs[1]: records[1],
				blockIDs[2]: records[2],
				blockIDs[3]: records[3],
			},
			after: map[flow.Identifier]*archive.BlockExecutionDataRecord{
				blockIDs[0]: records[0],
				blockIDs[1]: records[1],
				blockIDs[2]: records[2],
				blockIDs[3]: records[3],
			},
		},
		{
			name: "threshold is above highest height",

			threshold: blockHeights[3] + 1,
			before: map[flow.Identifier]*archive.BlockExecutionDataRecord{
				blockIDs[0]: records[0],
				blockIDs[1]: records[1],
				blockIDs[2]: records[2],
				blockIDs[3]: records[3],
			},
			after: map[flow.Identifier]*archive.BlockExecutionDataRecord{},
		},
		{
			name: "threshold is in-between",

			threshold: blockHeights[2],
			before: map[flow.Identifier]*archive.BlockExecutionDataRecord{
				blockIDs[0]: records[0],
				blockIDs[1]: records[1],
				blockIDs[2]: records[2],
				blockIDs[3]: records[3],
			},
			after: map[flow.Identifier]*archive.BlockExecutionDataRecord{
				blockIDs[2]: records[2],
				blockIDs[3]: records[3],
			},
		},
		{
			name: "does nothing when there is nothing to purge",

			threshold: blockHeights[2],
			before:    map[flow.Identifier]*archive.BlockExecutionDataRecord{},
			after:     map[flow.Identifier]*archive.BlockExecutionDataRecord{},
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			exec := BaselineExecution(t)
			exec.records = test.before
			exec.purge(test.threshold)
			assert.Len(t, exec.records, len(test.after))
			assert.Equal(t, test.after, exec.records)
		})
	}

}

func BaselineExecution(t *testing.T, opts ...func(*Execution)) *Execution {
	t.Helper()

	e := Execution{
		log:     zerolog.Nop(),
		queue:   deque.New(),
		stream:  mocks.BaselineRecordStreamer(t),
		records: make(map[flow.Identifier]*archive.BlockExecutionDataRecord),
	}

	for _, opt := range opts {
		opt(&e)
	}

	return &e
}

func WithStreamer(stream RecordStreamer) func(*Execution) {
	return func(execution *Execution) {
		execution.stream = stream
	}
}

func WithQueue(queue *deque.Deque) func(*Execution) {
	return func(execution *Execution) {
		execution.queue = queue
	}
}

func WithExecDB(db *badger.DB) func(*Execution) {
	return func(execution *Execution) {
		execution.db = db
	}
}
