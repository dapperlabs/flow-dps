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

package tracker_test

import (
	"github.com/onflow/flow-go/module/executiondatasync/execution_data"
	"testing"

	"github.com/gammazero/deque"
	"github.com/onflow/flow-go/storage/badger/operation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/onflow/flow-archive/service/tracker"
	"github.com/onflow/flow-archive/testing/helpers"
	"github.com/onflow/flow-archive/testing/mocks"
)

func TestExecution_Update(t *testing.T) {
	record := mocks.GenericRecord()
	update := mocks.GenericTrieUpdate(5)
	header := mocks.GenericHeader

	t.Run("nominal case with nothing in queue", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		streamer := mocks.BaselineRecordStreamer(t)
		streamer.NextFunc = func() (*execution_data.BlockExecutionData, error) {
			return record, nil
		}

		require.NoError(t, db.Update(operation.InsertHeader(record.BlockID, header)))
		exec := tracker.BaselineExecution(t, tracker.WithStreamer(streamer), tracker.WithExecDB(db))
		got, err := exec.Update()

		require.NoError(t, err)
		trieUpdate := record.ChunkExecutionDatas[0].TrieUpdate

		assert.Equal(t, trieUpdate, got)
	})

	t.Run("nominal case with queue already filled", func(t *testing.T) {
		t.Parallel()

		streamer := mocks.BaselineRecordStreamer(t)
		streamer.NextFunc = func() (*execution_data.BlockExecutionData, error) {
			t.Fatal("unexpected call to streamer.Next()") // This should never be called, since the queue is already filled.
			return record, nil
		}

		queue := deque.New()
		queue.PushBack(update)

		exec := tracker.BaselineExecution(
			t,
			tracker.WithQueue(queue),
			tracker.WithStreamer(streamer),
		)

		got, err := exec.Update()

		require.NoError(t, err)
		assert.Equal(t, update, got)
	})

	t.Run("handles streamer failure on Next", func(t *testing.T) {
		t.Parallel()

		streamer := mocks.BaselineRecordStreamer(t)
		streamer.NextFunc = func() (*execution_data.BlockExecutionData, error) {
			return nil, mocks.GenericError
		}

		exec := tracker.BaselineExecution(t, tracker.WithStreamer(streamer))

		_, err := exec.Update()

		assert.Error(t, err)
	})

	t.Run("handles duplicate records", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		// Only keep one trie update per block to make each update call go through one block.
		smallBlock := mocks.GenericRecord()
		smallBlock.ChunkExecutionDatas = smallBlock.ChunkExecutionDatas[:1]

		require.NoError(t, db.Update(operation.InsertHeader(record.BlockID, header)))

		streamer := mocks.BaselineRecordStreamer(t)
		streamer.NextFunc = func() (*execution_data.BlockExecutionData, error) {
			return smallBlock, nil
		}

		exec := tracker.BaselineExecution(t, tracker.WithStreamer(streamer), tracker.WithExecDB(db))

		// The first call loads our "small block" with only one trie update and consumes it.
		_, err := exec.Update()

		assert.NoError(t, err)

		// The next call loads the same block, realizes something is wrong and returns an error.
		_, err = exec.Update()

		assert.Error(t, err)
	})
}

func TestExecution_Record(t *testing.T) {
	record := mocks.GenericRecord()
	header := mocks.GenericHeader

	t.Run("nominal case with nothing in queue", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		streamer := mocks.BaselineRecordStreamer(t)
		streamer.NextFunc = func() (*execution_data.BlockExecutionData, error) {
			return record, nil
		}

		require.NoError(t, db.Update(operation.InsertHeader(record.BlockID, header)))

		exec := tracker.BaselineExecution(t, tracker.WithStreamer(streamer), tracker.WithExecDB(db))
		got, err := exec.Record(record.BlockID)

		require.NoError(t, err)
		assert.Equal(t, record, got)
	})

	t.Run("handles streamer failure on Next", func(t *testing.T) {
		t.Parallel()

		streamer := mocks.BaselineRecordStreamer(t)
		streamer.NextFunc = func() (*execution_data.BlockExecutionData, error) {
			return nil, mocks.GenericError
		}

		exec := tracker.BaselineExecution(t, tracker.WithStreamer(streamer))

		_, err := exec.Record(record.BlockID)

		assert.Error(t, err)
	})
}
