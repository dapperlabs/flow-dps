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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/onflow/flow-archive/service/tracker"
	"github.com/onflow/flow-archive/testing/helpers"
	"github.com/onflow/flow-archive/testing/mocks"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/storage/badger/operation"
)

func TestConsensus_Root(t *testing.T) {
	header := mocks.GenericHeader

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.InsertRootHeight(header.Height)))

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db))

		got, err := cons.Root()

		require.NoError(t, err)
		assert.Equal(t, header.Height, got)
	})

	t.Run("handles missing root height", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db))

		_, err := cons.Root()

		assert.Error(t, err)
	})
}

func TestConsensus_Header(t *testing.T) {
	header := mocks.GenericHeader

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
		require.NoError(t, db.Update(operation.InsertHeader(header.ID(), header)))

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		got, err := cons.Header(header.Height)

		require.NoError(t, err)
		assert.Equal(t, header, got)
	})

	t.Run("handles requested height over last finalized height", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
		require.NoError(t, db.Update(operation.InsertHeader(header.ID(), header)))

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		_, err := cons.Header(header.Height + 1)

		assert.Error(t, err)
	})

	t.Run("handles missing block height index in DB", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.InsertHeader(header.ID(), header)))

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		_, err := cons.Header(header.Height)

		assert.Error(t, err)
	})

	t.Run("handles missing header in DB", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		_, err := cons.Header(header.Height)

		assert.Error(t, err)
	})
}

func TestConsensus_Guarantees(t *testing.T) {
	header := mocks.GenericHeader
	guarantees := mocks.GenericGuarantees(4)

	var collectionIDs []flow.Identifier
	for _, guarantee := range guarantees {
		collectionIDs = append(collectionIDs, guarantee.CollectionID)
	}

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
		require.NoError(t, db.Update(operation.IndexPayloadGuarantees(header.ID(), collectionIDs)))
		for _, guarantee := range guarantees {
			require.NoError(t, db.Update(operation.InsertGuarantee(guarantee.CollectionID, guarantee)))
		}

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		got, err := cons.Guarantees(header.Height)

		require.NoError(t, err)
		assert.Equal(t, guarantees, got)
	})

	t.Run("handles requested height over last finalized height", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
		require.NoError(t, db.Update(operation.IndexPayloadGuarantees(header.ID(), collectionIDs)))
		for _, guarantee := range guarantees {
			require.NoError(t, db.Update(operation.InsertGuarantee(guarantee.CollectionID, guarantee)))
		}

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		_, err := cons.Guarantees(header.Height + 999)

		assert.Error(t, err)
	})

	t.Run("handles missing block height index in DB", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexPayloadGuarantees(header.ID(), collectionIDs)))
		for _, guarantee := range guarantees {
			require.NoError(t, db.Update(operation.InsertGuarantee(guarantee.CollectionID, guarantee)))
		}

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		_, err := cons.Guarantees(header.Height)

		assert.Error(t, err)
	})

	t.Run("handles missing guarantee IDs in DB", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
		for _, guarantee := range guarantees {
			require.NoError(t, db.Update(operation.InsertGuarantee(guarantee.CollectionID, guarantee)))
		}

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		_, err := cons.Guarantees(header.Height)

		assert.Error(t, err)
	})

	t.Run("handles missing guarantees in DB", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
		require.NoError(t, db.Update(operation.IndexPayloadGuarantees(header.ID(), collectionIDs)))

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		_, err := cons.Guarantees(header.Height)

		assert.Error(t, err)
	})
}

func TestConsensus_Seals(t *testing.T) {
	header := mocks.GenericHeader
	seals := mocks.GenericSeals(4)
	sealIDs := mocks.GenericSealIDs(4)

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
		require.NoError(t, db.Update(operation.IndexPayloadSeals(header.ID(), sealIDs)))
		for _, seal := range seals {
			require.NoError(t, db.Update(operation.InsertSeal(seal.ID(), seal)))
		}

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		got, err := cons.Seals(header.Height)

		require.NoError(t, err)
		assert.Equal(t, seals, got)
	})

	t.Run("handles requested height over last finalized height", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
		require.NoError(t, db.Update(operation.IndexPayloadSeals(header.ID(), sealIDs)))
		for _, seal := range seals {
			require.NoError(t, db.Update(operation.InsertSeal(seal.ID(), seal)))
		}

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		_, err := cons.Seals(header.Height + 999)

		assert.Error(t, err)
	})

	t.Run("handles missing block height index in DB", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexPayloadSeals(header.ID(), sealIDs)))
		for _, seal := range seals {
			require.NoError(t, db.Update(operation.InsertSeal(seal.ID(), seal)))
		}

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		_, err := cons.Seals(header.Height)

		assert.Error(t, err)
	})

	t.Run("handles missing seal index in DB", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
		for _, seal := range seals {
			require.NoError(t, db.Update(operation.InsertSeal(seal.ID(), seal)))
		}

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height))

		_, err := cons.Seals(header.Height)

		assert.Error(t, err)
	})

	t.Run("handles missing seals in DB", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
		require.NoError(t, db.Update(operation.IndexPayloadSeals(header.ID(), sealIDs)))

		cons := tracker.BaselineConsensus(t, tracker.WithDB(db), tracker.WithLast(header.Height), tracker.WithLast(header.Height))

		_, err := cons.Seals(header.Height)

		assert.Error(t, err)
	})
}

func TestConsensus_Collections(t *testing.T) {
	header := mocks.GenericHeader
	record := mocks.GenericRecord()

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, record.BlockID)))
		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(blockID flow.Identifier) (*execution_data.BlockExecutionData, error) {
			assert.Equal(t, header.ID(), blockID)

			return record, nil
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		got, err := cons.Collections(header.Height, flow.Testnet.Chain())
		require.NoError(t, err)
		require.Len(t, got, len(record.ChunkExecutionDatas))
		for i, chunk := range record.ChunkExecutionDatas {
			for _, tx := range chunk.Collection.Transactions {
				assert.Contains(t, got[i].Transactions, tx.ID())
			}
		}
	})

	t.Run("handles requested height over last finalized height", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))

		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(flow.Identifier) (*execution_data.BlockExecutionData, error) {
			return record, nil
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		_, err := cons.Collections(header.Height+999, flow.Testnet.Chain())

		assert.Error(t, err)
	})

	t.Run("handles missing block height index in DB", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(flow.Identifier) (*execution_data.BlockExecutionData, error) {
			return record, nil
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		_, err := cons.Collections(header.Height, flow.Testnet.Chain())

		assert.Error(t, err)
	})

	t.Run("handles record holder failure", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))

		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(flow.Identifier) (*execution_data.BlockExecutionData, error) {
			return nil, mocks.GenericError
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		_, err := cons.Collections(header.Height, flow.Testnet.Chain())

		assert.Error(t, err)
	})
}

func TestConsensus_Transactions(t *testing.T) {
	header := mocks.GenericHeader
	record := mocks.GenericRecord()

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))

		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(blockID flow.Identifier) (*execution_data.BlockExecutionData, error) {
			assert.Equal(t, header.ID(), blockID)

			return record, nil
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		got, err := cons.Transactions(header.Height, flow.Testnet.Chain())

		require.NoError(t, err)
		for _, chunk := range record.ChunkExecutionDatas {
			for _, tx := range chunk.Collection.Transactions {
				assert.Contains(t, got, tx)
			}
		}
	})

	t.Run("handles requested height over last finalized height", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))

		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(flow.Identifier) (*execution_data.BlockExecutionData, error) {
			return record, nil
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		_, err := cons.Transactions(header.Height+999, flow.Testnet.Chain())

		assert.Error(t, err)
	})

	t.Run("handles missing block height index in DB", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(flow.Identifier) (*execution_data.BlockExecutionData, error) {
			return record, nil
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		_, err := cons.Transactions(header.Height, flow.Testnet.Chain())

		assert.Error(t, err)
	})

	t.Run("handles record holder failure", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))

		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(flow.Identifier) (*execution_data.BlockExecutionData, error) {
			return nil, mocks.GenericError
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		_, err := cons.Transactions(header.Height, flow.Testnet.Chain())

		assert.Error(t, err)
	})
}

// TODO: Re-add when transaction results are available from execution data
//func TestConsensus_Results(t *testing.T) {
//	header := mocks.GenericHeader
//	record := mocks.GenericRecord()
//
//	t.Run("nominal case", func(t *testing.T) {
//		t.Parallel()
//
//		db := helpers.InMemoryDB(t)
//		defer db.Close()
//
//		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
//
//		holder := mocks.BaselineRecordHolder(t)
//		holder.RecordFunc = func(blockID flow.Identifier) (*entities.BlockExecutionData, error) {
//			assert.Equal(t, header.ID(), blockID)
//
//			return record, 0, nil
//		}
//
//		cons := tracker.BaselineConsensus(
//			t,
//			tracker.WithDB(db),
//			tracker.WithLast(header.Height),
//			tracker.WithHolder(holder),
//		)
//
//		got, err := cons.Results(header.Height)
//
//		require.NoError(t, err)
//		assert.Equal(t, record.TxResults, got)
//	})
//
//	t.Run("handles requested height over last finalized height", func(t *testing.T) {
//		t.Parallel()
//
//		db := helpers.InMemoryDB(t)
//		defer db.Close()
//
//		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
//
//		holder := mocks.BaselineRecordHolder(t)
//		holder.RecordFunc = func(flow.Identifier) (*entities.BlockExecutionData, error) {
//			return record, 0, nil
//		}
//
//		cons := tracker.BaselineConsensus(
//			t,
//			tracker.WithDB(db),
//			tracker.WithLast(header.Height),
//			tracker.WithHolder(holder),
//		)
//
//		_, err := cons.Results(header.Height + 999)
//
//		assert.Error(t, err)
//	})
//
//	t.Run("handles missing block height index in DB", func(t *testing.T) {
//		t.Parallel()
//
//		db := helpers.InMemoryDB(t)
//		defer db.Close()
//
//		holder := mocks.BaselineRecordHolder(t)
//		holder.RecordFunc = func(flow.Identifier) (*entities.BlockExecutionData, error) {
//			return record, 0, nil
//		}
//
//		cons := tracker.BaselineConsensus(
//			t,
//			tracker.WithDB(db),
//			tracker.WithLast(header.Height),
//			tracker.WithHolder(holder),
//		)
//
//		_, err := cons.Results(header.Height)
//
//		assert.Error(t, err)
//	})
//
//	t.Run("handles record holder failure", func(t *testing.T) {
//		t.Parallel()
//
//		db := helpers.InMemoryDB(t)
//		defer db.Close()
//
//		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))
//
//		holder := mocks.BaselineRecordHolder(t)
//		holder.RecordFunc = func(flow.Identifier) (*entities.BlockExecutionData, error) {
//			return nil, 0, mocks.GenericError
//		}
//
//		cons := tracker.BaselineConsensus(
//			t,
//			tracker.WithDB(db),
//			tracker.WithLast(header.Height),
//			tracker.WithHolder(holder),
//		)
//
//		_, err := cons.Results(header.Height)
//
//		assert.Error(t, err)
//	})
//}

func TestConsensus_Events(t *testing.T) {
	header := mocks.GenericHeader
	record := mocks.GenericRecord()

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))

		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(blockID flow.Identifier) (*execution_data.BlockExecutionData, error) {
			assert.Equal(t, header.ID(), blockID)

			return record, nil
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		got, err := cons.Events(header.Height)

		require.NoError(t, err)
		eventLen := 0
		for _, chunk := range record.ChunkExecutionDatas {
			eventLen += len(chunk.Events)
		}
		assert.Len(t, got, eventLen)
		for _, chunk := range record.ChunkExecutionDatas {
			for _, event := range chunk.Events {
				assert.Contains(t, got, event)
			}
		}
	})

	t.Run("handles requested height over last finalized height", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))

		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(flow.Identifier) (*execution_data.BlockExecutionData, error) {
			return record, nil
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		_, err := cons.Events(header.Height + 999)

		assert.Error(t, err)
	})

	t.Run("handles missing block height index in DB", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(flow.Identifier) (*execution_data.BlockExecutionData, error) {
			return record, nil
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		_, err := cons.Events(header.Height)

		assert.Error(t, err)
	})

	t.Run("handles record holder failure", func(t *testing.T) {
		t.Parallel()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		require.NoError(t, db.Update(operation.IndexBlockHeight(header.Height, header.ID())))

		holder := mocks.BaselineRecordHolder(t)
		holder.RecordFunc = func(flow.Identifier) (*execution_data.BlockExecutionData, error) {
			return nil, mocks.GenericError
		}

		cons := tracker.BaselineConsensus(
			t,
			tracker.WithDB(db),
			tracker.WithLast(header.Height),
			tracker.WithHolder(holder),
		)

		_, err := cons.Events(header.Height)

		assert.Error(t, err)
	})
}
