//go:build integration
// +build integration

package archive_test

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/onflow/flow-go/model/flow"

	"github.com/onflow/flow-archive/api/archive"
	"github.com/onflow/flow-archive/codec/zbor"
	"github.com/onflow/flow-archive/models/convert"
	"github.com/onflow/flow-archive/service/index"
	"github.com/onflow/flow-archive/service/storage"
	"github.com/onflow/flow-archive/testing/helpers"
	"github.com/onflow/flow-archive/testing/mocks"
)

func TestIntegrationServer_GetFirst(t *testing.T) {
	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.First(mocks.GenericHeight))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetFirstRequest{}
		resp, err := server.GetFirst(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, mocks.GenericHeight, resp.Height)
	})

	t.Run("handles indexer failure on First", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetFirstRequest{}
		_, err := server.GetFirst(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetLast(t *testing.T) {

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Last(mocks.GenericHeight))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetLastRequest{}
		resp, err := server.GetLast(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, mocks.GenericHeight, resp.Height)
	})

	t.Run("handles indexer failure on Last", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetLastRequest{}
		_, err := server.GetLast(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetHeightForBlock(t *testing.T) {
	blockID := mocks.GenericHeader.ID()
	height := mocks.GenericHeader.Height

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Height(blockID, height))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetHeightForBlockRequest{
			BlockID: blockID[:],
		}
		resp, err := server.GetHeightForBlock(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, height, resp.Height)
	})

	t.Run("handles indexer failure on HeightForBlock", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetHeightForBlockRequest{
			BlockID: blockID[:],
		}
		_, err := server.GetHeightForBlock(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetCommit(t *testing.T) {
	commit := mocks.GenericCommit(0)
	height := mocks.GenericHeader.Height

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Commit(height, commit))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetCommitRequest{
			Height: height,
		}
		resp, err := server.GetCommit(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, height, resp.Height)
		assert.Equal(t, commit[:], resp.Commit)
	})

	t.Run("handles indexer failure on Commit", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetCommitRequest{
			Height: height,
		}
		_, err := server.GetCommit(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetHeader(t *testing.T) {
	header := mocks.GenericHeader
	height := mocks.GenericHeader.Height

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Header(height, header))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetHeaderRequest{
			Height: height,
		}
		resp, err := server.GetHeader(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, height, resp.Height)

		wantData, err := codec.Marshal(header)
		require.NoError(t, err)
		assert.Equal(t, wantData, resp.Data)
	})

	t.Run("handles indexer failure on Header", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetHeaderRequest{
			Height: height,
		}
		_, err := server.GetHeader(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetEvents(t *testing.T) {
	withdrawalType := mocks.GenericEventType(0)
	depositType := mocks.GenericEventType(1)
	withdrawals := mocks.GenericEvents(2, withdrawalType)
	deposits := mocks.GenericEvents(2, depositType)
	events := append(withdrawals, deposits...)

	height := mocks.GenericHeight

	t.Run("nominal case without type specified", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.First(height))
		require.NoError(t, writer.Last(height))
		require.NoError(t, writer.Events(height, events))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetEventsRequest{
			Height: height,
		}
		resp, err := server.GetEvents(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, height, resp.Height)

		var got []flow.Event
		require.NoError(t, codec.Unmarshal(resp.Data, &got))
		assert.ElementsMatch(t, events, got)
	})

	t.Run("nominal case with type specified", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.First(height))
		require.NoError(t, writer.Last(height))
		require.NoError(t, writer.Events(height, events))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetEventsRequest{
			Types:  []string{string(withdrawalType)},
			Height: height,
		}
		resp, err := server.GetEvents(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, height, resp.Height)

		var got []flow.Event
		require.NoError(t, codec.Unmarshal(resp.Data, &got))
		assert.ElementsMatch(t, withdrawals, got)
	})

	t.Run("handles indexer failure on Events", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetEventsRequest{
			Height: height,
		}
		_, err := server.GetEvents(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetRegisterValues(t *testing.T) {
	paths := mocks.GenericLedgerPaths(4)
	payloads := mocks.GenericLedgerPayloads(4)
	height := mocks.GenericHeight

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.First(height))
		require.NoError(t, writer.Last(height))
		require.NoError(t, writer.Payloads(height, paths, payloads))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetRegisterValuesRequest{
			Height: height,
			Paths:  convert.PathsToBytes(paths),
		}
		resp, err := server.GetRegisterValues(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, height, resp.Height)

		assert.Len(t, resp.Values, len(paths))
		for _, payload := range payloads {
			assert.Contains(t, resp.Values, []byte(payload.Value()))
		}
		assert.Equal(t, convert.PathsToBytes(paths), resp.Paths)
	})

	t.Run("handles conversion error", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.First(height))
		require.NoError(t, writer.Last(height))
		require.NoError(t, writer.Payloads(height, paths, payloads))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetRegisterValuesRequest{
			Height: height,
			Paths:  [][]byte{mocks.GenericBytes},
		}
		_, err := server.GetRegisterValues(context.Background(), req)

		assert.Error(t, err)
	})

	t.Run("handles indexer failure on Values", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)
		server := archive.NewServer(reader, codec)

		req := &archive.GetRegisterValuesRequest{
			Height: height,
			Paths:  [][]byte{mocks.GenericBytes},
		}
		_, err := server.GetRegisterValues(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetCollection(t *testing.T) {
	collections := mocks.GenericCollections(4)
	collID := collections[0].ID()

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Collections(mocks.GenericHeight, collections))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetCollectionRequest{
			CollectionID: collID[:],
		}
		resp, err := server.GetCollection(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, collID[:], resp.CollectionID)

		wantData, err := codec.Marshal(collections[0])
		require.NoError(t, err)
		assert.Equal(t, wantData, resp.Data)
	})

	t.Run("handles indexer failure on Collection", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetCollectionRequest{
			CollectionID: collID[:],
		}
		_, err := server.GetCollection(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_ListCollectionsForHeight(t *testing.T) {
	collections := mocks.GenericCollections(4)
	height := mocks.GenericHeight

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Collections(height, collections))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.ListCollectionsForHeightRequest{
			Height: mocks.GenericHeight,
		}
		resp, err := server.ListCollectionsForHeight(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, mocks.GenericHeight, resp.Height)

		assert.Len(t, resp.CollectionIDs, len(collections))
		for _, collection := range collections {
			wantID := collection.ID()
			assert.Contains(t, resp.CollectionIDs, wantID[:])
		}
	})

	t.Run("handles indexer failure on CollectionsByHeight", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.ListCollectionsForHeightRequest{
			Height: mocks.GenericHeight,
		}
		_, err := server.ListCollectionsForHeight(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetGuarantee(t *testing.T) {
	guarantees := mocks.GenericGuarantees(4)
	collID := guarantees[0].ID()

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Guarantees(mocks.GenericHeight, guarantees))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetGuaranteeRequest{
			CollectionID: collID[:],
		}
		resp, err := server.GetGuarantee(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, collID[:], resp.CollectionID)

		wantData, err := codec.Marshal(guarantees[0])
		require.NoError(t, err)
		assert.Equal(t, wantData, resp.Data)
	})

	t.Run("handles indexer failure on Guarantee", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetGuaranteeRequest{
			CollectionID: collID[:],
		}
		_, err := server.GetGuarantee(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetTransaction(t *testing.T) {
	transactions := mocks.GenericTransactions(4)
	txID := transactions[0].ID()

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Transactions(mocks.GenericHeight, transactions))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetTransactionRequest{
			TransactionID: txID[:],
		}
		resp, err := server.GetTransaction(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, txID[:], resp.TransactionID)

		wantData, err := codec.Marshal(transactions[0])
		require.NoError(t, err)
		assert.Equal(t, wantData, resp.Data)
	})

	t.Run("handles indexer failure on Transaction", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetTransactionRequest{
			TransactionID: txID[:],
		}
		_, err := server.GetTransaction(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetHeightForTransaction(t *testing.T) {
	transactions := mocks.GenericTransactions(4)
	txID := transactions[0].ID()

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Transactions(mocks.GenericHeight, transactions))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetHeightForTransactionRequest{
			TransactionID: txID[:],
		}
		resp, err := server.GetHeightForTransaction(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, txID[:], resp.TransactionID)
		assert.Equal(t, mocks.GenericHeight, resp.Height)
	})

	t.Run("handles indexer failure on HeightForTransaction", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetHeightForTransactionRequest{
			TransactionID: txID[:],
		}
		_, err := server.GetHeightForTransaction(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_ListTransactionsForHeight(t *testing.T) {
	transactions := mocks.GenericTransactions(4)
	height := mocks.GenericHeight

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Transactions(height, transactions))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.ListTransactionsForHeightRequest{
			Height: mocks.GenericHeight,
		}
		resp, err := server.ListTransactionsForHeight(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, mocks.GenericHeight, resp.Height)

		assert.Len(t, resp.TransactionIDs, len(transactions))
		for _, tx := range transactions {
			wantID := tx.ID()
			assert.Contains(t, resp.TransactionIDs, wantID[:])
		}
	})

	t.Run("handles indexer failure on TransactionsByHeight", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.ListTransactionsForHeightRequest{
			Height: mocks.GenericHeight,
		}
		_, err := server.ListTransactionsForHeight(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetResult(t *testing.T) {
	results := mocks.GenericResults(4)
	txID := results[0].TransactionID

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Results(results))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetResultRequest{
			TransactionID: txID[:],
		}
		resp, err := server.GetResult(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, txID[:], resp.TransactionID)

		wantData, err := codec.Marshal(results[0])
		require.NoError(t, err)
		assert.Equal(t, wantData, resp.Data)
	})

	t.Run("handles indexer failure on Result", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetResultRequest{
			TransactionID: txID[:],
		}
		_, err := server.GetResult(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_GetSeal(t *testing.T) {
	seals := mocks.GenericSeals(4)
	sealID := seals[0].ID()

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Seals(mocks.GenericHeight, seals))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.GetSealRequest{
			SealID: sealID[:],
		}
		resp, err := server.GetSeal(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, sealID[:], resp.SealID)

		wantData, err := codec.Marshal(seals[0])
		require.NoError(t, err)
		assert.Equal(t, wantData, resp.Data)
	})

	t.Run("handles indexer failure on Seal", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.GetSealRequest{
			SealID: sealID[:],
		}
		_, err := server.GetSeal(context.Background(), req)

		assert.Error(t, err)
	})
}

func TestIntegrationServer_ListSealsForHeight(t *testing.T) {
	seals := mocks.GenericSeals(4)
	height := mocks.GenericHeight

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		reader := index.NewReader(log, db, disk)
		writer := index.NewWriter(db, disk)

		// Insert mock data in database.
		require.NoError(t, writer.Seals(height, seals))
		require.NoError(t, writer.Last(height))
		require.NoError(t, writer.Close())

		server := archive.NewServer(reader, codec)

		req := &archive.ListSealsForHeightRequest{
			Height: mocks.GenericHeight,
		}
		resp, err := server.ListSealsForHeight(context.Background(), req)

		require.NoError(t, err)
		assert.Equal(t, mocks.GenericHeight, resp.Height)

		assert.Len(t, resp.SealIDs, len(seals))
		for _, seal := range seals {
			wantID := seal.ID()
			assert.Contains(t, resp.SealIDs, wantID[:])
		}
	})

	t.Run("handles indexer failure on SealsByHeight", func(t *testing.T) {
		t.Parallel()

		log := zerolog.Nop()
		codec := zbor.NewCodec()

		db := helpers.InMemoryDB(t)
		defer db.Close()

		disk := storage.New(codec)
		// No data is written in the database, so the index should fail to retrieve anything.
		reader := index.NewReader(log, db, disk)

		server := archive.NewServer(reader, codec)

		req := &archive.ListSealsForHeightRequest{
			Height: mocks.GenericHeight,
		}
		_, err := server.ListSealsForHeight(context.Background(), req)

		assert.Error(t, err)
	})
}
