package main

import (
	"context"
	"fmt"
	"io"
	"math"

	gcloud "cloud.google.com/go/storage"
	"github.com/fxamacker/cbor/v2"
	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/engine/execution/ingestion/uploader"
	"github.com/onflow/flow-go/ledger"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow/protobuf/go/flow/access"
	execData "github.com/onflow/flow/protobuf/go/flow/executiondata"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func CompareTrieUpdates(e []*ledger.TrieUpdate, g []*ledger.TrieUpdate) error {
	// compare lengths
	if len(e) != len(g) {
		return fmt.Errorf("unequal lengths")
	}
	// compare updates
	for idx, update := range e {
		if !update.Equals(g[idx]) {
			return fmt.Errorf("mismatching trie update at index %d", idx)
		}
	}
	return nil
}

func CompareCborRegistersWithExecutionResult(
	ctx context.Context,
	a access.AccessAPIClient,
	e execData.ExecutionDataAPIClient,
	b gcloud.BucketHandle,
	height uint64,
	decoder cbor.DecMode,
	chain flow.Chain,
) error {
	// get block and determine file name
	req := access.GetBlockByHeightRequest{Height: height, FullBlockResponse: true}
	res, err := a.GetBlockByHeight(ctx, &req)
	if err != nil {
		panic(fmt.Errorf("could not get block for height %d: %w", height, err))
	}
	block, err := convert.MessageToBlock(res.Block)
	if err != nil {
		panic(fmt.Errorf("could not convert message to block: %w", err))
	}
	fileName := block.ID().String() + ".cbor"
	print(fileName)
	// get cbor file and unmarhsall to block data
	object := b.Object(fileName)
	reader, err := object.NewReader(context.Background())
	if err != nil {
		panic(fmt.Errorf("could not create object reader: %w", err))
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		panic(fmt.Errorf("could not read execution record: %w", err))
	}

	var record uploader.BlockData
	err = decoder.Unmarshal(data, &record)
	if err != nil {
		panic(fmt.Errorf("could not decode execution record: %w", err))
	}

	// get execution result for the block to compare trieUpdates
	req2 := execData.GetExecutionDataByBlockIDRequest{BlockId: convert.IdentifierToMessage(block.ID())}
	res2, err := e.GetExecutionDataByBlockID(ctx, &req2)
	if err != nil {
		panic(fmt.Errorf("could not get execution data for height %d: %w", height, err))
	}
	extractedData, err := convert.MessageToBlockExecutionData(res2.BlockExecutionData, chain)
	// extract trie Updates
	trieUpdates := make([]*ledger.TrieUpdate, 0, 0)
	for _, chunk := range extractedData.ChunkExecutionDatas {
		trieUpdates = append(trieUpdates, chunk.TrieUpdate)
	}
	// compare
	return CompareTrieUpdates(trieUpdates, record.TrieUpdates)
}

func main() {
	accessAddr := "access-002.devnet46.nodes.onflow.org:9000"
	gbucketName := "flow_public_devnet46_execution_state"
	gClient, err := gcloud.NewClient(context.Background(),
		option.WithoutAuthentication(),
	)
	bucket := gClient.Bucket(gbucketName)

	decOptions := cbor.DecOptions{
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
		MaxArrayElements:  math.MaxInt64,
	}
	decoder, err := decOptions.DecMode()
	if err != nil {
		panic(fmt.Errorf("could not create decoder: %w", err))
	}
	defer func() {
		err := gClient.Close()
		if err != nil {

		}
	}()
	// initialize clients
	opts := []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(40 * 1024 * 1024)),
		grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn1, err := grpc.Dial(accessAddr, opts...)
	if err != nil {
		panic(err)
	}
	conn2, err := grpc.Dial(accessAddr, opts...)
	if err != nil {
		panic(err)
	}
	execDataAPI := execData.NewExecutionDataAPIClient(conn1)
	accessAPI := access.NewAccessAPIClient(conn2)
	// set chain
	chain := flow.Testnet.Chain()
	var start uint64 = 110950403
	var end uint64 = 110950404
	for height := start; height < end; height++ {
		CompareCborRegistersWithExecutionResult(context.Background(), accessAPI, execDataAPI, *bucket, height, decoder,
			chain)
	}
}
