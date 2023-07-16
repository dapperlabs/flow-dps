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
	"github.com/onflow/flow/protobuf/go/flow/access"
	"google.golang.org/api/option"
)

func CompareTrieUpdates(a []*ledger.TrieUpdate, b []*ledger.TrieUpdate) error {

}

func CompareCborRegistersWithExecutionResult(
	ctx context.Context,
	a access.AccessAPIClient,
	e access.,
	b *gcloud.BucketHandle,
	height uint64,
	decoder cbor.DecMode,
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
	req2 := access.GetExecutionResultForBlockIDRequest{BlockId: convert.IdentifierToMessage(block.ID())}
	res2, err := a.GetExecutionResultForBlockID(ctx, &req2)
	if err != nil {
		panic(fmt.Errorf("could not get execution result for height %d: %w", height, err))
	}
	executionResult, err := convert.MessageToExecutionResult(res2.ExecutionResult)
	if err != nil {

	}

	err = CompareTrieUpdates()
}

func main() {
	gbucketName := ""
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
	CompareCborRegistersWithExecutionResult(context.Background())
}
