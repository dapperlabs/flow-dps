package mocks

import (
	"context"

	"google.golang.org/grpc"

	execData "github.com/onflow/flow/protobuf/go/flow/executiondata"
)

type MockExecAPIClient struct {
	Response *execData.GetExecutionDataByBlockIDResponse
}

func (m MockExecAPIClient) GetExecutionDataByBlockID(_ context.Context, _ *execData.GetExecutionDataByBlockIDRequest, _ ...grpc.CallOption) (*execData.GetExecutionDataByBlockIDResponse, error) {
	return m.Response, nil
}
