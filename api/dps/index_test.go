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

package dps

import (
	"context"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/onflow/flow-go/ledger"
	"github.com/onflow/flow-go/model/flow"

	"github.com/optakt/flow-dps/models/convert"
	"github.com/optakt/flow-dps/models/dps"
	"github.com/optakt/flow-dps/testing/mocks"
)

func TestIndexFromAPI(t *testing.T) {
	mock := &apiMock{}
	codec := &mocks.Codec{}

	index := IndexFromAPI(mock, codec)

	if assert.NotNil(t, index) {
		assert.Equal(t, mock, index.client)
		assert.NotNil(t, mock, index.codec)
	}
}

func TestIndex_First(t *testing.T) {
	testHeight := uint64(42)

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetFirstFunc: func(_ context.Context, in *GetFirstRequest, _ ...grpc.CallOption) (*GetFirstResponse, error) {
					assert.NotNil(t, in)

					return &GetFirstResponse{
						Height: testHeight,
					}, nil
				},
			},
		}

		got, err := index.First()

		if assert.NoError(t, err) {
			assert.Equal(t, testHeight, got)
		}
	})

	t.Run("handles index failure", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetFirstFunc: func(_ context.Context, in *GetFirstRequest, _ ...grpc.CallOption) (*GetFirstResponse, error) {
					assert.NotNil(t, in)

					return nil, mocks.DummyError
				},
			},
		}

		_, err := index.First()
		assert.Error(t, err)
	})
}

func TestIndex_Last(t *testing.T) {
	testHeight := uint64(42)

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetLastFunc: func(_ context.Context, in *GetLastRequest, _ ...grpc.CallOption) (*GetLastResponse, error) {
					assert.NotNil(t, in)

					return &GetLastResponse{
						Height: testHeight,
					}, nil
				},
			},
		}

		got, err := index.Last()

		if assert.NoError(t, err) {
			assert.Equal(t, testHeight, got)
		}
	})

	t.Run("handles index failure", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetLastFunc: func(_ context.Context, in *GetLastRequest, _ ...grpc.CallOption) (*GetLastResponse, error) {
					assert.NotNil(t, in)

					return nil, mocks.DummyError
				},
			},
		}

		_, err := index.Last()
		assert.Error(t, err)
	})

}

func TestIndex_Header(t *testing.T) {
	testHeader := &flow.Header{
		ChainID: dps.FlowTestnet,
		Height:  42,
	}

	testHeaderB, err := cbor.Marshal(testHeader)
	require.NoError(t, err)

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetHeaderFunc: func(_ context.Context, in *GetHeaderRequest, _ ...grpc.CallOption) (*GetHeaderResponse, error) {
					assert.Equal(t, testHeader.Height, in.Height)

					return &GetHeaderResponse{
						Height: testHeader.Height,
						Data:   testHeaderB,
					}, nil
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		got, err := index.Header(testHeader.Height)

		if assert.NoError(t, err) {
			assert.Equal(t, testHeader, got)
		}
	})

	t.Run("handles index failures", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetHeaderFunc: func(_ context.Context, in *GetHeaderRequest, _ ...grpc.CallOption) (*GetHeaderResponse, error) {
					assert.Equal(t, testHeader.Height, in.Height)

					return nil, mocks.DummyError
				},
			},
		}

		_, err := index.Header(testHeader.Height)

		assert.Error(t, err)
	})

	t.Run("handles decoding failures", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetHeaderFunc: func(_ context.Context, in *GetHeaderRequest, _ ...grpc.CallOption) (*GetHeaderResponse, error) {
					assert.Equal(t, testHeader.Height, in.Height)

					return &GetHeaderResponse{
						Height: testHeader.Height,
						Data:   []byte(`invalid data`),
					}, nil
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		_, err := index.Header(testHeader.Height)

		assert.Error(t, err)
	})
}

func TestIndex_Commit(t *testing.T) {
	testHeight := uint64(42)
	testCommit := flow.StateCommitment{
		0x07, 0x01, 0x80, 0x30, 0x18, 0x7e, 0xef, 0x04,
		0x94, 0x5f, 0x35, 0xf1, 0xe3, 0x3a, 0x89, 0xdc,
		0x07, 0x01, 0x80, 0x30, 0x18, 0x7e, 0xef, 0x04,
		0x94, 0x5f, 0x35, 0xf1, 0xe3, 0x3a, 0x89, 0xdc,
	}

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetCommitFunc: func(_ context.Context, in *GetCommitRequest, _ ...grpc.CallOption) (*GetCommitResponse, error) {
					assert.Equal(t, testHeight, in.Height)

					return &GetCommitResponse{
						Height: testHeight,
						Commit: testCommit[:],
					}, nil
				},
			},
		}

		got, err := index.Commit(testHeight)

		if assert.NoError(t, err) {
			assert.Equal(t, testCommit, got)
		}
	})

	t.Run("handles index failures", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetCommitFunc: func(_ context.Context, in *GetCommitRequest, _ ...grpc.CallOption) (*GetCommitResponse, error) {
					assert.Equal(t, testHeight, in.Height)

					return nil, mocks.DummyError
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		_, err := index.Commit(testHeight)

		assert.Error(t, err)
	})

	t.Run("handles invalid indexed data", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetCommitFunc: func(_ context.Context, in *GetCommitRequest, _ ...grpc.CallOption) (*GetCommitResponse, error) {
					assert.Equal(t, testHeight, in.Height)

					return &GetCommitResponse{
						Height: testHeight,
						Commit: []byte(`not a commit`),
					}, nil
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		_, err := index.Commit(testHeight)

		assert.Error(t, err)
	})
}

func TestIndex_Values(t *testing.T) {
	testHeight := uint64(42)
	path1 := ledger.Path{0xaa, 0xc5, 0x13, 0xeb, 0x1a, 0x04, 0x57, 0x70, 0x0a, 0xc3, 0xfa, 0x8d, 0x29, 0x25, 0x13, 0xe1}
	path2 := ledger.Path{0xbb, 0xc5, 0x13, 0xeb, 0x1a, 0x54, 0x65, 0x41, 0x5a, 0xc3, 0xfa, 0x8d, 0x29, 0x25, 0x14, 0xf2}
	testPaths := []ledger.Path{path1, path2}
	testValues := []ledger.Value{ledger.Value(`test1`), ledger.Value(`test2`)}

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetRegisterValuesFunc: func(_ context.Context, in *GetRegisterValuesRequest, _ ...grpc.CallOption) (*GetRegisterValuesResponse, error) {
					if assert.Len(t, in.Paths, 2) {
						assert.Equal(t, []byte(path1), in.Paths[0])
						assert.Equal(t, []byte(path2), in.Paths[1])
					}
					assert.Equal(t, in.Height, testHeight)

					return &GetRegisterValuesResponse{
						Height: testHeight,
						Paths:  convert.PathsToBytes(testPaths),
						Values: convert.ValuesToBytes(testValues),
					}, nil
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		got, err := index.Values(testHeight, testPaths)

		if assert.NoError(t, err) {
			assert.Equal(t, testValues, got)
		}
	})

	t.Run("handles index failures", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetRegisterValuesFunc: func(_ context.Context, in *GetRegisterValuesRequest, _ ...grpc.CallOption) (*GetRegisterValuesResponse, error) {
					if assert.Len(t, in.Paths, 2) {
						assert.Equal(t, []byte(path1), in.Paths[0])
						assert.Equal(t, []byte(path2), in.Paths[1])
					}
					assert.Equal(t, in.Height, testHeight)

					return nil, mocks.DummyError
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		_, err := index.Values(testHeight, testPaths)

		assert.Error(t, err)
	})
}

func TestIndex_Height(t *testing.T) {
	testHeight := uint64(42)
	testBlockID, err := flow.HexStringToIdentifier("98827808c61af6b29c7f16071e69a9bbfba40d0f96b572ce23994b3aa605c7c2")
	require.NoError(t, err)

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetHeightForBlockFunc: func(_ context.Context, in *GetHeightForBlockRequest, _ ...grpc.CallOption) (*GetHeightForBlockResponse, error) {
					assert.Equal(t, testBlockID[:], in.BlockID)

					return &GetHeightForBlockResponse{
						BlockID: testBlockID[:],
						Height:  testHeight,
					}, nil
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		got, err := index.HeightForBlock(testBlockID)

		if assert.NoError(t, err) {
			assert.Equal(t, testHeight, got)
		}
	})

	t.Run("handles index failures", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetHeightForBlockFunc: func(_ context.Context, in *GetHeightForBlockRequest, _ ...grpc.CallOption) (*GetHeightForBlockResponse, error) {
					assert.Equal(t, testBlockID[:], in.BlockID)

					return nil, mocks.DummyError
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		_, err := index.HeightForBlock(testBlockID)

		assert.Error(t, err)
	})
}

func TestIndex_Transaction(t *testing.T) {
	testTransactionID := flow.Identifier{0x98, 0x82, 0x78, 0x08, 0xc6, 0x1a, 0xf6, 0xb2, 0x9c, 0x7f, 0x16, 0x07, 0x1e, 0x69, 0xa9, 0xbb, 0xfb, 0xa4, 0x0d, 0x0f, 0x96, 0xb5, 0x72, 0xce, 0x23, 0x99, 0x4b, 0x3a, 0xa6, 0x05, 0xc7, 0xc2}
	testTransaction := &flow.TransactionBody{
		ReferenceBlockID: flow.Identifier{0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a},
		Payer:            flow.Address{0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a},
	}
	testTransactionB, err := cbor.Marshal(testTransaction)
	require.NoError(t, err)

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetTransactionFunc: func(_ context.Context, in *GetTransactionRequest, _ ...grpc.CallOption) (*GetTransactionResponse, error) {
					assert.Equal(t, testTransactionID[:], in.TransactionID)

					return &GetTransactionResponse{
						TransactionID: testTransactionID[:],
						Data:          testTransactionB,
					}, nil
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		got, err := index.Transaction(testTransactionID)

		if assert.NoError(t, err) {
			assert.Equal(t, testTransaction, got)
		}
	})

	t.Run("handles index failures", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetTransactionFunc: func(_ context.Context, in *GetTransactionRequest, _ ...grpc.CallOption) (*GetTransactionResponse, error) {
					assert.Equal(t, testTransactionID[:], in.TransactionID)

					return nil, mocks.DummyError
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		_, err := index.Transaction(testTransactionID)

		assert.Error(t, err)
	})
}

func TestIndex_Events(t *testing.T) {
	testHeight := uint64(42)
	testTypes := []flow.EventType{"deposit", "withdrawal"}
	testEvents := []flow.Event{
		{Type: "deposit"},
		{Type: "withdrawal"},
	}
	testEventsB, err := cbor.Marshal(testEvents)
	require.NoError(t, err)

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetEventsFunc: func(_ context.Context, in *GetEventsRequest, _ ...grpc.CallOption) (*GetEventsResponse, error) {
					assert.Equal(t, testHeight, in.Height)
					assert.Equal(t, convert.TypesToStrings(testTypes), in.Types)

					return &GetEventsResponse{
						Height: testHeight,
						Types:  convert.TypesToStrings(testTypes),
						Data:   testEventsB,
					}, nil
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		got, err := index.Events(testHeight, testTypes...)

		if assert.NoError(t, err) {
			assert.Equal(t, testEvents, got)
		}
	})

	t.Run("handles index failures", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetEventsFunc: func(_ context.Context, in *GetEventsRequest, _ ...grpc.CallOption) (*GetEventsResponse, error) {
					assert.Equal(t, testHeight, in.Height)
					assert.Equal(t, convert.TypesToStrings(testTypes), in.Types)

					return nil, mocks.DummyError
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		_, err := index.Events(testHeight, testTypes...)

		assert.Error(t, err)
	})

	t.Run("handles invalid indexed data", func(t *testing.T) {
		t.Parallel()

		index := Index{
			client: &apiMock{
				GetEventsFunc: func(_ context.Context, in *GetEventsRequest, _ ...grpc.CallOption) (*GetEventsResponse, error) {
					assert.Equal(t, testHeight, in.Height)
					assert.Equal(t, convert.TypesToStrings(testTypes), in.Types)

					return &GetEventsResponse{
						Height: testHeight,
						Types:  convert.TypesToStrings(testTypes),
						Data:   []byte(`invalid data`),
					}, nil
				},
			},
			codec: &mocks.Codec{
				UnmarshalFunc: cbor.Unmarshal,
			},
		}

		_, err := index.Events(testHeight, testTypes...)

		assert.Error(t, err)
	})
}

type apiMock struct {
	GetFirstFunc                  func(ctx context.Context, in *GetFirstRequest, opts ...grpc.CallOption) (*GetFirstResponse, error)
	GetLastFunc                   func(ctx context.Context, in *GetLastRequest, opts ...grpc.CallOption) (*GetLastResponse, error)
	GetHeightForBlockFunc         func(ctx context.Context, in *GetHeightForBlockRequest, opts ...grpc.CallOption) (*GetHeightForBlockResponse, error)
	GetCommitFunc                 func(ctx context.Context, in *GetCommitRequest, opts ...grpc.CallOption) (*GetCommitResponse, error)
	GetHeaderFunc                 func(ctx context.Context, in *GetHeaderRequest, opts ...grpc.CallOption) (*GetHeaderResponse, error)
	GetEventsFunc                 func(ctx context.Context, in *GetEventsRequest, opts ...grpc.CallOption) (*GetEventsResponse, error)
	GetRegisterValuesFunc         func(ctx context.Context, in *GetRegisterValuesRequest, opts ...grpc.CallOption) (*GetRegisterValuesResponse, error)
	GetTransactionFunc            func(ctx context.Context, in *GetTransactionRequest, opts ...grpc.CallOption) (*GetTransactionResponse, error)
	ListTransactionsForHeightFunc func(ctx context.Context, in *ListTransactionsForHeightRequest, opts ...grpc.CallOption) (*ListTransactionsForHeightResponse, error)
}

func (a *apiMock) GetFirst(ctx context.Context, in *GetFirstRequest, opts ...grpc.CallOption) (*GetFirstResponse, error) {
	return a.GetFirstFunc(ctx, in, opts...)
}

func (a *apiMock) GetLast(ctx context.Context, in *GetLastRequest, opts ...grpc.CallOption) (*GetLastResponse, error) {
	return a.GetLastFunc(ctx, in, opts...)
}

func (a *apiMock) GetHeightForBlock(ctx context.Context, in *GetHeightForBlockRequest, opts ...grpc.CallOption) (*GetHeightForBlockResponse, error) {
	return a.GetHeightForBlockFunc(ctx, in, opts...)
}

func (a *apiMock) GetCommit(ctx context.Context, in *GetCommitRequest, opts ...grpc.CallOption) (*GetCommitResponse, error) {
	return a.GetCommitFunc(ctx, in, opts...)
}

func (a *apiMock) GetHeader(ctx context.Context, in *GetHeaderRequest, opts ...grpc.CallOption) (*GetHeaderResponse, error) {
	return a.GetHeaderFunc(ctx, in, opts...)
}

func (a *apiMock) GetEvents(ctx context.Context, in *GetEventsRequest, opts ...grpc.CallOption) (*GetEventsResponse, error) {
	return a.GetEventsFunc(ctx, in, opts...)
}

func (a *apiMock) GetRegisterValues(ctx context.Context, in *GetRegisterValuesRequest, opts ...grpc.CallOption) (*GetRegisterValuesResponse, error) {
	return a.GetRegisterValuesFunc(ctx, in, opts...)
}

func (a *apiMock) GetTransaction(ctx context.Context, in *GetTransactionRequest, opts ...grpc.CallOption) (*GetTransactionResponse, error) {
	return a.GetTransactionFunc(ctx, in, opts...)
}

func (a *apiMock) ListTransactionsForHeight(ctx context.Context, in *ListTransactionsForHeightRequest, opts ...grpc.CallOption) (*ListTransactionsForHeightResponse, error) {
	return a.ListTransactionsForHeightFunc(ctx, in, opts...)
}
