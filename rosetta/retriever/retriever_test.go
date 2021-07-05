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

package retriever

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/onflow/cadence"
	"github.com/onflow/cadence/encoding/json"
	"github.com/onflow/cadence/runtime/tests/utils"
	"github.com/optakt/flow-dps/rosetta/converter"

	"github.com/onflow/flow-go/model/flow"

	"github.com/optakt/flow-dps/models/dps"
	"github.com/optakt/flow-dps/rosetta/identifier"
	"github.com/optakt/flow-dps/rosetta/object"
	"github.com/optakt/flow-dps/testing/mocks"
)

func TestNew(t *testing.T) {
	params := dps.Params{ChainID: dps.FlowTestnet}
	index := &mocks.Reader{}
	validate := &mocks.Validator{}
	generator := &mocks.Generator{}
	invoke := &mocks.Invoker{}
	convert := &mocks.Converter{}

	r := New(params, index, validate, generator, invoke, convert)

	if assert.NotNil(t, r) {
		assert.Equal(t, params, r.params)
		assert.Equal(t, index, r.index)
		assert.Equal(t, validate, r.validate)
		assert.Equal(t, generator, r.generator)
		assert.Equal(t, invoke, r.invoke)
		assert.Equal(t, convert, r.convert)
	}
}

func TestRetriever_Oldest(t *testing.T) {
	testHeight := uint64(42)
	testTime := time.Time{} // 1/1/1970
	testBlockID := identifier.Block{
		Index: getUint64P(42),
		Hash:  "499b933f5ecd062d5ff7914218a40f8abf0efee9267d46ae827c938f8a5c18ae",
	}
	testHeader := &flow.Header{
		Height:    testHeight,
		Timestamp: testTime,
	}

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		index := &mocks.Reader{
			FirstFunc: func() (uint64, error) {
				return testHeight, nil
			},
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return testHeader, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.index = index

		blockID, blockTime, err := ret.Oldest()

		if assert.NoError(t, err) {
			assert.Equal(t, testTime, blockTime)
			assert.Equal(t, testBlockID, blockID)
		}
	})

	t.Run("handles index.First failure", func(t *testing.T) {
		t.Parallel()

		index := &mocks.Reader{
			FirstFunc: func() (uint64, error) {
				return 0, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.index = index

		_, _, err = ret.Oldest()

		assert.Error(t, err)
	})

	t.Run("handles index.Header failure", func(t *testing.T) {
		t.Parallel()

		index := &mocks.Reader{
			FirstFunc: func() (uint64, error) {
				return testHeight, nil
			},
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return nil, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.index = index

		_, _, err = ret.Oldest()

		assert.Error(t, err)
	})
}

func TestRetriever_Current(t *testing.T) {
	testHeight := uint64(42)
	testTime := time.Time{} // 1/1/1970
	testBlockID := identifier.Block{
		Index: getUint64P(42),
		Hash:  "499b933f5ecd062d5ff7914218a40f8abf0efee9267d46ae827c938f8a5c18ae",
	}
	testHeader := &flow.Header{
		Height:    testHeight,
		Timestamp: testTime,
	}

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		index := &mocks.Reader{
			LastFunc: func() (uint64, error) {
				return testHeight, nil
			},
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return testHeader, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.index = index

		blockID, blockTime, err := ret.Current()

		if assert.NoError(t, err) {
			assert.Equal(t, testTime, blockTime)
			assert.Equal(t, testBlockID, blockID)
		}
	})

	t.Run("handles index.Last failure", func(t *testing.T) {
		t.Parallel()

		index := &mocks.Reader{
			LastFunc: func() (uint64, error) {
				return 0, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.index = index

		_, _, err = ret.Current()

		assert.Error(t, err)
	})

	t.Run("handles index.Header failure", func(t *testing.T) {
		t.Parallel()

		index := &mocks.Reader{
			LastFunc: func() (uint64, error) {
				return testHeight, nil
			},
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return nil, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.index = index

		_, _, err = ret.Current()

		assert.Error(t, err)
	})
}

func TestRetriever_Balances(t *testing.T) {
	testBlockID := identifier.Block{
		Index: getUint64P(42),
		Hash:  "2c5efefc2fafa000a3102f2931598d2d",
	}
	testAccount := identifier.Account{Address: "f7e6413e94feda9c"}
	testCurrency1 := identifier.Currency{Symbol: "TEST1"}
	testCurrency2 := identifier.Currency{Symbol: "TEST2"}
	testCurrency3 := identifier.Currency{Symbol: "TEST3"}
	testCurrencies := []identifier.Currency{testCurrency1, testCurrency2, testCurrency3}
	testAmounts := []object.Amount{
		{
			Value:    "42",
			Currency: testCurrency1,
		},
		{
			Value:    "42",
			Currency: testCurrency2,
		},
		{
			Value:    "42",
			Currency: testCurrency3,
		},
	}
	testScript := []byte(`testScript`)
	testValue, err := cadence.NewValue(uint64(42))
	require.NoError(t, err)

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			AccountFunc: func(address identifier.Account) error {
				assert.Equal(t, testAccount, address)
				return nil
			},
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
			CurrencyFunc: func(currency identifier.Currency) (identifier.Currency, error) {
				assert.Contains(t, testCurrencies, currency)
				return currency, nil
			},
		}
		generator := &mocks.Generator{
			GetBalanceFunc: func(symbol string) ([]byte, error) {
				assert.Contains(t, testCurrencies, identifier.Currency{Symbol: symbol})
				return testScript, nil
			},
		}
		invoker := &mocks.Invoker{
			ScriptFunc: func(height uint64, script []byte, parameters []cadence.Value) (cadence.Value, error) {
				assert.Equal(t, testBlockID.Index, &height)
				assert.Equal(t, testScript, script)
				if assert.Len(t, parameters, 1) {
					assert.Equal(t, cadence.NewAddress(flow.HexToAddress(testAccount.Address)), parameters[0])
				}
				return testValue, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator
		ret.invoke = invoker

		blockID, amounts, err := ret.Balances(testBlockID, testAccount, testCurrencies)

		if assert.NoError(t, err) {
			assert.Equal(t, testBlockID, blockID)
			assert.Equal(t, testAmounts, amounts)
		}
	})

	t.Run("handles invalid block", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return identifier.Block{}, errors.New("invalid block")
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator

		_, _, err = ret.Balances(testBlockID, testAccount, testCurrencies)
		assert.Error(t, err)
	})

	t.Run("handles invalid account", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
			AccountFunc: func(address identifier.Account) error {
				assert.Equal(t, testAccount, address)
				return errors.New("invalid account")
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator

		_, _, err = ret.Balances(testBlockID, testAccount, testCurrencies)
		assert.Error(t, err)
	})

	t.Run("handles invalid currency", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
			AccountFunc: func(address identifier.Account) error {
				assert.Equal(t, testAccount, address)
				return nil
			},
			CurrencyFunc: func(currency identifier.Currency) (identifier.Currency, error) {
				assert.Contains(t, testCurrencies, currency)
				return currency, errors.New("invalid currency")
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator

		_, _, err = ret.Balances(testBlockID, testAccount, testCurrencies)
		assert.Error(t, err)
	})

	t.Run("handles generator failure", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			AccountFunc: func(address identifier.Account) error {
				assert.Equal(t, testAccount, address)
				return nil
			},
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
			CurrencyFunc: func(currency identifier.Currency) (identifier.Currency, error) {
				assert.Contains(t, testCurrencies, currency)
				return currency, nil
			},
		}
		generator := &mocks.Generator{
			GetBalanceFunc: func(symbol string) ([]byte, error) {
				assert.Contains(t, testCurrencies, identifier.Currency{Symbol: symbol})
				return nil, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator

		_, _, err = ret.Balances(testBlockID, testAccount, testCurrencies)
		assert.Error(t, err)
	})

	t.Run("handles invoker failure", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			AccountFunc: func(address identifier.Account) error {
				assert.Equal(t, testAccount, address)
				return nil
			},
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
			CurrencyFunc: func(currency identifier.Currency) (identifier.Currency, error) {
				assert.Contains(t, testCurrencies, currency)
				return currency, nil
			},
		}
		generator := &mocks.Generator{
			GetBalanceFunc: func(symbol string) ([]byte, error) {
				assert.Contains(t, testCurrencies, identifier.Currency{Symbol: symbol})
				return []byte(`testScript`), nil
			},
		}
		invoker := &mocks.Invoker{
			ScriptFunc: func(height uint64, script []byte, parameters []cadence.Value) (cadence.Value, error) {
				assert.Equal(t, testBlockID.Index, &height)
				assert.Equal(t, testScript, script)
				if assert.Len(t, parameters, 1) {
					assert.Equal(t, cadence.NewAddress(flow.HexToAddress(testAccount.Address)), parameters[0])
				}

				return nil, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator
		ret.invoke = invoker

		_, _, err = ret.Balances(testBlockID, testAccount, testCurrencies)
		assert.Error(t, err)
	})
}

func TestRetriever_Block(t *testing.T) {
	testHeight := uint64(42)
	parentID, err := flow.HexStringToIdentifier("ecd01710d4a40c11f4aa884bcb926b43f162f4cf302e4c4f1dd0f9e231c30879")
	require.NoError(t, err)
	testHeader := &flow.Header{
		Height:    testHeight,
		ParentID:  parentID,
		Timestamp: time.Date(1972, 12, 31, 0, 0, 0, 0, time.UTC),
	}
	id1, err := flow.HexStringToIdentifier("a4c4194eae1a2dd0de4f4d51a884db4255bf265a40ddd98477a1d60ef45909ec")
	require.NoError(t, err)
	id2, err := flow.HexStringToIdentifier("fcd01710d4a40c11f4aa884bcb926b43f162f4cf302e4c4f1dd0f9e231c30878")
	require.NoError(t, err)
	testBlockID := identifier.Block{
		Index: &testHeight,
		Hash:  "69c5a0c3dce44c9e80f7ee41995f6746f78013787a88057995cb3556e721a4b6",
	}
	depositOp := object.Operation{
		ID:         identifier.Operation{Index: 0},
		RelatedIDs: []identifier.Operation{{Index: 1}},
		Type:       dps.OperationTransfer,
		Status:     dps.StatusCompleted,
		AccountID:  identifier.Account{Address: "0102030405060708"},
		Amount: object.Amount{
			Value:    "42",
			Currency: identifier.Currency{Symbol: dps.FlowSymbol, Decimals: dps.FlowDecimals},
		},
	}
	withdrawalOp := object.Operation{
		ID:         identifier.Operation{Index: 1},
		RelatedIDs: []identifier.Operation{{Index: 0}},
		Type:       dps.OperationTransfer,
		Status:     dps.StatusCompleted,
		AccountID:  identifier.Account{Address: "0203040506070809"},
		Amount: object.Amount{
			Value: "-42",
			Currency: identifier.Currency{
				Symbol: dps.FlowSymbol, Decimals: dps.FlowDecimals,
			},
		},
	}
	testTransaction1 := &object.Transaction{
		ID: identifier.Transaction{
			Hash: id1.String(),
		},
		Operations: []object.Operation{
			depositOp,
			withdrawalOp,
		},
	}
	testTransaction2 := &object.Transaction{
		ID: identifier.Transaction{
			Hash: id2.String(),
		},
		Operations: []object.Operation{
			depositOp,
			withdrawalOp,
		},
	}

	depositType := &cadence.EventType{
		Location:            utils.TestLocation,
		QualifiedIdentifier: "deposit",
		Fields: []cadence.Field{
			{
				Identifier: "amount",
				Type:       cadence.UInt64Type{},
			},
			{
				Identifier: "address",
				Type:       cadence.AddressType{},
			},
		},
	}
	depositEvent := cadence.NewEvent(
		[]cadence.Value{
			cadence.NewUInt64(42),
			cadence.NewAddress([8]byte{1, 2, 3, 4, 5, 6, 7, 8}),
		},
	).WithType(depositType)
	depositEventPayload := json.MustEncode(depositEvent)

	withdrawalType := &cadence.EventType{
		Location:            utils.TestLocation,
		QualifiedIdentifier: "withdrawal",
		Fields: []cadence.Field{
			{
				Identifier: "amount",
				Type:       cadence.UInt64Type{},
			},
			{
				Identifier: "address",
				Type:       cadence.AddressType{},
			},
		},
	}
	withdrawalEvent := cadence.NewEvent(
		[]cadence.Value{
			cadence.NewUInt64(42),
			cadence.NewAddress([8]byte{2, 3, 4, 5, 6, 7, 8, 9}),
		},
	).WithType(withdrawalType)
	withdrawalEventPayload := json.MustEncode(withdrawalEvent)

	testEvents := []flow.Event{
		{
			TransactionID: id1,
			Type:          "deposit",
			EventIndex:    0,
			Payload:       depositEventPayload,
		},
		{
			TransactionID: id1,
			Type:          "withdrawal",
			EventIndex:    1,
			Payload:       withdrawalEventPayload,
		},
		{
			TransactionID: id2,
			Type:          "deposit",
			EventIndex:    2,
			Payload:       depositEventPayload,
		},
		{
			TransactionID: id2,
			Type:          "withdrawal",
			EventIndex:    3,
			Payload:       withdrawalEventPayload,
		},
	}

	t.Run("nominal case without limit", func(t *testing.T) {
		t.Parallel()

		wantBlock := &object.Block{
			ID: testBlockID,
			ParentID: identifier.Block{
				Index: getUint64P(testHeight - 1),
				Hash:  parentID.String(),
			},
			Timestamp: testHeader.Timestamp.UnixNano() / 1_000_000,

			// No limit, so all transactions should be in block.
			Transactions: []*object.Transaction{
				testTransaction1,
				testTransaction2,
			},
		}

		index := &mocks.Reader{
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return testHeader, nil
			},
			EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
				assert.Equal(t, testHeight, height)
				if assert.Len(t, types, 2) {
					assert.Equal(t, flow.EventType("deposit"), types[0])
					assert.Equal(t, flow.EventType("withdrawal"), types[1])
				}
				return testEvents, nil
			},
		}
		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "withdrawal", nil
			},
		}
		convert := &mocks.Converter{
			EventToOperationFunc: func(event flow.Event) (*object.Operation, error) {
				assert.Contains(t, testEvents, event)

				var op object.Operation
				switch event.Type {
				case "deposit":
					op = depositOp
				case "withdrawal":
					op = withdrawalOp
				}

				op.RelatedIDs = nil // unset RelatedIDs to prevent having duplicate related IDs.
				return &op, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.index = index
		ret.validate = validator
		ret.generator = generator
		ret.convert = convert

		block, extra, err := ret.Block(testBlockID)

		if assert.NoError(t, err) {
			// Since transactions are built from a map, their order can vary, so we need to check them this way.
			for _, tx := range wantBlock.Transactions {
				assert.Contains(t, block.Transactions, tx)
			}
			assert.Equal(t, wantBlock.ID, block.ID)
			assert.Empty(t, extra)
		}
	})

	t.Run("nominal case with limit reached exactly", func(t *testing.T) {
		t.Parallel()

		wantBlock := &object.Block{
			ID: testBlockID,
			ParentID: identifier.Block{
				Index: getUint64P(41),
				Hash:  parentID.String(),
			},
			Timestamp: testHeader.Timestamp.UnixNano() / 1_000_000,

			// Limit is 2, so both transactions should be included in block.
			Transactions: []*object.Transaction{
				testTransaction1,
				testTransaction2,
			},
		}

		index := &mocks.Reader{
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return testHeader, nil
			},
			EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
				assert.Equal(t, testHeight, height)
				if assert.Len(t, types, 2) {
					assert.Equal(t, flow.EventType("deposit"), types[0])
					assert.Equal(t, flow.EventType("withdrawal"), types[1])
				}
				return testEvents, nil
			},
		}
		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "withdrawal", nil
			},
		}
		convert := &mocks.Converter{
			EventToOperationFunc: func(event flow.Event) (*object.Operation, error) {
				assert.Contains(t, testEvents, event)

				var op object.Operation
				switch event.Type {
				case "deposit":
					op = depositOp
				case "withdrawal":
					op = withdrawalOp
				}

				op.RelatedIDs = nil // unset RelatedIDs to prevent having duplicate related IDs.
				return &op, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.index = index
		ret.validate = validator
		ret.generator = generator
		ret.convert = convert
		ret.cfg.TransactionLimit = 2

		block, extra, err := ret.Block(testBlockID)

		if assert.NoError(t, err) {
			// Since transactions are built from a map, their order can vary, so we need to check them this way.
			for _, tx := range wantBlock.Transactions {
				assert.Contains(t, block.Transactions, tx)
			}
			assert.Equal(t, wantBlock.ID, block.ID)
			assert.Empty(t, extra)
		}
	})

	t.Run("nominal case with more transactions than limit", func(t *testing.T) {
		t.Parallel()

		wantBlock := &object.Block{
			ID: testBlockID,
			ParentID: identifier.Block{
				Index: getUint64P(41),
				Hash:  parentID.String(),
			},
			Timestamp: testHeader.Timestamp.UnixNano() / 1_000_000,

			// Limit is 1, so only the first transaction should be included in the block.
			Transactions: []*object.Transaction{
				testTransaction1,
			},
		}

		index := &mocks.Reader{
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return testHeader, nil
			},
			EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
				assert.Equal(t, testHeight, height)
				if assert.Len(t, types, 2) {
					assert.Equal(t, flow.EventType("deposit"), types[0])
					assert.Equal(t, flow.EventType("withdrawal"), types[1])
				}
				return testEvents, nil
			},
		}
		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "withdrawal", nil
			},
		}
		convert := &mocks.Converter{
			EventToOperationFunc: func(event flow.Event) (*object.Operation, error) {
				assert.Contains(t, testEvents, event)

				var op object.Operation
				switch event.Type {
				case "deposit":
					op = depositOp
				case "withdrawal":
					op = withdrawalOp
				}

				op.RelatedIDs = nil // unset RelatedIDs to prevent having duplicate related IDs.
				return &op, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.index = index
		ret.validate = validator
		ret.generator = generator
		ret.convert = convert
		ret.cfg.TransactionLimit = 1

		block, extra, err := ret.Block(testBlockID)

		if assert.NoError(t, err) {
			assert.Equal(t, wantBlock.ID, block.ID)

			assert.Len(t, block.Transactions, 1)
			assert.Len(t, extra, 1)
		}
	})

	t.Run("handles block without transactions", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "withdrawal", nil
			},
		}
		index := &mocks.Reader{
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return testHeader, nil
			},
			EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
				assert.Equal(t, testHeight, height)
				if assert.Len(t, types, 2) {
					assert.Equal(t, flow.EventType("deposit"), types[0])
					assert.Equal(t, flow.EventType("withdrawal"), types[1])
				}

				return []flow.Event{}, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.index = index
		ret.validate = validator
		ret.generator = generator

		got, _, err := ret.Block(testBlockID)
		if assert.NoError(t, err) {
			assert.Empty(t, got.Transactions)
		}
	})

	t.Run("handles block without relevant events", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "withdrawal", nil
			},
		}
		index := &mocks.Reader{
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return testHeader, nil
			},
			EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
				assert.Equal(t, testHeight, height)
				if assert.Len(t, types, 2) {
					assert.Equal(t, flow.EventType("deposit"), types[0])
					assert.Equal(t, flow.EventType("withdrawal"), types[1])
				}

				return testEvents, nil
			},
		}
		convert := &mocks.Converter{
			EventToOperationFunc: func(event flow.Event) (*object.Operation, error) {
				assert.Contains(t, testEvents, event)
				return &depositOp, converter.ErrIrrelevant
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.index = index
		ret.validate = validator
		ret.generator = generator
		ret.convert = convert

		got, _, err := ret.Block(testBlockID)

		if assert.NoError(t, err) {
			assert.Empty(t, got.Transactions)
		}
	})

	t.Run("handles invalid block", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return identifier.Block{}, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator

		_, _, err = ret.Block(testBlockID)
		assert.Error(t, err)
	})

	t.Run("handles deposit script generator failure", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "", mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator

		_, _, err = ret.Block(testBlockID)
		assert.Error(t, err)
	})

	t.Run("handles withdrawal script generator failure", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "", mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator

		_, _, err = ret.Block(testBlockID)
		assert.Error(t, err)
	})

	t.Run("handles index header retrieval failure", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "", nil
			},
		}
		index := &mocks.Reader{
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return nil, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator
		ret.index = index

		_, _, err = ret.Block(testBlockID)
		assert.Error(t, err)
	})

	t.Run("handles index event retrieval failure", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "withdrawal", nil
			},
		}
		index := &mocks.Reader{
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return testHeader, nil
			},
			EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
				assert.Equal(t, testHeight, height)
				if assert.Len(t, types, 2) {
					assert.Equal(t, flow.EventType("deposit"), types[0])
					assert.Equal(t, flow.EventType("withdrawal"), types[1])
				}
				return nil, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator
		ret.index = index

		_, _, err = ret.Block(testBlockID)
		assert.Error(t, err)
	})

	t.Run("handles event converter failure", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, symbol, dps.FlowSymbol)
				return "withdrawal", nil
			},
		}
		index := &mocks.Reader{
			HeaderFunc: func(height uint64) (*flow.Header, error) {
				assert.Equal(t, testHeight, height)
				return testHeader, nil
			},
			EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
				assert.Equal(t, testHeight, height)
				if assert.Len(t, types, 2) {
					assert.Equal(t, flow.EventType("deposit"), types[0])
					assert.Equal(t, flow.EventType("withdrawal"), types[1])
				}

				return testEvents, nil
			},
		}
		convert := &mocks.Converter{
			EventToOperationFunc: func(event flow.Event) (*object.Operation, error) {
				return nil, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator
		ret.index = index
		ret.convert = convert

		_, _, err = ret.Block(testBlockID)
		assert.Error(t, err)
	})
}

func TestRetriever_Transaction(t *testing.T) {
	id, err := flow.HexStringToIdentifier("a4c4194eae1a2dd0de4f4d51a884db4255bf265a40ddd98477a1d60ef45909ec")
	require.NoError(t, err)

	depositType := &cadence.EventType{
		Location:            utils.TestLocation,
		QualifiedIdentifier: "deposit",
		Fields: []cadence.Field{
			{
				Identifier: "amount",
				Type:       cadence.UInt64Type{},
			},
			{
				Identifier: "address",
				Type:       cadence.AddressType{},
			},
		},
	}
	depositEvent := cadence.NewEvent(
		[]cadence.Value{
			cadence.NewUInt64(42),
			cadence.NewAddress([8]byte{1, 2, 3, 4, 5, 6, 7, 8}),
		},
	).WithType(depositType)
	depositEventPayload := json.MustEncode(depositEvent)

	withdrawalType := &cadence.EventType{
		Location:            utils.TestLocation,
		QualifiedIdentifier: "withdrawal",
		Fields: []cadence.Field{
			{
				Identifier: "amount",
				Type:       cadence.UInt64Type{},
			},
			{
				Identifier: "address",
				Type:       cadence.AddressType{},
			},
		},
	}
	withdrawalEvent := cadence.NewEvent(
		[]cadence.Value{
			cadence.NewUInt64(42),
			cadence.NewAddress([8]byte{2, 3, 4, 5, 6, 7, 8, 9}),
		},
	).WithType(withdrawalType)
	withdrawalEventPayload := json.MustEncode(withdrawalEvent)

	testEvents := []flow.Event{{
		TransactionID: id,
		EventIndex:    0,
		Type:          "deposit",
		Payload:       depositEventPayload,
	}, {
		TransactionID: id,
		Type:          "withdrawal",
		EventIndex:    1,
		Payload:       withdrawalEventPayload,
	}}

	testHeight := uint64(42)
	testBlockID := identifier.Block{
		Index: &testHeight,
		Hash:  "2c4c176c5c095bc3529ab425735077efb2afedd16c9ffc215a898df14fa8ac91",
	}
	testTransactionID := identifier.Transaction{
		Hash: id.String(),
	}

	depositOp := object.Operation{
		ID:         identifier.Operation{Index: 0},
		RelatedIDs: []identifier.Operation{{Index: 1}},
		Type:       dps.OperationTransfer,
		Status:     dps.StatusCompleted,
		AccountID:  identifier.Account{Address: "0102030405060708"},
		Amount: object.Amount{
			Value:    "42",
			Currency: identifier.Currency{Symbol: dps.FlowSymbol, Decimals: dps.FlowDecimals},
		},
	}
	withdrawalOp := object.Operation{
		ID:         identifier.Operation{Index: 1},
		RelatedIDs: []identifier.Operation{{Index: 0}},
		Type:       dps.OperationTransfer,
		Status:     dps.StatusCompleted,
		AccountID:  identifier.Account{Address: "0203040506070809"},
		Amount: object.Amount{
			Value: "-42",
			Currency: identifier.Currency{
				Symbol: dps.FlowSymbol, Decimals: dps.FlowDecimals,
			},
		},
	}
	testTransaction := &object.Transaction{
		ID: identifier.Transaction{
			Hash: id.String(),
		},
		Operations: []object.Operation{
			depositOp, withdrawalOp,
		},
	}

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
			TransactionFunc: func(transaction identifier.Transaction) error {
				assert.Equal(t, testTransactionID, transaction)
				return nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, dps.FlowSymbol, symbol)
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, dps.FlowSymbol, symbol)
				return "withdrawal", nil
			},
		}
		index := &mocks.Reader{
			EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
				assert.Equal(t, testHeight, height)
				if assert.Len(t, types, 2) {
					assert.Equal(t, flow.EventType("deposit"), types[0])
					assert.Equal(t, flow.EventType("withdrawal"), types[1])
				}

				return testEvents, nil
			},
			TransactionsByHeightFunc: func(height uint64) ([]flow.Identifier, error) {
				assert.Equal(t, testHeight, height)
				return []flow.Identifier{id}, nil
			},
		}
		convert := &mocks.Converter{
			EventToOperationFunc: func(event flow.Event) (*object.Operation, error) {
				assert.Contains(t, testEvents, event)

				var op object.Operation
				switch event.Type {
				case "deposit":
					op = depositOp
				case "withdrawal":
					op = withdrawalOp
				}

				op.RelatedIDs = nil // unset RelatedIDs to prevent having duplicate related IDs.
				return &op, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator
		ret.index = index
		ret.convert = convert

		got, err := ret.Transaction(testBlockID, testTransactionID)

		if assert.NoError(t, err) {
			assert.Equal(t, testTransaction, got)
		}
	})

	t.Run("handles transaction with no relevant operations", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
			TransactionFunc: func(transaction identifier.Transaction) error {
				assert.Equal(t, testTransactionID, transaction)
				return nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, dps.FlowSymbol, symbol)
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, dps.FlowSymbol, symbol)
				return "withdrawal", nil
			},
		}
		index := &mocks.Reader{
			EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
				assert.Equal(t, testHeight, height)
				if assert.Len(t, types, 2) {
					assert.Equal(t, flow.EventType("deposit"), types[0])
					assert.Equal(t, flow.EventType("withdrawal"), types[1])
				}

				return testEvents, nil
			},
			TransactionsByHeightFunc: func(height uint64) ([]flow.Identifier, error) {
				assert.Equal(t, testHeight, height)
				return []flow.Identifier{id}, nil
			},
		}
		convert := &mocks.Converter{
			EventToOperationFunc: func(event flow.Event) (*object.Operation, error) {
				assert.Contains(t, testEvents, event)
				return &depositOp, converter.ErrIrrelevant
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator
		ret.index = index
		ret.convert = convert

		got, err := ret.Transaction(testBlockID, testTransactionID)

		if assert.NoError(t, err) {
			assert.Empty(t, got.Operations)
		}
	})

	t.Run("handles invalid block", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return identifier.Block{}, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator

		_, err = ret.Transaction(testBlockID, testTransactionID)
		assert.Error(t, err)
	})

	t.Run("handles invalid transaction", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
			TransactionFunc: func(transaction identifier.Transaction) error {
				assert.Equal(t, testTransactionID, transaction)
				return mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator

		_, err = ret.Transaction(testBlockID, testTransactionID)
		assert.Error(t, err)
	})

	t.Run("block does not contain transaction", func(t *testing.T) {
		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return identifier.Block{}, mocks.DummyError
			},
			TransactionFunc: func(transaction identifier.Transaction) error {
				assert.Equal(t, testTransactionID, transaction)
				return mocks.DummyError
			},
		}
		index := &mocks.Reader{
			TransactionsByHeightFunc: func(height uint64) ([]flow.Identifier, error) {
				assert.Equal(t, testHeight, height)
				return []flow.Identifier{}, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.index = index

		_, err = ret.Transaction(testBlockID, testTransactionID)
		assert.Error(t, err)
	})

	t.Run("handles transactions index failure", func(t *testing.T) {
		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return identifier.Block{}, mocks.DummyError
			},
			TransactionFunc: func(transaction identifier.Transaction) error {
				assert.Equal(t, testTransactionID, transaction)
				return mocks.DummyError
			},
		}
		index := &mocks.Reader{
			TransactionsByHeightFunc: func(height uint64) ([]flow.Identifier, error) {
				assert.Equal(t, testHeight, height)
				return nil, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.index = index

		_, err = ret.Transaction(testBlockID, testTransactionID)
		assert.Error(t, err)
	})

	t.Run("handles deposit script generator failure", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				return block, nil
			},
			TransactionFunc: func(transaction identifier.Transaction) error {
				return nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				return "", mocks.DummyError
			},
		}
		index := &mocks.Reader{
			TransactionsByHeightFunc: func(height uint64) ([]flow.Identifier, error) {
				return []flow.Identifier{id}, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator
		ret.index = index

		_, err = ret.Transaction(testBlockID, testTransactionID)
		assert.Error(t, err)
	})

	t.Run("handles withdrawal script generator failure", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				return block, nil
			},
			TransactionFunc: func(transaction identifier.Transaction) error {
				return nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				return "", mocks.DummyError
			},
		}
		index := &mocks.Reader{
			TransactionsByHeightFunc: func(height uint64) ([]flow.Identifier, error) {
				return []flow.Identifier{id}, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator
		ret.index = index

		_, err = ret.Transaction(testBlockID, testTransactionID)
		assert.Error(t, err)
	})

	t.Run("handles index event retrieval failure", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
			TransactionFunc: func(transaction identifier.Transaction) error {
				assert.Equal(t, testTransactionID, transaction)
				return nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, dps.FlowSymbol, symbol)
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, dps.FlowSymbol, symbol)
				return "withdrawal", nil
			},
		}
		index := &mocks.Reader{
			EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
				assert.Equal(t, testHeight, height)
				if assert.Len(t, types, 2) {
					assert.Equal(t, flow.EventType("deposit"), types[0])
					assert.Equal(t, flow.EventType("withdrawal"), types[1])
				}

				return nil, mocks.DummyError
			},
			TransactionsByHeightFunc: func(height uint64) ([]flow.Identifier, error) {
				return []flow.Identifier{id}, nil
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator
		ret.index = index

		_, err = ret.Transaction(testBlockID, testTransactionID)
		assert.Error(t, err)
	})

	t.Run("handles converter failure", func(t *testing.T) {
		t.Parallel()

		validator := &mocks.Validator{
			BlockFunc: func(block identifier.Block) (identifier.Block, error) {
				assert.Equal(t, testBlockID, block)
				return block, nil
			},
			TransactionFunc: func(transaction identifier.Transaction) error {
				assert.Equal(t, testTransactionID, transaction)
				return nil
			},
		}
		generator := &mocks.Generator{
			TokensDepositedFunc: func(symbol string) (string, error) {
				assert.Equal(t, dps.FlowSymbol, symbol)
				return "deposit", nil
			},
			TokensWithdrawnFunc: func(symbol string) (string, error) {
				assert.Equal(t, dps.FlowSymbol, symbol)
				return "withdrawal", nil
			},
		}
		index := &mocks.Reader{
			EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
				assert.Equal(t, testHeight, height)
				if assert.Len(t, types, 2) {
					assert.Equal(t, flow.EventType("deposit"), types[0])
					assert.Equal(t, flow.EventType("withdrawal"), types[1])
				}

				return testEvents, nil
			},
			TransactionsByHeightFunc: func(height uint64) ([]flow.Identifier, error) {
				return []flow.Identifier{id}, nil
			},
		}
		convert := &mocks.Converter{
			EventToOperationFunc: func(event flow.Event) (*object.Operation, error) {
				assert.Contains(t, testEvents, event)
				return nil, mocks.DummyError
			},
		}

		ret, err := baselineRetriever(t)
		require.NoError(t, err)

		ret.validate = validator
		ret.generator = generator
		ret.index = index
		ret.convert = convert

		_, err = ret.Transaction(testBlockID, testTransactionID)

		assert.Error(t, err)
	})
}

func baselineRetriever(t *testing.T) (*Retriever, error) {
	t.Helper()

	testScript := []byte(`testScript`)
	testValue, err := cadence.NewValue(uint64(42))
	if err != nil {
		return nil, err
	}

	id, err := flow.HexStringToIdentifier("a4c4194eae1a2dd0de4f4d51a884db4255bf265a40ddd98477a1d60ef45909ec")
	if err != nil {
		return nil, err
	}

	testHeight := uint64(42)
	testTime := time.Time{} // 1/1/1970
	testHeader := &flow.Header{
		Height:    testHeight,
		Timestamp: testTime,
	}

	depositType := &cadence.EventType{
		Location:            utils.TestLocation,
		QualifiedIdentifier: "deposit",
		Fields: []cadence.Field{
			{
				Identifier: "amount",
				Type:       cadence.UInt64Type{},
			},
			{
				Identifier: "address",
				Type:       cadence.AddressType{},
			},
		},
	}
	depositEvent := cadence.NewEvent(
		[]cadence.Value{
			cadence.NewUInt64(42),
			cadence.NewAddress([8]byte{1, 2, 3, 4, 5, 6, 7, 8}),
		},
	).WithType(depositType)
	depositEventPayload := json.MustEncode(depositEvent)

	withdrawalType := &cadence.EventType{
		Location:            utils.TestLocation,
		QualifiedIdentifier: "withdrawal",
		Fields: []cadence.Field{
			{
				Identifier: "amount",
				Type:       cadence.UInt64Type{},
			},
			{
				Identifier: "address",
				Type:       cadence.AddressType{},
			},
		},
	}
	withdrawalEvent := cadence.NewEvent(
		[]cadence.Value{
			cadence.NewUInt64(42),
			cadence.NewAddress([8]byte{2, 3, 4, 5, 6, 7, 8, 9}),
		},
	).WithType(withdrawalType)
	withdrawalEventPayload := json.MustEncode(withdrawalEvent)

	testEvents := []flow.Event{{
		TransactionID: id,
		EventIndex:    0,
		Type:          "deposit",
		Payload:       depositEventPayload,
	}, {
		TransactionID: id,
		Type:          "withdrawal",
		EventIndex:    1,
		Payload:       withdrawalEventPayload,
	}}

	depositOp := object.Operation{
		ID:         identifier.Operation{Index: 0},
		RelatedIDs: []identifier.Operation{{Index: 1}},
		Type:       dps.OperationTransfer,
		Status:     dps.StatusCompleted,
		AccountID:  identifier.Account{Address: "0102030405060708"},
		Amount: object.Amount{
			Value:    "42",
			Currency: identifier.Currency{Symbol: dps.FlowSymbol, Decimals: dps.FlowDecimals},
		},
	}
	withdrawalOp := object.Operation{
		ID:         identifier.Operation{Index: 1},
		RelatedIDs: []identifier.Operation{{Index: 0}},
		Type:       dps.OperationTransfer,
		Status:     dps.StatusCompleted,
		AccountID:  identifier.Account{Address: "0203040506070809"},
		Amount: object.Amount{
			Value: "-42",
			Currency: identifier.Currency{
				Symbol: dps.FlowSymbol, Decimals: dps.FlowDecimals,
			},
		},
	}

	validator := &mocks.Validator{
		AccountFunc: func(address identifier.Account) error {
			return nil
		},
		BlockFunc: func(block identifier.Block) (identifier.Block, error) {
			return block, nil
		},
		CurrencyFunc: func(currency identifier.Currency) (identifier.Currency, error) {
			return currency, nil
		},
		TransactionFunc: func(transaction identifier.Transaction) error {
			return nil
		},
	}
	generator := &mocks.Generator{
		TokensDepositedFunc: func(symbol string) (string, error) {
			return "deposit", nil
		},
		TokensWithdrawnFunc: func(symbol string) (string, error) {
			return "withdrawal", nil
		},
		GetBalanceFunc: func(symbol string) ([]byte, error) {
			return testScript, nil
		},
	}
	index := &mocks.Reader{
		FirstFunc: func() (uint64, error) {
			return testHeight, nil
		},
		HeaderFunc: func(height uint64) (*flow.Header, error) {
			return testHeader, nil
		},
		EventsFunc: func(height uint64, types ...flow.EventType) ([]flow.Event, error) {
			return testEvents, nil
		},
		TransactionsByHeightFunc: func(height uint64) ([]flow.Identifier, error) {
			return []flow.Identifier{id}, nil
		},
	}
	convert := &mocks.Converter{
		EventToOperationFunc: func(event flow.Event) (*object.Operation, error) {
			var op object.Operation
			switch event.Type {
			case "deposit":
				op = depositOp
			case "withdrawal":
				op = withdrawalOp
			}

			op.RelatedIDs = nil // unset RelatedIDs to prevent having duplicate related IDs.
			return &op, nil
		},
	}
	invoker := &mocks.Invoker{
		ScriptFunc: func(height uint64, script []byte, parameters []cadence.Value) (cadence.Value, error) {
			return testValue, nil
		},
	}

	retriever := Retriever{
		cfg:       Config{TransactionLimit: 999},
		params:    dps.Params{ChainID: dps.FlowTestnet},
		index:     index,
		validate:  validator,
		generator: generator,
		invoke:    invoker,
		convert:   convert,
	}

	return &retriever, nil
}

func getUint64P(n uint64) *uint64 {
	return &n
}
