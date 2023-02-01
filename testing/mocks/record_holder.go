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

package mocks

import (
	"github.com/onflow/flow-go/ledger/common/testutils"
	"github.com/onflow/flow-go/module/executiondatasync/execution_data"
	"github.com/onflow/flow-go/utils/unittest"
	"testing"

	"github.com/onflow/flow/protobuf/go/flow/entities"

	"github.com/onflow/flow-go/engine/common/rpc/convert"
	"github.com/onflow/flow-go/model/flow"
)

type RecordHolder struct {
	RecordFunc func(blockID flow.Identifier) (*entities.BlockExecutionData, error)
}

func BaselineRecordHolder(t *testing.T) *RecordHolder {
	t.Helper()

	r := RecordHolder{
		RecordFunc: func(id flow.Identifier) (*entities.BlockExecutionData, error) {
			numChunks := 5
			ced := make([]*entities.ChunkExecutionData, numChunks)

			for i := 0; i < numChunks; i++ {
				header := unittest.BlockHeaderFixture()
				events := unittest.BlockEventsFixture(header, 5).Events
				tx1 := unittest.TransactionBodyFixture()
				tx2 := unittest.TransactionBodyFixture()
				col := &flow.Collection{Transactions: []*flow.TransactionBody{&tx1, &tx2}}

				chunk := &execution_data.ChunkExecutionData{
					Collection: col,
					Events:     events,
					TrieUpdate: testutils.TrieUpdateFixture(1, 1, 8),
				}
				convertedChunk, err := convert.ChunkExecutionDataToMessage(chunk)
				if err != nil {
					return nil, err
				}
				ced[i] = convertedChunk
			}

			data := entities.BlockExecutionData{
				BlockId:            convert.IdentifierToMessage(id),
				ChunkExecutionData: ced,
			}

			return &data, nil
		},
	}

	return &r
}

func (r *RecordHolder) Record(blockID flow.Identifier) (*entities.BlockExecutionData, error) {
	return r.RecordFunc(blockID)
}
