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

package feeder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/onflow/flow-go/ledger/common/encoding"
	"github.com/onflow/flow-go/ledger/complete/wal"
	"github.com/optakt/flow-dps/models/dps"
	"github.com/optakt/flow-dps/testing/mocks"
)

func TestFromReader(t *testing.T) {
	reader := mocks.BaselineWALReader(t)

	feeder := FromReader(reader)

	assert.Equal(t, reader, feeder.reader)
}

func TestFeeder_Update(t *testing.T) {
	encodedTrieUpdate := encoding.EncodeTrieUpdate(mocks.GenericTrieUpdate)
	rootHash := mocks.GenericTrieUpdate.RootHash[:]

	t.Run("nominal case", func(t *testing.T) {
		t.Parallel()

		var recordCalled bool
		reader := mocks.BaselineWALReader(t)
		reader.RecordFunc = func() []byte {
			// On the first call, return a Delete operation which should get ignored and skipped.
			if !recordCalled {
				recordCalled = true
				return append([]byte{byte(wal.WALDelete)}, rootHash...)
			}

			// On any subsequent call, return the Update operation.
			return append([]byte{byte(wal.WALUpdate)}, encodedTrieUpdate...)
		}

		feeder := &Feeder{
			reader: reader,
		}

		got, err := feeder.Update()

		require.NoError(t, err)
		assert.Equal(t, mocks.GenericTrieUpdate, got)
	})

	t.Run("handles reader failure", func(t *testing.T) {
		t.Parallel()

		reader := mocks.BaselineWALReader(t)
		reader.NextFunc = func() bool {
			return false
		}
		reader.ErrFunc = func() error {
			return mocks.GenericError
		}

		feeder := &Feeder{
			reader: reader,
		}

		_, err := feeder.Update()

		assert.Error(t, err)
	})

	t.Run("handles unavailable next record", func(t *testing.T) {
		t.Parallel()

		reader := mocks.BaselineWALReader(t)
		reader.NextFunc = func() bool {
			return false
		}

		feeder := &Feeder{
			reader: reader,
		}

		_, err := feeder.Update()

		require.Error(t, err)
		assert.Equal(t, dps.ErrUnavailable, err)
	})

	t.Run("handles badly encoded record", func(t *testing.T) {
		t.Parallel()

		reader := mocks.BaselineWALReader(t)
		reader.RecordFunc = func() []byte {
			return mocks.GenericBytes
		}

		feeder := &Feeder{
			reader: reader,
		}

		_, err := feeder.Update()

		assert.Error(t, err)
	})
}
