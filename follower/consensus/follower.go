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

package consensus

import (
	"math"

	"github.com/dgraph-io/badger/v2"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/storage/badger/operation"
	"github.com/rs/zerolog"

	"github.com/onflow/flow-go/follower"
)

type Follower struct {
	log zerolog.Logger

	db        *badger.DB
	consensus follower.ConsensusFollower
	height    uint64
}

func New(log zerolog.Logger, follower follower.ConsensusFollower, db *badger.DB) *Follower {
	f := Follower{
		log: log,

		consensus: follower,
		db:        db,
		height:    math.MaxUint64,
	}

	follower.AddOnBlockFinalizedConsumer(f.OnBlockFinalized)

	return &f
}

func (f *Follower) OnBlockFinalized(finalizedBlockID flow.Identifier) {
	var height uint64
	err := f.db.View(operation.RetrieveFinalizedHeight(&height))
	if err != nil {
		f.log.Error().Err(err).Msg("Could not retrieve finalized block height")
		return
	}

	f.height = height
}

func (f *Follower) Height() uint64 {
	return f.height
}

// FIXME: Add BlockID method to show latest block ID.

// FIXME: Document in this file the indexes that are automatically written by
//  the follower. Only the ones that we use though, as the maintenance effort
//  would not be worth it otherwise.
