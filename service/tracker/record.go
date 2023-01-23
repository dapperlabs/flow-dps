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

package tracker

import (
	"github.com/koko1123/flow-go-1/model/flow"
	"github.com/onflow/flow-go/engine/execution/computation/computer/uploader"
)

// RecordStreamer represents something that can stream block data.
type RecordStreamer interface {
	Next() (*uploader.BlockData, error)
}

// RecordHolder represents something that can be used to request
// block data for a specific block identifier.
type RecordHolder interface {
	Record(blockID flow.Identifier) (*uploader.BlockData, error)
}
