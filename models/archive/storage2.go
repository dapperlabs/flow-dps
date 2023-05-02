// Copyright 2023 Dapper Labs
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

package archive

import (
	"github.com/onflow/flow-go/model/flow"
)

type RegisterReader interface {
	Get(height uint64, reg flow.RegisterID) ([]byte, error)
}

type RegisterWriter interface {
	SetBatch(height uint64, entries flow.RegisterEntries) error
}
