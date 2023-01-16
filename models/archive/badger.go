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

package archive

import (
	"github.com/dgraph-io/badger/v3"
)

// DefaultOptions returns the default Badger options preferred by the DPS for its index database.
func DefaultOptions(dir string) badger.Options {
	return badger.DefaultOptions(dir).
		WithNumMemtables(1).
		WithCompactL0OnClose(false).
		WithBaseTableSize(256 << 20)
}
