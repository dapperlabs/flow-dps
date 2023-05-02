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

package invoker

import (
	"fmt"

	"github.com/onflow/flow-go/model/flow"

	"github.com/onflow/flow-archive/models/archive"
)

func readRegister(index archive.Reader, cache Cache, height uint64) func(owner string, key string) (flow.RegisterValue, error) {
	return func(owner string, key string) (flow.RegisterValue, error) {
		// TODO(rbtz): remove cache once we switch to using read snapshot instead of delta view.
		cacheKey := fmt.Sprintf("%d/%x/%s", height, owner, key)
		cacheValue, ok := cache.Get(cacheKey)
		if ok {
			return cacheValue.(flow.RegisterValue), nil
		}

		values, err := index.Values(height, flow.RegisterIDs{flow.NewRegisterID(owner, key)})
		if err != nil {
			return nil, fmt.Errorf("could not read register: %w", err)
		}
		if len(values) != 1 {
			return nil, fmt.Errorf("wrong number of register values: %d", len(values))
		}

		value := flow.RegisterValue(values[0])
		_ = cache.Set(cacheKey, value, int64(len(value)))

		return value, nil
	}
}
