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

package proxy

import (
	"github.com/labstack/echo/v4/middleware"
)

type spork struct {
	RootHeight uint64 `yaml:"root_height"`
	Target     target `yaml:"target"`
}

type sporks []spork

func (s sporks) Len() int {
	return len(s)
}

func (s sporks) Less(i, j int) bool {
	return s[i].RootHeight < s[j].RootHeight
}

func (s sporks) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sporks) ProxyTarget(height uint64) *middleware.ProxyTarget {
	// Iterate over all sporks except the last one and check if the requested height is between the root height of this
	// element and the next's.
	for i := range s[:len(s)-1] {
		if height >= s[i].RootHeight && height <= s[i+1].RootHeight {
			return s[i].Target.ToProxyTarget()
		}
	}

	// If no match was found, it means the height is above the root height of the last listed spork. Return its target.
	return s[len(s)-1].Target.ToProxyTarget()
}
