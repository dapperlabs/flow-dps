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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v2"

	"github.com/optakt/flow-dps/rosetta/identifier"
)

type FlowHeightAwareBalancer struct {
	log     zerolog.Logger
	targets sporks
}

func NewHeightAwareBalancer(log zerolog.Logger, configPath string) (*FlowHeightAwareBalancer, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("could not open configuration file: %w", err)
	}

	b, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("could not read configuration file: %w", err)
	}

	var targets sporks
	err = yaml.Unmarshal(b, &targets)
	if err != nil {
		return nil, fmt.Errorf("could not decode configuration file: %w", err)
	}

	sort.Sort(targets)

	f := FlowHeightAwareBalancer{
		log:     log,
		targets: targets,
	}

	return &f, nil
}

// blockAwareRequest is a request that contains a BlockID field. It partially matches the Rosetta API requests.
type blockAwareRequest struct {
	BlockID identifier.Block `json:"block_identifier,omitempty"`
}

func (f *FlowHeightAwareBalancer) Next(e echo.Context) *middleware.ProxyTarget {
	var reqBody []byte
	if e.Request().Body != nil {
		var err error
		reqBody, err = io.ReadAll(e.Request().Body) // Read
		if err != nil {
			e.Error(err)
		}
		e.Request().Body = io.NopCloser(bytes.NewBuffer(reqBody)) // Reset
	}

	var req blockAwareRequest
	err := json.Unmarshal(reqBody, &req)
	if err != nil {
		e.Error(err)
	}

	height := uint64(math.MaxUint64)
	if req.BlockID.Index != nil {
		height = *req.BlockID.Index
	}

	return f.targets.ProxyTarget(height)
}

func (f *FlowHeightAwareBalancer) AddTarget(*middleware.ProxyTarget) bool {
	return true
}

func (f *FlowHeightAwareBalancer) RemoveTarget(string) bool {
	return true
}
