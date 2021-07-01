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

package mapper

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/optakt/flow-dps/models/dps"
)

type FSM struct {
	state       *State
	transitions map[Status]TransitionFunc
	wg          *sync.WaitGroup
}

func NewFSM(state *State, options ...func(*FSM)) *FSM {

	f := FSM{
		state:       state,
		transitions: make(map[Status]TransitionFunc),
		wg:          &sync.WaitGroup{},
	}

	for _, option := range options {
		option(&f)
	}

	return &f
}

func (f *FSM) Run() error {
	f.wg.Add(1)
	defer f.wg.Done()
	for {
		select {
		case <-f.state.done:
			return nil
		default:
			// continue
		}
		transition, ok := f.transitions[f.state.status]
		if !ok {
			return fmt.Errorf("could not find transition for status (%d)", f.state.status)
		}
		err := transition(f.state)
		if errors.Is(err, dps.ErrFinished) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("could not apply transition to state: %w", err)
		}
	}
}

func (f *FSM) Stop(ctx context.Context) error {
	close(f.state.done)
	f.wg.Wait()
	return nil
}
