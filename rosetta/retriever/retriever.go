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

package retriever

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go/model/flow"
	"github.com/optakt/flow-dps/rosetta/converter"
	"github.com/optakt/flow-dps/rosetta/failure"

	"github.com/optakt/flow-dps/models/dps"
	"github.com/optakt/flow-dps/models/index"
	"github.com/optakt/flow-dps/rosetta/identifier"
	"github.com/optakt/flow-dps/rosetta/object"
)

type Retriever struct {
	cfg Config

	params    dps.Params
	index     index.Reader
	validate  Validator
	generator Generator
	invoke    Invoker
	convert   Converter
}

func New(params dps.Params, index index.Reader, validate Validator, generator Generator, invoke Invoker, convert Converter, options ...func(*Config)) *Retriever {

	cfg := Config{
		TransactionLimit: 200,
	}

	for _, opt := range options {
		opt(&cfg)
	}

	r := Retriever{
		cfg:       cfg,
		params:    params,
		index:     index,
		validate:  validate,
		generator: generator,
		invoke:    invoke,
		convert:   convert,
	}

	return &r
}

func (r *Retriever) Oldest() (identifier.Block, time.Time, error) {

	first, err := r.index.First()
	if err != nil {
		return identifier.Block{}, time.Time{}, fmt.Errorf("could not find first indexed block: %w", err)
	}

	header, err := r.index.Header(first)
	if err != nil {
		return identifier.Block{}, time.Time{}, fmt.Errorf("could not find block header: %w", err)
	}

	block := identifier.Block{
		Hash:  header.ID().String(),
		Index: &header.Height,
	}

	return block, header.Timestamp, nil
}

func (r *Retriever) Current() (identifier.Block, time.Time, error) {

	last, err := r.index.Last()
	if err != nil {
		return identifier.Block{}, time.Time{}, fmt.Errorf("could not find last indexed block: %w", err)
	}

	header, err := r.index.Header(last)
	if err != nil {
		return identifier.Block{}, time.Time{}, fmt.Errorf("could not find block header: %w", err)
	}

	block := identifier.Block{
		Hash:  header.ID().String(),
		Index: &header.Height,
	}

	return block, header.Timestamp, nil
}

func (r *Retriever) Balances(block identifier.Block, account identifier.Account, currencies []identifier.Currency) (identifier.Block, []object.Amount, error) {

	// Run validation on the block identifier. This also fills in missing fields, where possible.
	completed, err := r.validate.Block(block)
	if err != nil {
		return identifier.Block{}, nil, fmt.Errorf("could not validate block: %w", err)
	}

	// Run validation on the account ID. This uses the chain ID to check the
	// address validation.
	err = r.validate.Account(account)
	if err != nil {
		return identifier.Block{}, nil, fmt.Errorf("could not validate account: %w", err)
	}

	// Run validation on the currencies. This checks basically if we know the
	// currency and if it has the correct decimals set, if they are set.
	for idx, currency := range currencies {
		completeCurrency, err := r.validate.Currency(currency)
		if err != nil {
			return identifier.Block{}, nil, fmt.Errorf("could not validate currency: %w", err)
		}
		currencies[idx] = completeCurrency
	}

	// Get the Cadence value that is the result of the script execution.
	amounts := make([]object.Amount, 0, len(currencies))
	address := cadence.NewAddress(flow.HexToAddress(account.Address))
	for _, currency := range currencies {
		getBalance, err := r.generator.GetBalance(currency.Symbol)
		if err != nil {
			return identifier.Block{}, nil, fmt.Errorf("could not generate script: %w", err)
		}
		value, err := r.invoke.Script(*completed.Index, getBalance, []cadence.Value{address})
		if err != nil {
			return identifier.Block{}, nil, fmt.Errorf("could not invoke script: %w", err)
		}
		balance, ok := value.ToGoValue().(uint64)
		if !ok {
			return identifier.Block{}, nil, fmt.Errorf("could not convert balance (type: %T)", value.ToGoValue())
		}
		amount := object.Amount{
			Currency: currency,
			Value:    strconv.FormatUint(balance, 10),
		}
		amounts = append(amounts, amount)
	}

	return completed, amounts, nil
}

func (r *Retriever) Block(id identifier.Block) (*object.Block, []identifier.Transaction, error) {

	// Run validation on the block ID. This also fills in missing information.
	completed, err := r.validate.Block(id)
	if err != nil {
		return nil, nil, fmt.Errorf("could not validate block: %w", err)
	}

	// Retrieve the Flow token default withdrawal and deposit events.
	deposit, err := r.generator.TokensDeposited(dps.FlowSymbol)
	if err != nil {
		return nil, nil, fmt.Errorf("could not generate deposit event type: %w", err)
	}
	withdrawal, err := r.generator.TokensWithdrawn(dps.FlowSymbol)
	if err != nil {
		return nil, nil, fmt.Errorf("could not generate withdrawal event type: %w", err)
	}

	// Then, get the header; it contains the block ID, parent ID and timestamp.
	header, err := r.index.Header(*completed.Index)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get header: %w", err)
	}

	// Next, we get all the events for the block to extract deposit and withdrawal events.
	events, err := r.index.Events(*completed.Index, flow.EventType(deposit), flow.EventType(withdrawal))
	if err != nil {
		return nil, nil, fmt.Errorf("could not get events: %w", err)
	}

	// Convert events to operations and group them by transaction ID.
	buckets := make(map[string][]object.Operation)
	for _, event := range events {
		op, err := r.convert.EventToOperation(event)
		if errors.Is(err, converter.ErrIrrelevant) {
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("could not convert event: %w", err)
		}

		txID := event.TransactionID.String()
		buckets[txID] = append(buckets[txID], *op)
	}

	// Iterate over all transactionIDs to create transactions with all relevant operations.
	// We first do this so we can sort transactions in the resulting slice.
	var transactions []*object.Transaction
	for txID, operations := range buckets {

		// Set RelatedIDs for all operations for the same transaction.
		for i := range operations {
			for j := range operations {
				if i == j {
					continue
				}
				operations[i].RelatedIDs = append(operations[i].RelatedIDs, operations[j].ID)
			}
		}

		transaction := object.Transaction{
			ID:         identifier.Transaction{Hash: txID},
			Operations: operations,
		}
		transactions = append(transactions, &transaction)
	}

	sort.Slice(transactions, func(i int, j int) bool {
		return strings.Compare(transactions[i].ID.Hash, transactions[j].ID.Hash) < 0
	})
	limit := r.cfg.TransactionLimit
	if limit > uint(len(transactions)) {
		limit = uint(len(transactions))
	}
	blockTransactions := transactions[:limit]
	extraTransactions := transactions[limit:]
	extraIdentifiers := make([]identifier.Transaction, 0, len(extraTransactions))
	for _, transaction := range extraTransactions {
		extraIdentifiers = append(extraIdentifiers, transaction.ID)
	}

	// We need the *uint64 for the parent block identifier.
	var parentHeight *uint64
	if header.Height > 0 {
		h := header.Height - 1
		parentHeight = &h
	}

	// Now we just need to build the block.
	block := object.Block{
		ID: identifier.Block{
			Index: &header.Height,
			Hash:  header.ID().String(),
		},
		ParentID: identifier.Block{
			Index: parentHeight,
			Hash:  header.ParentID.String(),
		},
		Timestamp:    header.Timestamp.UnixNano() / 1_000_000,
		Transactions: blockTransactions,
	}

	return &block, extraIdentifiers, nil
}

func (r *Retriever) Transaction(block identifier.Block, id identifier.Transaction) (*object.Transaction, error) {

	// Run validation on the block ID. This also fills in missing information.
	completed, err := r.validate.Block(block)
	if err != nil {
		return nil, fmt.Errorf("could not validate block: %w", err)
	}

	// Run validation on the transaction ID. This should never fail, as we
	// already check the length, but let's run it anyway.
	err = r.validate.Transaction(id)
	if err != nil {
		return nil, fmt.Errorf("could not validate transaction: %w", err)
	}

	txIDs, err := r.index.TransactionsByHeight(*completed.Index)
	if err != nil {
		return nil, fmt.Errorf("could not list block transactions: %w", err)
	}
	var found bool
	for _, txID := range txIDs {
		if txID.String() == id.Hash {
			found = true
			break
		}
	}
	if !found {
		return nil, failure.UnknownTransaction{
			Index:   *completed.Index,
			Hash:    id.Hash,
			Message: "transaction not found in block",
		}
	}

	// Retrieve the Flow token default withdrawal and deposit events.
	deposit, err := r.generator.TokensDeposited(dps.FlowSymbol)
	if err != nil {
		return nil, fmt.Errorf("could not generate deposit event type: %w", err)
	}
	withdrawal, err := r.generator.TokensWithdrawn(dps.FlowSymbol)
	if err != nil {
		return nil, fmt.Errorf("could not generate withdrawal event type: %w", err)
	}

	// Retrieve the deposit and withdrawal events for the block (yes, all of them).
	events, err := r.index.Events(*completed.Index, flow.EventType(deposit), flow.EventType(withdrawal))
	if err != nil {
		return nil, fmt.Errorf("could not get events: %w", err)
	}

	// Convert events to operations and group them by transaction ID.
	var ops []object.Operation
	for _, event := range events {
		// Ignore events that are related to other transactions.
		if event.TransactionID.String() != id.Hash {
			continue
		}

		op, err := r.convert.EventToOperation(event)
		if errors.Is(err, converter.ErrIrrelevant) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("could not convert event: %w", err)
		}

		ops = append(ops, *op)
	}

	// Set RelatedIDs for all operations for the same transaction.
	for i := range ops {
		for j := range ops {
			if i == j {
				continue
			}

			ops[i].RelatedIDs = append(ops[i].RelatedIDs, ops[j].ID)
		}

	}

	transaction := object.Transaction{
		ID:         id,
		Operations: ops,
	}

	return &transaction, nil
}
