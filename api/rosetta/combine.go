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

package rosetta

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/onflow/flow-go-sdk"

	"github.com/optakt/flow-dps/rosetta/identifier"
	"github.com/optakt/flow-dps/rosetta/object"
)

// CombineRequest implements the request schema for /construction/combine.
// See https://www.rosetta-api.org/docs/ConstructionApi.html#request
type CombineRequest struct {
	NetworkID           identifier.Network `json:"network_identifier"`
	UnsignedTransaction string             `json:"unsigned_transaction"`
	Signatures          []object.Signature `json:"signatures"`
}

// CombineResponse implements the response schema for /construction/combine.
// See https://www.rosetta-api.org/docs/ConstructionApi.html#response
type CombineResponse struct {
	SignedTransaction string `json:"signed_transaction"`
}

// Combine implements the /construction/combine endpoint of the Rosetta Construction API.
// Combine endpoint creates a signed transaction by combining an unsigned transaction and
// a list of signatures.
// See https://www.rosetta-api.org/docs/ConstructionApi.html#constructioncombine
func (c *Construction) Combine(ctx echo.Context) error {

	var req CombineRequest
	err := ctx.Bind(&req)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, invalidEncoding(invalidJSON, err))
	}

	if req.NetworkID.Blockchain == "" {
		return echo.NewHTTPError(http.StatusBadRequest, invalidFormat(blockchainEmpty))
	}
	if req.NetworkID.Network == "" {
		return echo.NewHTTPError(http.StatusBadRequest, invalidFormat(networkEmpty))
	}

	if req.UnsignedTransaction == "" {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("transaction text empty"))
	}

	if len(req.Signatures) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Errorf("signatures list is empty"))
	}

	sig := req.Signatures[0]
	if sig.SignatureType != FlowSignatureAlgorithm {
		return echo.NewHTTPError(http.StatusUnprocessableEntity, fmt.Errorf("unsupported signature algorithm: %v", sig.SignatureType))
	}

	var txPayload flow.Transaction
	err = json.Unmarshal([]byte(req.UnsignedTransaction), &txPayload)
	if err != nil {
		return echo.NewHTTPError(http.StatusUnprocessableEntity, fmt.Errorf("could not decode transaction: %w", err))
	}
	tx := &txPayload

	var sender flow.Address
	if len(tx.Authorizers) > 0 {
		sender = tx.Authorizers[0]
	}

	// Determine if the signature belongs to the sender.
	if sig.SigningPayload.AccountID.Address == sender.Hex() {

		// Since we're treating the sender as also the payer and the proposer,
		// we only need to sign the transaction envelope.

		// TODO: We can have multiple keys and signatures for a single account.
		// However, in order to add multiple signatures, we need to know the key
		// index for each individual signature.
		signer := flow.HexToAddress(sig.SigningPayload.AccountID.Address)
		tx = tx.AddEnvelopeSignature(signer, 0, []byte(sig.HexBytes))
	}

	encoded, err := json.Marshal(tx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Errorf("could not encode transaction: %w", err))
	}

	res := CombineResponse{
		SignedTransaction: string(encoded),
	}

	return ctx.JSON(http.StatusOK, res)
}
