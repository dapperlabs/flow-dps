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

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/dgraph-io/badger/v2"
	"github.com/labstack/echo/v4"
	modelindex "github.com/optakt/flow-dps/models/index"
	serviceindex "github.com/optakt/flow-dps/service/index"
	"github.com/optakt/flow-dps/service/storage"
	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"github.com/ziflex/lecho/v2"
	"google.golang.org/grpc"

	"github.com/onflow/flow-go/model/flow"

	api "github.com/optakt/flow-dps/api/dps"
	"github.com/optakt/flow-dps/api/rosetta"
	"github.com/optakt/flow-dps/codec/zbor"
	"github.com/optakt/flow-dps/models/dps"
	"github.com/optakt/flow-dps/rosetta/configuration"
	"github.com/optakt/flow-dps/rosetta/converter"
	"github.com/optakt/flow-dps/rosetta/invoker"
	"github.com/optakt/flow-dps/rosetta/retriever"
	"github.com/optakt/flow-dps/rosetta/scripts"
	"github.com/optakt/flow-dps/rosetta/validator"
)

const (
	success = 0
	failure = 1
)

func main() {
	os.Exit(run())
}

func run() int {

	// Signal catching for clean shutdown.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	// Command line parameter initialization.
	var (
		flagAPI          string
		flagCache        uint64
		flagChain        string
		flagLevel        string
		flagPort         uint16
		flagTransactions uint
		flagIndex        string
	)

	pflag.StringVarP(&flagAPI, "api", "a", "127.0.0.1:5005", "host URL for GRPC API endpoint")
	pflag.Uint64VarP(&flagCache, "cache", "e", uint64(datasize.GB), "maximum cache size for register reads in bytes")
	pflag.StringVarP(&flagChain, "chain", "c", dps.FlowTestnet.String(), "chain ID for Flow network core contracts")
	pflag.StringVarP(&flagLevel, "level", "l", "info", "log output level")
	pflag.Uint16VarP(&flagPort, "port", "p", 8080, "port to host Rosetta API on")
	pflag.UintVarP(&flagTransactions, "transaction-limit", "t", 200, "maximum amount of transactions to include in a block response")
	pflag.StringVarP(&flagIndex, "index", "i", "", "database directory for state index (disables GRPC API if specified)")

	pflag.Parse()

	// Logger initialization.
	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
	log := zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.DebugLevel)
	level, err := zerolog.ParseLevel(flagLevel)
	if err != nil {
		log.Error().Str("level", flagLevel).Err(err).Msg("could not parse log level")
		return failure
	}
	log = log.Level(level)
	elog := lecho.From(log)

	// Check if the configured chain ID is valid.
	params, ok := dps.FlowParams[flow.ChainID(flagChain)]
	if !ok {
		log.Error().Str("chain", flagChain).Msg("invalid chain ID for params")
		return failure
	}

	// Initialize the API client.
	conn, err := grpc.Dial(flagAPI, grpc.WithInsecure())
	if err != nil {
		log.Error().Str("api", flagAPI).Err(err).Msg("could not dial API host")
		return failure
	}
	defer conn.Close()

	// Initialize storage library.
	codec, err := zbor.NewCodec()
	if err != nil {
		log.Error().Err(err).Msg("could not initialize storage codec")
		return failure
	}

	var index modelindex.Reader

	// Rosetta API initialization.
	if flagIndex != "" {
		db, err := badger.Open(dps.DefaultOptions(flagIndex))
		if err != nil {
			log.Error().Str("index", flagIndex).Err(err).Msg("could not open index DB")
			return failure
		}
		defer db.Close()

		library := storage.New(codec)

		index = serviceindex.NewReader(db, library)
	} else {

		// Initialize the API client.
		conn, err := grpc.Dial(flagAPI, grpc.WithInsecure())
		if err != nil {
			log.Error().Str("api", flagAPI).Err(err).Msg("could not dial API host")
			return failure
		}
		defer conn.Close()

		client := api.NewAPIClient(conn)
		index = api.IndexFromAPI(client, codec)
	}

	// Rosetta API initialization.
	config := configuration.New(params.ChainID)
	validate := validator.New(params, index)
	generate := scripts.NewGenerator(params)
	invoke, err := invoker.New(index, invoker.WithCacheSize(flagCache))
	if err != nil {
		log.Error().Err(err).Msg("could not initialize invoker")
		return failure
	}

	convert, err := converter.New(generate)
	if err != nil {
		log.Error().Err(err).Msg("could not generate transaction event types")
		return failure
	}

	retrieve := retriever.New(params, index, validate, generate, invoke, convert,
		retriever.WithTransactionLimit(flagTransactions),
	)
	ctrl := rosetta.NewData(config, retrieve)

	server := echo.New()
	server.HideBanner = true
	server.HidePort = true
	server.Logger = elog
	server.Use(lecho.Middleware(lecho.Config{Logger: elog}))
	server.POST("/network/list", ctrl.Networks)
	server.POST("/network/options", ctrl.Options)
	server.POST("/network/status", ctrl.Status)
	server.POST("/account/balance", ctrl.Balance)
	server.POST("/block", ctrl.Block)
	server.POST("/block/transaction", ctrl.Transaction)

	// This section launches the main executing components in their own
	// goroutine, so they can run concurrently. Afterwards, we wait for an
	// interrupt signal in order to proceed with the next section.
	done := make(chan struct{})
	failed := make(chan struct{})
	go func() {
		log.Info().Msg("Flow Rosetta Server starting")
		err := server.Start(fmt.Sprint(":", flagPort))
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Warn().Err(err).Msg("Flow Rosetta Server failed")
			close(failed)
		} else {
			close(done)
		}
		log.Info().Msg("Flow Rosetta Server stopped")
	}()

	select {
	case <-sig:
		log.Info().Msg("Flow Rosetta Server stopping")
	case <-done:
		log.Info().Msg("Flow Rosetta Server done")
	case <-failed:
		log.Warn().Msg("Flow Rosetta Server aborted")
		return failure
	}
	go func() {
		<-sig
		log.Warn().Msg("forcing exit")
		os.Exit(1)
	}()

	// The following code starts a shut down with a certain timeout and makes
	// sure that the main executing components are shutting down within the
	// allocated shutdown time. Otherwise, we will force the shutdown and log
	// an error. We then wait for shutdown on each component to complete.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = server.Shutdown(ctx)
	if err != nil {
		log.Error().Err(err).Msg("could not shut down Rosetta API")
		return failure
	}

	return success
}
