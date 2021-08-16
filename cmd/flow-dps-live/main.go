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
	"os"
	"os/signal"
	"runtime"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/optakt/flow-dps/testing/mocks"
	"github.com/rs/zerolog"
	"github.com/spf13/pflag"

	"github.com/onflow/flow-go/follower"
	"github.com/onflow/flow-go/model/flow"

	"github.com/optakt/flow-dps/bucket"
	"github.com/optakt/flow-dps/codec/zbor"
	"github.com/optakt/flow-dps/follower/consensus"
	"github.com/optakt/flow-dps/follower/execution"
	"github.com/optakt/flow-dps/metrics/output"
	"github.com/optakt/flow-dps/metrics/rcrowley"
	"github.com/optakt/flow-dps/models/dps"
	"github.com/optakt/flow-dps/service/chain"
	"github.com/optakt/flow-dps/service/forest"
	"github.com/optakt/flow-dps/service/index"
	"github.com/optakt/flow-dps/service/loader"
	"github.com/optakt/flow-dps/service/mapper"
	"github.com/optakt/flow-dps/service/metrics"
	"github.com/optakt/flow-dps/service/storage"
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
		flagAccessID          string
		flagBindAddr          string
		flagBucket            string
		flagCheckpoint        string
		flagData              string
		flagDownloadDir       string
		flagForce             bool
		flagIndex             string
		flagIndexAll          bool
		flagIndexCollections  bool
		flagIndexCommit       bool
		flagIndexEvents       bool
		flagIndexGuarantees   bool
		flagIndexHeader       bool
		flagIndexPayloads     bool
		flagIndexResults      bool
		flagIndexSeals        bool
		flagIndexTransactions bool
		flagLevel             string
		flagMetrics           bool
		flagMetricsInterval   time.Duration
		flagNodeID            string
		flagRegion            string
		flagSkipBootstrap     bool
	)

	pflag.StringVar(&flagAccessID, "access-id", "", "id of the access node to connect to")
	pflag.StringVarP(&flagBucket, "bucket", "b", "", "name of the S3 bucket which contains the state ledger")
	pflag.StringVar(&flagBindAddr, "bind-addr", "127.0.0.1:FIXME", "address on which to bind the FIXME")
	pflag.StringVarP(&flagData, "data", "d", "", "database directory for protocol data")
	pflag.StringVarP(&flagCheckpoint, "checkpoint", "c", "", "checkpoint file for state trie")
	pflag.StringVarP(&flagDownloadDir, "download-directory", "d", "", "directory where to download ledger WAL checkpoints")
	pflag.BoolVarP(&flagForce, "force", "f", false, "overwrite existing index database")
	pflag.StringVarP(&flagIndex, "index", "i", "index", "database directory for state index")
	pflag.BoolVarP(&flagIndexAll, "index-all", "a", false, "index everything")
	pflag.BoolVar(&flagIndexCollections, "index-collections", false, "index collections")
	pflag.BoolVar(&flagIndexGuarantees, "index-guarantees", false, "index collection guarantees")
	pflag.BoolVar(&flagIndexCommit, "index-commits", false, "index commits")
	pflag.BoolVar(&flagIndexEvents, "index-events", false, "index events")
	pflag.BoolVar(&flagIndexHeader, "index-headers", false, "index headers")
	pflag.BoolVar(&flagIndexPayloads, "index-payloads", false, "index payloads")
	pflag.BoolVar(&flagIndexResults, "index-results", false, "index transaction results")
	pflag.BoolVar(&flagIndexTransactions, "index-transactions", false, "index transactions")
	pflag.BoolVar(&flagIndexSeals, "index-seals", false, "index seals")
	pflag.StringVarP(&flagLevel, "level", "l", "info", "log output level")
	pflag.BoolVarP(&flagMetrics, "metrics", "m", false, "enable metrics collection and output")
	pflag.DurationVar(&flagMetricsInterval, "metrics-interval", 5*time.Minute, "defines the interval of metrics output to log")
	pflag.StringVarP(&flagNodeID, "node-id", "n", "", "node id to use for the DPS")
	pflag.StringVarP(&flagRegion, "region", "r", "", "region in which the S3 bucket is available")
	pflag.BoolVar(&flagSkipBootstrap, "skip-bootstrap", false, "enable skipping checkpoint register payloads indexing")

	pflag.Parse()

	// Increase the GOMAXPROCS value in order to use the full IOPS available, see:
	// https://groups.google.com/g/golang-nuts/c/jPb_h3TvlKE
	_ = runtime.GOMAXPROCS(128)

	// Logger initialization.
	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
	log := zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.DebugLevel)
	level, err := zerolog.ParseLevel(flagLevel)
	if err != nil {
		log.Error().Str("level", flagLevel).Err(err).Msg("could not parse log level")
		return failure
	}
	log = log.Level(level)

	// Ensure that at least one index is specified.
	if !flagIndexAll && !flagIndexCommit && !flagIndexHeader && !flagIndexPayloads && !flagIndexCollections &&
		!flagIndexGuarantees && !flagIndexTransactions && !flagIndexResults && !flagIndexEvents && !flagIndexSeals {
		log.Error().Str("level", flagLevel).Msg("no indexing option specified, use -a/--all to build all indexes")
		pflag.Usage()
		return failure
	}

	// Fail if IndexAll is specified along with other index flags, as this would most likely mean that the user does
	// not understand what they are doing.
	if flagIndexAll && (flagIndexCommit || flagIndexHeader || flagIndexPayloads || flagIndexGuarantees ||
		flagIndexCollections || flagIndexTransactions || flagIndexResults || flagIndexEvents || flagIndexSeals) {
		log.Error().Str("level", flagLevel).Msg("-a/--all is mutually exclusive with specific indexing flags")
		pflag.Usage()
		return failure
	}

	// Open index database.
	db, err := badger.Open(dps.DefaultOptions(flagIndex))
	if err != nil {
		log.Error().Str("index", flagIndex).Err(err).Msg("could not open index DB")
		return failure
	}
	defer db.Close()

	// Open protocol state database.
	data, err := badger.Open(dps.DefaultOptions(flagData))
	if err != nil {
		log.Error().Err(err).Msg("could not open blockchain database")
		return failure
	}
	defer data.Close()

	// We initialize a metrics logger regardless of whether metrics are enabled;
	// it will just do nothing if there are no registered metrics.
	mout := output.New(log, flagMetricsInterval)

	// The storage library is initialized with a codec and provides functions to
	// interact with a Badger database while encoding and compressing
	// transparently.
	var codec dps.Codec
	codec, err = zbor.NewCodec()
	if err != nil {
		log.Error().Err(err).Msg("could not initialize storage codec")
		return failure
	}
	if flagMetrics {
		size := rcrowley.NewSize("store")
		mout.Register(size)
		codec = metrics.NewCodec(codec, size)
	}
	storage := storage.New(codec)

	// Check if index already exists.
	_, err = index.NewReader(db, storage).First()
	indexExists := err == nil
	if indexExists && !flagForce {
		log.Error().Err(err).Msg("index already exists, manually delete it or use (-f, --force) to overwrite it")
		return failure
	}

	// The loader component is responsible for loading and decoding the checkpoint.
	load := loader.New(
		loader.WithCheckpointPath(flagCheckpoint),
	)

	nodeID, err := flow.HexStringToIdentifier(flagNodeID)
	if err != nil {
		log.Error().Err(err).Msg("invalid node ID")
		return failure
	}

	accessNodeID, err := flow.HexStringToIdentifier(flagAccessID)
	if err != nil {
		log.Error().Err(err).Msg("invalid access node ID")
		return failure
	}

	downloader, err := bucket.NewDownloader(log, flagRegion, flagBucket)
	if err != nil {
		log.Error().
			Err(err).
			Str("bucket", flagBucket).
			Str("region", flagRegion).
			Msg("could not create S3 bucket downloader")
		return failure
	}

	follower := follower.NewConsensusFollower(nodeID, accessNodeID, flagBindAddr, follower.WithDataDir(flagData))
	consensus := consensus.New(log, follower, data)
	execution := execution.New(log, downloader, data)
	chain := chain.FromFollowers(log, execution, data)

	// FIXME: Replace with a real feeder.
	feed := mocks.BaselineFeeder(&testing.T{})

	// Writer is responsible for writing the index data to the index database.
	index := index.NewWriter(db, storage)
	defer func() {
		err := index.Close()
		if err != nil {
			log.Error().Err(err).Msg("could not close index")
		}
	}()
	write := dps.Writer(index)
	if flagMetrics {
		time := rcrowley.NewTime("write")
		mout.Register(time)
		write = metrics.NewWriter(write, time)
	}

	// Initialize the transitions with the dependencies and add them to the FSM.
	transitions := mapper.NewTransitions(log, load, chain, feed, write,
		mapper.WithIndexCommit(flagIndexAll || flagIndexCommit),
		mapper.WithIndexHeader(flagIndexAll || flagIndexHeader),
		mapper.WithIndexCollections(flagIndexAll || flagIndexCollections),
		mapper.WithIndexGuarantees(flagIndexAll || flagIndexGuarantees),
		mapper.WithIndexTransactions(flagIndexAll || flagIndexTransactions),
		mapper.WithIndexResults(flagIndexAll || flagIndexResults),
		mapper.WithIndexEvents(flagIndexAll || flagIndexEvents),
		mapper.WithIndexPayloads(flagIndexAll || flagIndexPayloads),
		mapper.WithIndexSeals(flagIndexAll || flagIndexSeals),
		mapper.WithSkipBootstrap(flagSkipBootstrap),
	)
	forest := forest.New()
	state := mapper.EmptyState(forest)
	fsm := mapper.NewFSM(state,
		mapper.WithTransition(mapper.StatusEmpty, transitions.BootstrapState),
		mapper.WithTransition(mapper.StatusUpdating, transitions.UpdateTree),
		mapper.WithTransition(mapper.StatusMatched, transitions.CollectRegisters),
		mapper.WithTransition(mapper.StatusCollected, transitions.IndexRegisters),
		mapper.WithTransition(mapper.StatusIndexed, transitions.ForwardHeight),
		mapper.WithTransition(mapper.StatusForwarded, transitions.IndexChain),
	)

	// This section launches the main executing components in their own
	// goroutine, so they can run concurrently. Afterwards, we wait for an
	// interrupt signal in order to proceed with the next section.
	done := make(chan struct{})
	failed := make(chan struct{})
	go func() {
		start := time.Now()
		log.Info().Time("start", start).Msg("Flow DPS Indexer starting")
		err := fsm.Run()
		if err != nil {
			log.Warn().Err(err).Msg("Flow DPS Indexer failed")
			close(failed)
		} else {
			close(done)
		}
		finish := time.Now()
		duration := finish.Sub(start)
		log.Info().Time("finish", finish).Str("duration", duration.Round(time.Second).String()).Msg("Flow DPS Indexer stopped")
	}()

	go func() {
		downloader.Run()
	}()

	// Start metrics output.
	if flagMetrics {
		mout.Run()
	}

	select {
	case <-sig:
		log.Info().Msg("Flow DPS Indexer stopping")
	case <-done:
		log.Info().Msg("Flow DPS Indexer done")
	case <-failed:
		log.Warn().Msg("Flow DPS Indexer aborted")
		return failure
	}
	go func() {
		<-sig
		log.Warn().Msg("forcing exit")
		os.Exit(1)
	}()

	// Stop metrics output.
	if flagMetrics {
		mout.Stop()
	}

	// The following code starts a shut down with a certain timeout and makes
	// sure that the main executing components are shutting down within the
	// allocated shutdown time. Otherwise, we will force the shutdown and log
	// an error. We then wait for shutdown on each component to complete.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = fsm.Stop(ctx)
	if err != nil {
		log.Error().Err(err).Msg("could not stop indexer")
		return failure
	}
	err = downloader.Stop(ctx)
	if err != nil {
		log.Error().Err(err).Msg("could not stop S3 bucket downloader")
		return failure
	}

	return success
}
