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
	"encoding/hex"
	"io"
	"os"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog"
	"github.com/spf13/pflag"

	"github.com/optakt/flow-dps/codec/zbor"
	"github.com/optakt/flow-dps/models/dps"
)

const (
	success = 0
	failure = 1
)

func main() {
	os.Exit(run())
}

func run() int {

	// Parse the command line arguments.
	var (
		flagIndex string
		flagLevel string
		flagRaw   bool
	)

	pflag.StringVarP(&flagIndex, "index", "i", "index", "database directory for state index")
	pflag.StringVarP(&flagLevel, "level", "l", "info", "log output level")
	pflag.BoolVarP(&flagRaw, "raw", "r", false, "use raw binary output instead of hexadecimal")

	pflag.Parse()

	// Initialize the logger.
	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }
	log := zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.DebugLevel)
	level, err := zerolog.ParseLevel(flagLevel)
	if err != nil {
		log.Error().Str("level", flagLevel).Err(err).Msg("could not parse log level")
		return failure
	}
	log = log.Level(level)

	// Open the index database.
	db, err := badger.Open(dps.ReadOptions(flagIndex).WithReadOnly(true).WithBypassLockGuard(true))
	if err != nil {
		log.Error().Str("index", flagIndex).Err(err).Msg("could not open badger db")
		return failure
	}
	defer db.Close()

	// Write to stdout and wrap the writer into a hex encoder if output is set to be hexadecimal.
	var writer io.Writer
	writer = os.Stdout
	if !flagRaw {
		writer = hex.NewEncoder(writer)
	}

	// Create a compressor to make sure only compressed bytes are piped into the writer.
	compressor, err := zstd.NewWriter(writer,
		zstd.WithEncoderDict(zbor.Dictionary),
	)
	if err != nil {
		log.Error().Err(err).Msg("could not initialize compressor")
		return failure
	}
	defer compressor.Close()

	// Run the DB backup mechanism on top of the writer to directly
	// write the output.
	_, err = db.Backup(compressor, 0)
	if err != nil {
		log.Error().Err(err).Msg("could not backup database")
		return failure
	}

	return success
}
