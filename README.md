# Flow Data Provisioning Service

[![CI Status](https://img.shields.io/github/workflow/status/optakt/flow-dps/MasterCI?logo=GitHub%20Actions&label=&logoColor=silver&labelColor=gray)](https://github.com/optakt/flow-dps/actions/workflows/master.yml)
[![License](https://img.shields.io/github/license/nanomsg/mangos.svg?logoColor=silver&logo=Open%20Source%20Initiative&label=&color=blue)](https://github.com/optakt/flow-dps/blob/master/LICENSE)
[![Documentation](https://img.shields.io/badge/godoc-docs-blue.svg?label=&logo=go)](https://pkg.go.dev/github.com/optakt/flow-dps)
[![Internal Documentation](https://img.shields.io/badge/-documentation-grey?logo=markdown)](./docs/introduction.md)

The Flow Data Provisioning Service (DPS) aims at providing a scalable and efficient way to access the history of the Flow
execution state, both for current live sporks and for past sporks.

The state of past sporks is indexed by reading an execution node's protocol state and state trie write-ahead log.
Optionally, a root checkpoint is required to bootstrap state before a spork's start. In more specific terms, indexing
of past sporks requires a Badger key-value database containing the Flow protocol state of the spork and a LedgerWAL with
all the trie updates that happened on the spork.

Indexing the live spork works similarly, but it reads the protocol state by acting as a consensus follower, and it reads
the execution-related data from records written to a Google Cloud Storage bucket by an execution node.

The Flow DPS maintains multiple specialized indexes for different purposes.
Contrary to the execution node's state trie, the index for ledger registers allows random access to the execution state at any block height
which enables state retrieval at any point in history, overcoming the pruning limit seen on the execution node.

## Documentation

### Binaries

Below are links to the individual documentation for the binaries within this repository.

* [`flow-dps-client`](./cmd/flow-dps-client/README.md)
* [`flow-dps-indexer`](./cmd/flow-dps-indexer/README.md)
* [`flow-dps-live`](./cmd/flow-dps-live/README.md)
* [`flow-dps-server`](./cmd/flow-dps-server/README.md)

### Docker

A simple Dockerfile can be used to easily build a docker image with a selected binary. Use build argument `BINARY` with 
value equal to directory name from `cmd/` folder to build appropriate binary into an image.

### Scripts

`scripts/` folder contains examples how to run DPS for given network, prepare and download the appropriate data

### APIs

The DPS API gives access to historical data at any given height.

* [DPS API](./docs/dps-api.md)

There are also additional API layers that can be run on top of the DPS API:

* [Access API](https://github.com/optakt/flow-dps-access)
* [Rosetta API](https://github.com/optakt/flow-dps-rosetta)

### Developer Documentation

* [Introduction](./docs/introduction.md)
* [Architecture](./docs/architecture.md)
* [Database Schema](./docs/database.md)
* [Snapshots](./docs/snapshots.md)

## Dependencies

Go `v1.16` or higher is required to compile `flow-dps`.
Only Linux amd64 builds are supported, because of the dependency to the [`flow-go/crypto`](https://github.com/onflow/flow-go/tree/master/crypto) package.
Please note that it is also required to make sure that your `GOPATH` is exported in your environment in order to generate the DPS API.

If you want to make changes to the GRPC API, the following dependencies are required as well.

* [`protoc`](https://grpc.io/docs/protoc-installation/) version `3.17.3`
* `go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.26`
* `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1`
* `go install github.com/srikrsna/protoc-gen-gotag@v0.6.1`

Once they are installed, you can run `go generate ./...` from the root of this repository to update the generated protobuf files.

In order to build the live binary, the following extra steps and dependencies are required:

* [`CMake`](https://cmake.org/install/)

Please note that the flow-go repository should be cloned in the same folder as the DPS with its default name, so that the Go module replace statement works as intended: `replace github.com/onflow/flow-go/crypto => ./flow-go/crypto`.

* `git clone git@github.com:onflow/flow-go.git`
* `cd flow-go/crypto`
* `git checkout c0afa789365eb7a22713ed76b8de1e3efaf3a70a`
* `go generate`

You can then verify that the installation of the flow-go crypto package has been successful by running the tests of the project.

## Build

You can build every binary by running `go build -tags=relic -o . ./...` from the root of the repository.