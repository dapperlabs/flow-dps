# Flow DPS Live

## Description

### DPS API
The Flow DPS Live binary implements the core functionality to create the index for live sporks.
It needs access to a Google Cloud Storage bucket containing the execution state in the form of block data files, as well as access to the Flow network as an unstaked consensus follower.
The index is generated in the form of a Badger database that allows random access to any ledger register at any block height.

### Access API
The Flow Access Server runs on top of a DPS index to implement the [Flow Access API](https://developers.flow.com/nodes/access-api).
Both the Flow DPS Indexer and the Flow DPS Live tool can create such an index.
In the case of the indexer, the index is static and built from a previous spork's state.
For the live tool, the index is dynamic and updated on an ongoing basis from the data sent from a Flow execution node.


## Usage

```sh
Usage of flow-archive-live:
  -A, --address-access string     address to serve Access GRPC API on (default "127.0.0.1:9000")
  -a, --address string            bind address for serving DPS API (default "127.0.0.1:5005")
  -b, --bootstrap string          path to directory with bootstrap information for spork (default "bootstrap")
  -u, --bucket string             Google Cloude Storage bucket with block data records
  -c, --checkpoint string         path to root checkpoint file for execution state trie
  -d, --data string               path to database directory for protocol data (default "data")
  -f, --force                     force indexing to bootstrap from root checkpoint and overwrite existing index
  -i, --index string              path to database directory for state index (default "index")
  -l, --level string              log output level (default "info")
  -m, --metrics string            address on which to expose metrics (no metrics are exposed when left empty)
  -s, --skip                      skip indexing of execution state ledger registers
      --flush-interval duration   interval for flushing badger transactions (0s for disabled)
      --seed-address string       host address of seed node to follow consensus
      --seed-key string           hex-encoded public network key of seed node to follow consensus

```

## Example

The below command line starts indexing a live spork.

```sh
./flow-archive-live -u flow-block-data -i /var/flow/index -d /var/flow/data -c /var/flow/bootstrap/root.checkpoint -b /var/flow/bootstrap/public --seed-address access.canary.nodes.onflow.org:9000 --seed-key cfce845fa9b0fb38402640f997233546b10fec3f910bf866c43a0db58ab6a1e4
```
