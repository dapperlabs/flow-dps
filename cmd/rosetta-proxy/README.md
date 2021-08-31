# Rosetta Proxy

## Description

The Rosetta Proxy is a proxy which forwards HTTP requests to the appropriate spork based on the block height specified in requests.

The current implementation of Rosetta API requires requests to include block height (called Index in the Rosetta nomenclature.)
This allows the proxy to check height boundaries to redirect the request to the matching spork.
If we switch to make requests be based on block hashes instead of block heights, we will need to implement a new routine mechanism.

## Usage

```sh
Usage of rosetta-proxy:
    -c, --config string   path to the configuration file for the proxy (default "./config.yaml")
    -l, --level string    log output level (default "info")
    -p, --port uint16     port on which to bind the proxy (default 8080)
```

## Example

The following command line starts the proxy to serve requests on port 5005 using the spork configuration from the file at `/var/flow/config.yaml`.

```sh
./flow-dps-server -c /var/flow/config.yaml -p 5005
```
