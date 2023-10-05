# Bifrost Gateway Environment Variables

`bifrost-gateway` ships with some implicit defaults that can be adjusted via env variables below.

- [Configuration](#configuration)
  - [`KUBO_RPC_URL`](#kubo_rpc_url)
  - [`BLOCK_CACHE_SIZE`](#block_cache_size)
  - [`GRAPH_BACKEND`](#graph_backend)
- [Proxy Backend](#proxy-backend)
  - [`PROXY_GATEWAY_URL`](#proxy_gateway_url)
- [Saturn Backend](#saturn-backend)
  - [`STRN_ORCHESTRATOR_URL`](#strn_orchestrator_url)
  - [`STRN_LOGGER_URL`](#strn_logger_url)
  - [`STRN_LOGGER_SECRET`](#strn_logger_secret)
- [Logging](#logging)
  - [`GOLOG_LOG_LEVEL`](#golog_log_level)
  - [`GOLOG_LOG_FMT`](#golog_log_fmt)
  - [`GOLOG_FILE`](#golog_file)
  - [`GOLOG_TRACING_FILE`](#golog_tracing_file)
- [Testing](#testing)
  - [`GATEWAY_CONFORMANCE_TEST`](#gateway_conformance_test)
  - [`IPFS_NS_MAP`](#ipfs_ns_map)

## Configuration


### `KUBO_RPC_URL`

Single URL or a comma separated list of RPC endpoints that provide `/api/v0` from Kubo.
This is used to redirect legacy `/api/v0` commands that need to be handled on `ipfs.io`.
If this is not set, the redirects are not set up.

### `BLOCK_CACHE_SIZE`

Default: see `DefaultCacheBlockStoreSize`

The size of in-memory [2Q cache](https://pkg.go.dev/github.com/hashicorp/golang-lru/v2#TwoQueueCache) with recently used and most requently used blocks.

### `GRAPH_BACKEND`

Default: `false`

When set to `true`, requests to backend will use `?format=car` in addition to
`?format=raw` to reduce the number of round trips.

This is an experimental feature that depends on pathing, `dag-scope` and `entity-bytes`
parameters from [IPIP-402](https://github.com/ipfs/specs/pull/402).

Currently only `https://l1s.strn.pl` supports it, but our intention is to
standardize it ([IPIP-402](https://github.com/ipfs/specs/pull/402)) and add it
to the [trustless gateway spec](https://specs.ipfs.tech/http-gateways/trustless-gateway/)
and [boxo/gateway](https://github.com/ipfs/boxo/pull/303) reference library
in the near feature.

## Proxy Backend

### `PROXY_GATEWAY_URL`

Single URL or a comma separated list of Gateway endpoints that support `?format=block|car|ipns-record`
responses. Either this variable or `STRN_ORCHESTRATOR_URL` must be set.

If this gateway does not support `application/vnd.ipfs.ipns-record`, you can use `IPNS_RECORD_GATEWAY_URL`
to override the gateway address from which to retrieve IPNS Records from.

### `IPNS_RECORD_GATEWAY_URL`

Single URL or a comma separated list of Gateway endpoints that support requests for `application/vnd.ipfs.ipns-record`.
This is used for IPNS Record routing.

`IPNS_RECORD_GATEWAY_URL` also supports [Routing V1 HTTP API](https://specs.ipfs.tech/routing/http-routing-v1/)
for IPNS Record routing ([IPIP-379](https://specs.ipfs.tech/ipips/ipip-0379/)). To use it, the provided URL must end with `/routing/v1`.

If not set, the IPNS records will be fetched from `KUBO_RPC_URL`.

## Saturn Backend

### `STRN_ORCHESTRATOR_URL`

Saturn Orchestrator that will provide a list of  useful Saturn L1s (CDN points of presence).

### `STRN_LOGGER_URL`

Saturn Logger used for fraud detection.

### `STRN_LOGGER_SECRET`

JWT token provided by Saturn CDN. Staging (testnet) and production (mainnet)
should use different tokens.

## Logging

### `GOLOG_LOG_LEVEL`

Specifies the log-level, both globally and on a per-subsystem basis. Level can
be one of:

* `debug`
* `info`
* `warn`
* `error`
* `dpanic`
* `panic`
* `fatal`

Per-subsystem levels can be specified with `subsystem=level`.  One global level
and one or more per-subsystem levels can be specified by separating them with
commas.

Default: `error`

Example:

```console
GOLOG_LOG_LEVEL="error,bifrost-gateway=debug,caboose=debug" bifrost-gateway
```

### `GOLOG_LOG_FMT`

Specifies the log message format.  It supports the following values:

- `color` -- human readable, colorized (ANSI) output
- `nocolor` -- human readable, plain-text output.
- `json` -- structured JSON.

For example, to log structured JSON (for easier parsing):

```bash
export GOLOG_LOG_FMT="json"
```
The logging format defaults to `color` when the output is a terminal, and
`nocolor` otherwise.

### `GOLOG_FILE`

Sets the file to which the Bifrost Gateway logs. By default, the Bifrost Gateway
logs to the standard error output.

### `GOLOG_TRACING_FILE`

Sets the file to which the Bifrost Gateway sends tracing events. By default,
tracing is disabled.

Warning: Enabling tracing will likely affect performance.


## Testing

### `GATEWAY_CONFORMANCE_TEST`

Setting to `true` enables support for test fixtures required by [ipfs/gateway-conformance](https://github.com/ipfs/gateway-conformance) test suite.

### `IPFS_NS_MAP`

Adds static namesys records for deterministic tests and debugging.
Useful for testing `/ipns/` support without having to do real IPNS/DNS lookup.

Example:

```console
$ IPFS_NS_MAP="dnslink-test1.example.com:/ipfs/bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am,dnslink-test2.example.com:/ipns/dnslink-test1.example.com" ./gateway-binary
...
$ curl -is http://127.0.0.1:8081/dnslink-test2.example.com/ | grep Etag
Etag: "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am"
```
