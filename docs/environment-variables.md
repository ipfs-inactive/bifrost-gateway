# Bifrost Gateway Environment Variables

`bifrost-gateway` ships with some implicit defaults that can be adjusted via env variables below.

- [Configuration](#configuration)
  - [`KUBO_RPC_URL`](#kubo_rpc_url)
  - [`BLOCK_CACHE_SIZE`](#block_cache_size)
- [Proxy Backend](#proxy-backend)
- [Saturn Backend](#saturn-backend)
  - [`STRN_ORCHESTRATOR_URL`](#strn_orchestrator_url)
  - [`STRN_LOGGER_URL`](#strn_logger_url)
  - [`STRN_LOGGER_SECRET`](#strn_logger_secret)
- [Logging](#logging)
  - [`GOLOG_LOG_LEVEL`](#golog_log_level)
  - [`GOLOG_LOG_FMT`](#golog_log_fmt)
  - [`GOLOG_FILE`](#golog_file)
  - [`GOLOG_TRACING_FILE`](#golog_tracing_file)

## Configuration


### `KUBO_RPC_URL`

Single URL or a comma separated list of RPC endpoints that provide `/api/v0` from Kubo.

We use this as temporary solution for IPNS Record routing until [IPIP-351](https://github.com/ipfs/specs/pull/351) ships with Kubo 0.19,
and we also redirect some legacy `/api/v0` commands that need to be handled on `ipfs.io`.

### `BLOCK_CACHE_SIZE`

The size of in-memory [2Q cache](https://pkg.go.dev/github.com/hashicorp/golang-lru/v2#TwoQueueCache) with recently used and most requently used blocks.

## Proxy Backend

TODO: this will be the default backend used when `STRN_ORCHESTRATOR_URL` is not set.
We will have env variable that allows customizing URL of HTTP Gateway that supports Block/CAR responses.

## Saturn Backend

### `STRN_ORCHESTRATOR_URL`

Saturn Orchestrator that will provide a list of  useful Saturn L1s (CDN points of presence).

### `STRN_LOGGER_URL`

Saturn Logger used for fraud detection.

### `STRN_LOGGER_SECRET`

TODO [bifrost-gateway/issues/43](https://github.com/ipfs/bifrost-gateway/issues/43): Saturn Logger JWT used for fraud detection.

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
