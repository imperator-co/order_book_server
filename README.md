# Hyperliquid Orderbook WebSocket Server

## Disclaimer

This was a standalone project, not written by the Hyperliquid Labs core team. It is made available "as is", without warranty of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, or noninfringement. Use at your own risk.

This project has been further developed and maintained by [Imperator](https://hyperpc.app) to make the Hyperliquid public release production-ready and more efficient. Imperator provides no warranty, guarantee, or support obligation of any kind. The software is provided "as is" and you assume all risks associated with its use, including but not limited to data loss, system failure, or any other damages. Under no circumstances shall Imperator be held liable for any claim, damages, or other liability arising from the use of this software.

## Features

Real-time orderbook data from a local Hyperliquid node:

- **bbo** - Best Bid/Offer (top of book) with deduplication
- **l2Book** - Aggregated Level 2 orderbook with deduplication
- **trades** - Real-time trade feed
- **l4Book** - Full Level 4 orderbook with individual order details
- **orderUpdates** - User-specific order status stream

## Quick Start

### Prerequisites

1. **Hyperliquid Node** - Running with streaming enabled (Docker or systemctl)
2. **Rust** - For building from source

### Build

```bash
git clone https://github.com/imperator-co/order_book_server.git
cd order_book_server
cargo build --release
```

### Run

**Docker mode** (node running via `docker compose`):
```bash
./target/release/orderbook_server \
    --address 0.0.0.0 \
    --port 8000 \
    --data-dir /root/.hyperliquid_rpc_hlnode_mainnet/volumes/hl/data
```

**Direct mode** (node running via systemctl / bare metal):
```bash
./target/release/orderbook_server \
    --snapshot-mode direct \
    --hlnode-binary /path/to/hl-node \
    --data-dir /path/to/volumes/hl/data
```

## Configuration

### Core Options

| Flag | Default | Description |
|------|---------|-------------|
| `--address` | `0.0.0.0` | Bind address |
| `--port` | `8000` | WebSocket port |
| `--compression-level` | `1` | WebSocket compression level (0-9). See [Compression](#compression) |
| `--markets` | `all` | `perps`, `spot`, `hip3`, `all` |
| `--log-level` | `info` | `error`, `warn`, `info`, `debug`, `trace` |

### Compression

WebSocket messages (especially L2/L4 snapshots) can be large. The `--compression-level` flag controls `permessage-deflate` compression applied to every outgoing frame:

| Level | Behavior | Use case |
|-------|----------|----------|
| `0` | Disabled | Lowest latency. Use for HFT or when your client doesn't support `permessage-deflate` |
| `1` | Fast compression | Best tradeoff for most setups. Minimal CPU cost, significant bandwidth savings |
| `5` | Balanced | Good compression ratio with moderate CPU |
| `9` | Best ratio | Maximum compression. Higher CPU cost, useful for bandwidth-constrained links |

Your WebSocket client must support `permessage-deflate` for levels 1-9 to have any effect. If it doesn't, use `0`.

### Snapshot Mode

On startup, the server needs a **full L4 orderbook snapshot** to initialize its in-memory state. It obtains this by calling the `hl-node` binary's CLI, which reads the node's `abci_state.rmp` file (the node's persistent state) and dumps a JSON snapshot of every order currently on the book.

The `--snapshot-mode` flag controls *how* the server invokes `hl-node`:

**`docker` (default)** - Use when your Hyperliquid node runs inside a Docker container (the standard `docker compose` setup). The server runs `docker exec <container> hl-node --dump-abci-state ...` to execute the snapshot command inside the container, where `hl-node` and the state files are accessible.

**`direct`** - Use when your node runs directly on the host via systemctl or bare metal. The server calls the `hl-node` binary directly on the host to generate the snapshot.

After the initial snapshot, the server stays up to date by watching the node's `*_streaming/` directories for real-time order diffs, fills, and status updates via inotify. No further snapshots are needed unless the server restarts.

| Flag | Default | Description |
|------|---------|-------------|
| `--snapshot-mode` | `docker` | `docker` or `direct` |
| `--docker-container` | `hyperliquid_hlnode` | Container name for `docker exec` (docker mode only) |
| `--hlnode-binary` | `hl-node` | Path to hl-node binary on host (direct mode only) |
| `--data-dir` | `~` | Path to the folder containing `node_fills_streaming/`, `node_order_statuses_streaming/`, and `node_raw_book_diffs_streaming/`. This is where the node writes its real-time event files |
| `--abci-state-path` | auto | Path to `abci_state.rmp`. Auto-detected at `<data-dir>/hl/hyperliquid_data/abci_state.rmp` in direct mode. Override if your node stores state in a non-standard location |
| `--snapshot-output-path` | auto | Path where `hl-node` writes its JSON snapshot output. Defaults to `/tmp/hl_snapshot.json`. Override if `/tmp` is not writable or you want snapshots stored elsewhere |
| `--visor-state-path` | auto | Path to `visor_abci_state.json`, which contains the current block height. Auto-detected relative to `--data-dir`. Override if your visor state is in a non-standard location |

### Market Types

| Value | Description |
|-------|-------------|
| `perps` | Perpetual futures only (BTC, ETH, SOL...) |
| `spot` | Spot markets (`@*` coins, PURR/USDC) |
| `hip3` | HIP-3 markets (X:Y format, e.g., xyz:TSLA) |
| `all` | All markets (default) |

### Advanced Options

| Flag | Default | Description |
|------|---------|-------------|
| `--metrics-port` | `9090` | Prometheus metrics port (0 to disable) |
| `--bbo-only` | `false` | Lightweight BBO-only mode (~100MB RAM instead of ~2-3GB). Disables L2/L4/Trades subscriptions |

## Recommended Configurations

### Low Latency Trading

Optimized for sub-millisecond BBO updates. Compression is disabled to eliminate encoding overhead. Market scope is narrowed to perps only, reducing the number of coins tracked and memory usage.

```bash
./target/release/orderbook_server \
    --compression-level 0 \
    --markets perps \
    --data-dir /path/to/data
```

### General Purpose

Balanced configuration for dashboards, analytics, or multi-market monitoring. Light compression (`1`) provides significant bandwidth savings with negligible CPU cost.

```bash
./target/release/orderbook_server \
    --compression-level 1 \
    --markets all \
    --data-dir /path/to/data
```

### BBO-Only (Lightweight)

Track only the top-of-book bid/ask for all coins. Uses ~100MB RAM instead of 2-3GB. Ideal for price feeds, alerting, or environments with limited memory.

```bash
./target/release/orderbook_server \
    --bbo-only \
    --compression-level 0 \
    --data-dir /path/to/data
```

## WebSocket API

### Subscribe to BBO
```json
{ "method": "subscribe", "subscription": { "type": "bbo", "coin": "BTC" } }
```
Response:
```json
{ "channel": "bbo", "data": { "coin": "BTC", "time": 1702530000000, "bid": { "px": "100000.0", "sz": "0.5", "n": 1 }, "ask": { "px": "100001.0", "sz": "0.3", "n": 1 } } }
```

### Subscribe to Trades
```json
{ "method": "subscribe", "subscription": { "type": "trades", "coin": "BTC" } }
```
Response:
```json
{ "channel": "trades", "data": [{ "coin": "BTC", "side": "A", "px": "106296.0", "sz": "0.00017", "time": 1751430933565, "hash": "0x...", "tid": 293353986402527, "user": "0x..." }] }
```
Trades that are liquidations include an additional `liquidation` field with `liquidatedUser`, `markPx`, and `method`.

### Subscribe to L2 Orderbook
```json
{ "method": "subscribe", "subscription": { "type": "l2Book", "coin": "BTC" } }
```
Optional parameters: `nSigFigs` (2-5), `nLevels` (max 100, default 20), `mantissa` (2 or 5)

### Subscribe to L4 Orderbook
```json
{ "method": "subscribe", "subscription": { "type": "l4Book", "coin": "BTC" } }
```
> **Warning:** The initial L4 snapshot contains every individual order in the book and can be **very large** (several MB for liquid coins like BTC/ETH). Some WebSocket clients (e.g., Postman) may not handle payloads of this size. Use a capable client like `wscat` or `websocat`, or connect programmatically. After the initial snapshot, subsequent updates are incremental and lightweight.

### Subscribe to Order Updates (User-Specific)
Stream raw order status data for a specific user address:
```json
{ "method": "subscribe", "subscription": { "type": "orderUpdates", "user": "0x1234567890abcdef1234567890abcdef12345678" } }
```
Response:
```json
{ "channel": "orderUpdates", "data": [{ "user": "0x...", "time": 1702530000000, "height": 12345, "orderStatus": { "time": "...", "user": "0x...", "hash": "0x...", "status": "open", "order": { "coin": "BTC", "side": "B", "limitPx": "100000.0", "sz": "0.5", ... } } }] }
```
> **Note:** Requires node to run with `--write-order-statuses` flag enabled.

### Ping/Pong
```json
{ "method": "ping" }
```
Response:
```json
{ "channel": "pong" }
```

### Unsubscribe
```json
{ "method": "unsubscribe", "subscription": { "type": "l2Book", "coin": "BTC" } }
```

## Node Requirements

The Hyperliquid node must run with **all** of these flags enabled:
- `--write-fills`
- `--write-order-statuses`
- `--write-raw-book-diffs`
- `--stream-with-block-info` — **required**, the server only reads from `*_streaming` directories
- `--disable-output-file-buffering` — ensures data is flushed immediately for low latency

## Architecture

```
┌──────────────────────┐     ┌──────────────────────────────────────────────┐
│   Hyperliquid Node   │     │  Orderbook Server                           │
│   (Docker/Direct)    │     │                                             │
│                      │     │  ┌──────────────────────────────┐           │
│  writes to:          │     │  │ Parallel File Watchers       │           │
│  - fills_streaming/  │─────▶  │ (3 inotify threads)          │           │
│  - order_statuses_   │     │  │  - order diffs (BBO-critical)│           │
│    streaming/        │     │  │  - order statuses            │           │
│  - book_diffs_       │     │  │  - fills                     │           │
│    streaming/        │     │  └──────────┬───────────────────┘           │
│                      │     │             │ crossbeam → tokio bridge      │
│                      │     │  ┌──────────▼───────────────────┐           │
│  snapshot via:       │     │  │ OrderBook State              │           │
│  hl-node CLI ◀───────│─────│  │  - L4 in-memory book         │           │
│  (startup only)      │     │  │  - L2 snapshot computation   │           │
│                      │     │  │  - BBO deduplication         │           │
│                      │     │  └──────────┬───────────────────┘           │
│                      │     │             │ broadcast channel             │
│                      │     │  ┌──────────▼──────┐  ┌─────────────────┐  │
│                      │     │  │ WebSocket Server │──│ Connected       │  │
│                      │     │  │ (axum + yawc)    │  │ Clients         │  │
└──────────────────────┘     │  └──────────────────┘  └─────────────────┘  │
                             │                                             │
                             │  ┌─────────────────┐                        │
                             │  │ Metrics Server   │  GET /metrics         │
                             │  │ (Prometheus)     │  GET /health          │
                             │  └─────────────────┘                        │
                             └──────────────────────────────────────────────┘
```

**Data flow:**
1. The Hyperliquid node writes real-time events to `*_streaming/` directories as newline-delimited JSON
2. Three parallel inotify file watchers detect changes immediately (one per event source)
3. Events are bridged from crossbeam channels (blocking I/O threads) to the tokio async runtime
4. The OrderBook State applies diffs/statuses independently (no block-level batching) for lowest latency
5. Changed BBOs and L2 snapshots are broadcast to subscribed WebSocket clients with deduplication

## Performance

### Deduplication

| Type | Behavior |
|------|----------|
| BBO | Only sends when bid/ask px/sz changes |
| L2Book | Only sends when snapshot hash changes |
| Trades | Only sends on fills |

### Latency

| Metric | Value |
|--------|-------|
| BBO update frequency | ~100+/sec (streaming) |
| BBO latency | ~100ms |
| BBO dedup overhead | ~1us |
| L2 dedup overhead | ~10us |
| Savings when unchanged | ~500us |

## Prometheus Metrics

Metrics are exposed on port 9090 by default (configurable via `--metrics-port`):

```bash
curl http://localhost:9090/metrics
```

### Available Metrics

| Category | Metric | Description |
|----------|--------|-------------|
| **Connections** | `ws_connections_active` | Current WebSocket connections |
| | `ws_connections_total` | Total connections since startup |
| | `ws_subscriptions_active{type}` | Active subscriptions by type (bbo/l2Book/l4Book/trades/orderUpdates) |
| | `broadcast_receivers` | Number of broadcast channel receivers |
| **Throughput** | `events_processed_total{type}` | Events by type (orders/diffs/fills) |
| | `broadcasts_total{channel}` | Broadcasts by channel (bbo/l2/l4/trades) |
| | `messages_sent_total` | Total WebSocket messages sent |
| | `bbo_changes_total{coin}` | BBO changes per coin |
| **Health** | `orderbook_height` | Current block height |
| | `orderbook_time_ms` | Orderbook timestamp |
| | `orderbook_orders_total` | Total orders in the book |
| | `orderbook_coins_count` | Number of coins tracked |
| | `pending_orders_cache_size` | Pending order statuses in HFT cache |
| | `pending_diffs_cache_size` | Pending book diffs in HFT cache |
| | `uptime_seconds` | Server uptime in seconds |
| | `server_start_time_seconds` | Server start timestamp (unix) |
| **Latency** | `bbo_broadcast_latency_seconds` | BBO broadcast latency histogram |
| | `l2_broadcast_latency_seconds` | L2 broadcast latency histogram |
| | `event_processing_latency_seconds{event_type}` | Per-event processing latency |
| **File Watcher** | `file_events_total{source}` | File events received by source |
| | `file_lines_parsed_total{source}` | Lines parsed from files by source |
| **Errors** | `parse_errors_total{type}` | JSON parse errors by source |
| | `ws_send_errors_total` | WebSocket send errors |
| | `channel_drops_total` | Messages dropped due to lag |
| | `broadcast_channel_lag` | Broadcast channel lag (receivers behind) |

### Disable Metrics

```bash
./target/release/orderbook_server --metrics-port 0
```

## Health Check

A health endpoint is available on the same port as the WebSocket server:

```bash
curl http://localhost:8000/health
```

Response:
```json
{"status":"ready","uptime_seconds":3600,"height":123456,"connections":5}
```

| Field | Description |
|-------|-------------|
| `status` | `ready` or `initializing` |
| `uptime_seconds` | Server uptime |
| `height` | Current block height |
| `connections` | Active WebSocket connections |

## Deployment

### Systemd Service

An example service file is included (`orderbook-server.service`):

```bash
# Copy and edit the service file
cp orderbook-server.service /etc/systemd/system/
vim /etc/systemd/system/orderbook-server.service  # adjust paths

# Enable and start
systemctl daemon-reload
systemctl enable orderbook-server
systemctl start orderbook-server

# View logs
journalctl -u orderbook-server -f
```

## Caveats

- **No untriggered orders** - Only shows orders on the book
- **Snapshot sync time** - Initial snapshot takes ~10-30 seconds

## Differences from the Hyperliquid Public Release

This project started from the [Hyperliquid public orderbook server](https://github.com/hyperliquid-dex/order_book) and was substantially rewritten for production use. Here is what changed:

### Event Processing: Block-Batched vs Event-by-Event

The original server batches events by block number and waits for an entire block's worth of updates before applying them. This adds latency proportional to block time.

This fork processes every order diff, status, and fill **the instant it arrives** from the node's streaming files, without waiting for block boundaries. The result is ~100+ BBO updates per second with ~100ms end-to-end latency.

### File Watching: Single Thread vs 3 Parallel Threads

The original uses a single file watcher thread that handles all three event sources (order statuses, book diffs, fills) sequentially.

This fork spawns **3 dedicated inotify threads** (one per event source) with independent crossbeam channels bridged to the tokio async runtime. Order diffs (the BBO-critical path) are never blocked by slow fill or status parsing.

### New Subscription Types

| Feature | Original | This Fork |
|---------|----------|-----------|
| L2Book | Yes | Yes |
| L4Book | Yes | Yes |
| Trades | Yes | Yes |
| **BBO** | No | Yes - dedicated top-of-book feed with per-coin deduplication |
| **orderUpdates** | No | Yes - per-user order status stream (filter by address) |

### Deduplication

The original sends every update to every subscriber regardless of whether anything changed.

This fork deduplicates at the WebSocket level:
- **BBO**: only sends when bid/ask px/sz actually changes (~1us overhead)
- **L2Book**: only sends when the snapshot hash changes (~10us overhead)
- Saves ~500us per unchanged update and significantly reduces client-side bandwidth

### BBO-Only Lightweight Mode

New `--bbo-only` flag reduces memory from ~2-3GB to ~100MB by only tracking the top-of-book bid/ask per coin. L2/L4/Trades subscriptions are disabled. Useful for price feeds, alerting, or memory-constrained environments.

### Snapshot Modes: Docker & Direct

The original fetches snapshots via HTTP POST to `localhost:3001` (the node's local RPC).

This fork calls `hl-node --dump-abci-state` directly, with two modes:
- **Docker**: `docker exec <container> hl-node ...` for container-based setups
- **Direct**: calls the binary on the host for systemctl / bare metal deployments

All paths (`abci_state.rmp`, `snapshot.json`, `visor_abci_state.json`) are auto-detected with manual override options.

### Market Filtering

The original has a hard-coded `ignore_spot` flag. This fork adds a `--markets` CLI flag supporting `perps`, `spot`, `hip3` (HIP-3 tokens in X:Y format), or `all`.

### Prometheus Metrics & Health Endpoint

The original has no monitoring. This fork adds:
- **`/health`** endpoint with status, uptime, block height, and connection count
- **`/metrics`** endpoint exposing 25+ Prometheus metrics covering connections, subscriptions, latency histograms, throughput, orderbook health, parse errors, and file watcher stats
- Pre-built Grafana dashboard included in `monitoring/`

### WebSocket Compression

Both use `yawc` with `permessage-deflate`. This fork increases the broadcast channel buffer from 100 to 256 to reduce "channel lagged" drops under load, and adds detailed documentation on compression level tradeoffs.

### Other Improvements

- **Graceful shutdown** via `Ctrl+C` / `SIGINT` signal handling
- **Configurable log levels** (`--log-level error|warn|info|debug|trace`)
- **Faster JSON parsing** with `sonic-rs` on the hot path
- **Systemd service file** included for production deployment
- **Comprehensive CLI** with auto-detected defaults and full override support

### Summary

| | Original | This Fork |
|---|----------|-----------|
| Event model | Block-batched | Event-by-event |
| File watchers | 1 thread | 3 parallel threads |
| BBO subscription | No | Yes + dedup |
| Order updates | No | Yes (per-user) |
| BBO-only mode | No | Yes (~100MB) |
| Metrics | None | 25+ Prometheus metrics |
| Health endpoint | No | Yes |
| Snapshot modes | HTTP RPC | Docker + Direct CLI |
| Market filtering | Hard-coded | --markets flag |
| Graceful shutdown | No | Yes |
| JSON parser | serde_json | sonic-rs |

## Managed Orderbook Service

If you'd rather skip running your own infrastructure, or you need capabilities beyond what this open-source server provides, check out our managed offering at **[hyperpc.app/products/data/orderbook-websocket](https://hyperpc.app/products/data/orderbook-websocket)**.

- **Hosted orderbook feeds** - Connect directly and consume real-time BBO, L2, L4, and trade data without managing a node or this server yourself
- **Lower latency via direct sentry peering** - We can peer your orderbook server directly with a Hyperliquid sentry node for reduced hop count and tighter latencies
- **Extended data services** - Recurring L4 snapshots, liquidation feeds, historical orderbook data, and custom data pipelines tailored to your trading infrastructure
- **Custom deployments** - Dedicated instances, co-located setups, or private infrastructure built around your specific latency and throughput requirements

Reach out to us at [hyperpc.app](https://hyperpc.app) to discuss what you need.

## License

MIT
