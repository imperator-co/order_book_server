#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::significant_drop_tightening))]
mod listeners;
pub mod metrics;
mod order_book;
mod prelude;
mod servers;
mod types;

use std::path::PathBuf;

use clap::ValueEnum;

pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;

/// Snapshot fetching mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum SnapshotMode {
    /// Use docker exec to call hl-node inside container
    #[default]
    Docker,
    /// Call hl-node directly (for systemctl/bare metal setups)
    Direct,
}

/// Server configuration passed from CLI arguments
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Full address string (e.g., "0.0.0.0:8000")
    pub address: String,
    /// WebSocket compression level (0-9)
    pub compression_level: u32,
    /// Optional base directory for hlnode data
    pub data_dir: Option<PathBuf>,
    /// Include perpetual futures markets
    pub include_perps: bool,
    /// Include spot markets (@ coins, PURR/USDC)
    pub include_spot: bool,
    /// Include HIP-3 markets
    pub include_hip3: bool,
    /// Snapshot fetching mode (docker or direct)
    pub snapshot_mode: SnapshotMode,
    /// Docker container name for exec commands (docker mode only)
    pub docker_container: String,
    /// Path to hl-node binary (direct mode only)
    pub hlnode_binary: String,
    /// Path to abci_state.rmp file (direct mode only, has default)
    pub abci_state_path: Option<PathBuf>,
    /// Path where snapshot will be written (direct mode only, has default)
    pub snapshot_output_path: Option<PathBuf>,
    /// Path to visor_abci_state.json (optional)
    pub visor_state_path: Option<PathBuf>,
    /// Port for Prometheus metrics endpoint (0 to disable)
    pub metrics_port: u16,
    /// BBO-only mode: lightweight mode that only tracks best bid/ask per coin
    /// Disables L2/L4/Trades subscriptions but uses ~100MB RAM instead of 2-3GB
    pub bbo_only: bool,
    /// Resend the last l2Book payload every N ms when nothing has changed.
    /// 0 = disabled (default). Provides a heartbeat for low-liquidity coins
    /// whose snapshot hash rarely changes, matching the official HL API behavior.
    pub l2book_heartbeat_ms: u64,
    /// Resend the last bbo payload every N ms when nothing has changed.
    /// 0 = disabled (default).
    pub bbo_heartbeat_ms: u64,
    /// Tolerate drift: when true, data-loss events are counted in metrics but
    /// never trigger a snapshot re-fetch. The book keeps serving live events
    /// through drift and does NOT self-heal until restarted. Off by default.
    pub no_resync: bool,
    /// Cap on events cached for gapless replay while a snapshot fetch is in
    /// flight (~0.5-1KB resident each). Must cover a full fetch window at
    /// peak event rate or every market-hours re-sync overflows into another
    /// re-sync. 0 = keep the built-in default.
    pub replay_cache_events: usize,
    /// Slots in the broadcast channel that fans every event out to all
    /// connections. A connection that falls this many messages behind loses
    /// the overflow (`channel_drops_total`). 0 = keep the built-in default
    /// ([`DEFAULT_BROADCAST_CAPACITY`]).
    pub broadcast_capacity: usize,
}

/// Default broadcast channel capacity, sized from mainnet traffic (perps,
/// 2026-11): the listener emits one message per node event, ~42k events/s on
/// average (statuses + diffs), up to ~105k in one second and ~18k in a single
/// block. A connection whose socket stalls is dropped after `WS_SEND_TIMEOUT`
/// (5 s), so the channel must hold 5 s of peak traffic: ~170k messages with
/// bbo/l2Book subscribers (BboUpdate ~18k/s mean, 44k/s peak), ~570k once an
/// l4Book/bookDiffs/orderUpdates subscriber adds the per-event L4 messages.
/// 2^20 covers the latter with ~1.8x headroom. Slots cost ~40 B each up
/// front; queued messages (measured ~1.7 KB each for the bbo/l2 mix, ~2.6 KB
/// with L4) are only held while some connection is behind, so a full channel
/// costs ~1.8-2.7 GB at worst.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 1 << 20;
