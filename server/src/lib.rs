#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
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
}
