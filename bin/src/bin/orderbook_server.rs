use std::net::Ipv4Addr;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use server::{Result, ServerConfig, SnapshotMode, run_websocket_server};

/// Markets to include in the orderbook
#[derive(Debug, Clone, Copy, ValueEnum, Default)]
pub enum Markets {
    /// Perpetual futures only
    Perps,
    /// Spot markets only (including @ coins)
    Spot,
    /// HIP-3 markets only
    Hip3,
    /// All markets (perps + spot + hip3)
    #[default]
    All,
}

#[derive(Debug, Parser)]
#[command(author, version, about = "Real-time Orderbook WebSocket Server for Hyperliquid")]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long, default_value = "0.0.0.0")]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long, default_value = "8000")]
    port: u16,

    /// Compression level for WebSocket connections (0-9).
    /// 0 = disabled, 1 = fastest (default), 9 = best ratio
    #[arg(long, default_value = "1")]
    compression_level: u32,

    /// Base directory for hlnode data files.
    /// For Docker: the directory containing .hyperliquid_rpc_hlnode_mainnet/
    /// For Direct: the directory containing hl/hyperliquid_data/
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Which markets to include: perps, spot, hip3, all
    #[arg(long, value_enum, default_value = "all")]
    markets: Markets,

    // ========== Snapshot Configuration ==========
    /// Snapshot fetching mode: docker or direct
    /// - docker: Use 'docker exec <container> hl-node ...' (for Docker users)
    /// - direct: Call 'hl-node ...' directly (for systemctl/bare metal users)
    #[arg(long, value_enum, default_value = "docker")]
    snapshot_mode: SnapshotMode,

    /// Docker container name (only used in docker mode)
    #[arg(long, default_value = "hyperliquid_hlnode")]
    docker_container: String,

    /// Path to hl-node binary (only used in direct mode).
    /// Default: 'hl-node' (assumes in PATH)
    #[arg(long, default_value = "hl-node")]
    hlnode_binary: String,

    /// Path to abci_state.rmp file (only used in direct mode).
    /// Default: <data_dir>/hl/hyperliquid_data/abci_state.rmp
    #[arg(long)]
    abci_state_path: Option<PathBuf>,

    /// Path where snapshot.json will be written (only used in direct mode).
    /// Default: /tmp/hl_snapshot.json
    #[arg(long)]
    snapshot_output_path: Option<PathBuf>,

    /// Path to visor_abci_state.json (optional, for height info).
    /// Default: <data_dir>/.hyperliquid_rpc_hlnode_mainnet/volumes/hl/hyperliquid_data/visor_abci_state.json
    #[arg(long)]
    visor_state_path: Option<PathBuf>,

    /// Port for Prometheus metrics endpoint (0 to disable)
    #[arg(long, default_value = "9090")]
    metrics_port: u16,

    /// BBO-only mode: lightweight mode that only tracks best bid/ask per coin.
    /// Reduces RAM from 2-3GB to ~100MB. Disables L2/L4/Trades subscriptions.
    #[arg(long, default_value = "false")]
    bbo_only: bool,

    /// Log level: error, warn, info, debug, trace
    #[arg(long, default_value = "info")]
    log_level: String,
}

/// Start the Prometheus metrics HTTP server
async fn start_metrics_server(port: u16) {
    use axum::{Router, response::IntoResponse, routing::get};

    async fn metrics_handler() -> impl IntoResponse {
        server::metrics::gather_metrics()
    }

    let app = Router::new().route("/metrics", get(metrics_handler));
    let addr = format!("0.0.0.0:{}", port);

    log::info!("Metrics server listening on http://{}/metrics", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.expect("failed to bind metrics port");
    axum::serve(listener, app).await.expect("metrics server failed");
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logger with specified level
    // SAFETY: We're setting this before any threads are spawned
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("RUST_LOG", &args.log_level);
    }
    env_logger::init();

    // Register Prometheus metrics
    server::metrics::register_metrics();

    let full_address = format!("{}:{}", args.address, args.port);

    // Determine market flags from Markets enum
    let (include_perps, include_spot, include_hip3) = match args.markets {
        Markets::Perps => (true, false, false),
        Markets::Spot => (false, true, false),
        Markets::Hip3 => (false, false, true),
        Markets::All => (true, true, true),
    };

    // Build config
    let config = ServerConfig {
        address: full_address.clone(),
        compression_level: args.compression_level,
        data_dir: args.data_dir,
        include_perps,
        include_spot,
        include_hip3,
        snapshot_mode: args.snapshot_mode,
        docker_container: args.docker_container,
        hlnode_binary: args.hlnode_binary,
        abci_state_path: args.abci_state_path,
        snapshot_output_path: args.snapshot_output_path,
        visor_state_path: args.visor_state_path,
        metrics_port: args.metrics_port,
        bbo_only: args.bbo_only,
    };

    println!("Orderbook Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  Address: {}", config.address);
    println!("  Markets: {:?}", args.markets);
    if config.bbo_only {
        println!("  Mode: BBO-ONLY (lightweight, ~100MB RAM)");
        println!("  Note: L2/L4/Trades subscriptions disabled");
    }
    println!("  Snapshot mode: {:?}", config.snapshot_mode);
    match config.snapshot_mode {
        SnapshotMode::Docker => {
            println!("  Container: {}", config.docker_container);
        }
        SnapshotMode::Direct => {
            println!("  hl-node binary: {}", config.hlnode_binary);
            if let Some(ref path) = config.abci_state_path {
                println!("  abci_state: {}", path.display());
            }
            if let Some(ref path) = config.snapshot_output_path {
                println!("  snapshot output: {}", path.display());
            }
        }
    }
    if let Some(ref dir) = config.data_dir {
        println!("  Data dir: {}", dir.display());
    }
    if config.metrics_port > 0 {
        println!("  Metrics: http://0.0.0.0:{}/metrics", config.metrics_port);
    }
    println!("  Log level: {}", args.log_level);
    println!();

    // Spawn uptime counter
    tokio::spawn(async {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            server::metrics::UPTIME_SECONDS.inc();
        }
    });

    // Start metrics server if port > 0
    if config.metrics_port > 0 {
        let metrics_port = config.metrics_port;
        tokio::spawn(async move {
            start_metrics_server(metrics_port).await;
        });
    }

    tokio::select! {
        result = run_websocket_server(config) => {
            if let Err(e) = result {
                log::error!("Server error: {e}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            log::info!("Shutdown signal received, exiting gracefully");
        }
    }

    Ok(())
}
