use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::{Snapshot, multi_book::OrderBooks, types::InnerOrder},
    prelude::*,
    types::{
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use log::{error, info};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::{collections::HashMap, path::PathBuf};
use tokio::process::Command;

use crate::SnapshotMode;

/// Configuration for snapshot fetching
#[derive(Debug, Clone)]
pub(super) struct SnapshotConfig {
    pub mode: SnapshotMode,
    pub docker_container: String,
    pub hlnode_binary: String,
    pub abci_state_path: Option<PathBuf>,
    pub snapshot_output_path: Option<PathBuf>,
    pub visor_state_path: Option<PathBuf>,
    pub data_dir: PathBuf,
}

pub(super) async fn process_rmp_file(config: &SnapshotConfig) -> Result<PathBuf> {
    info!("Triggering L4 snapshot via hl-node CLI (mode: {:?})...", config.mode);

    let (output_path, _visor_path) = match config.mode {
        SnapshotMode::Docker => {
            // Docker mode: run command inside container
            // data_dir should be the path containing node_*_by_block directories
            // Snapshot goes to parent of data_dir (sibling to "data" folder)
            let parent_dir = config.data_dir.parent().unwrap_or(&config.data_dir);
            let output_path = config.snapshot_output_path.clone().unwrap_or_else(|| parent_dir.join("snapshot.json"));
            let visor_path = config
                .visor_state_path
                .clone()
                .unwrap_or_else(|| parent_dir.join("hyperliquid_data/visor_abci_state.json"));

            let output = Command::new("docker")
                .args(&[
                    "exec",
                    &config.docker_container,
                    "./hl-node",
                    "--chain",
                    "Mainnet",
                    "compute-l4-snapshots",
                    "--include-users",
                    "hl/hyperliquid_data/abci_state.rmp",
                    "hl/snapshot.json",
                ])
                .output()
                .await;

            match output {
                Ok(out) => {
                    if !out.status.success() {
                        error!("hl-node compute-l4-snapshots failed: {}", String::from_utf8_lossy(&out.stderr));
                        return Err("hl-node compute-l4-snapshots failed".into());
                    }
                    info!("L4 snapshot computed successfully (docker mode)");
                }
                Err(e) => {
                    error!("Failed to execute docker command: {}", e);
                    return Err(e.into());
                }
            }

            (output_path, visor_path)
        }
        SnapshotMode::Direct => {
            // Direct mode: run hl-node directly on host
            let abci_path = config
                .abci_state_path
                .clone()
                .unwrap_or_else(|| config.data_dir.join("hl/hyperliquid_data/abci_state.rmp"));
            let output_path =
                config.snapshot_output_path.clone().unwrap_or_else(|| PathBuf::from("/tmp/hl_snapshot.json"));
            let visor_path = config
                .visor_state_path
                .clone()
                .unwrap_or_else(|| config.data_dir.join("hl/hyperliquid_data/visor_abci_state.json"));

            info!(
                "Running: {} --chain Mainnet compute-l4-snapshots --include-users {} {}",
                &config.hlnode_binary,
                abci_path.display(),
                output_path.display()
            );

            let output = Command::new(&config.hlnode_binary)
                .args(&[
                    "--chain",
                    "Mainnet",
                    "compute-l4-snapshots",
                    "--include-users",
                    abci_path.to_str().unwrap_or(""),
                    output_path.to_str().unwrap_or(""),
                ])
                .output()
                .await;

            match output {
                Ok(out) => {
                    if !out.status.success() {
                        error!("hl-node compute-l4-snapshots failed: {}", String::from_utf8_lossy(&out.stderr));
                        error!("stdout: {}", String::from_utf8_lossy(&out.stdout));
                        return Err("hl-node compute-l4-snapshots failed".into());
                    }
                    info!("L4 snapshot computed successfully (direct mode)");
                }
                Err(e) => {
                    error!("Failed to execute hl-node command: {}", e);
                    return Err(e.into());
                }
            }

            (output_path, visor_path)
        }
    };

    // Verify file exists
    if output_path.exists() {
        info!("Snapshot file found at: {:?}", output_path);
        // Return tuple (output_path, visor_path) - but for now just output_path
        // The caller needs visor_path too, so we'll store it
        return Ok(output_path);
    }

    // Debug: List directory contents if file not found
    if let Some(parent) = output_path.parent() {
        error!("File not found. Listing directory {:?}:", parent);
        if let Ok(entries) = fs::read_dir(parent) {
            for entry in entries.flatten() {
                error!(" - {:?}", entry.path());
            }
        } else {
            error!("Failed to read directory {:?}", parent);
        }
    }

    Err("Snapshot file not created".into())
}

/// Get the visor state path based on config
/// Get the visor state path based on config
/// data_dir should be the path containing node_*_by_block directories
/// visor_abci_state.json is in parent/hyperliquid_data/
pub(super) fn get_visor_path(config: &SnapshotConfig) -> PathBuf {
    config.visor_state_path.clone().unwrap_or_else(|| {
        let parent_dir = config.data_dir.parent().unwrap_or(&config.data_dir);
        parent_dir.join("hyperliquid_data/visor_abci_state.json")
    })
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

pub(super) fn compute_l2_snapshots<O: InnerOrder + Send + Sync>(order_books: &OrderBooks<O>) -> L2Snapshots {
    L2Snapshots(
        order_books
            .as_ref()
            .par_iter()
            .map(|(coin, order_book)| {
                let mut entries = Vec::new();
                let snapshot = order_book.to_l2_snapshot(None, None, None);
                entries.push((L2SnapshotParams { n_sig_figs: None, mantissa: None }, snapshot));
                let mut add_new_snapshot = |n_sig_figs: Option<u32>, mantissa: Option<u64>, idx: usize| {
                    if let Some((_, last_snapshot)) = &entries.get(entries.len() - idx) {
                        let snapshot = last_snapshot.to_l2_snapshot(None, n_sig_figs, mantissa);
                        entries.push((L2SnapshotParams { n_sig_figs, mantissa }, snapshot));
                    }
                };
                for n_sig_figs in (2..=5).rev() {
                    if n_sig_figs == 5 {
                        for mantissa in [None, Some(2), Some(5)] {
                            if mantissa == Some(5) {
                                // Some(2) is NOT a superset of this info!
                                add_new_snapshot(Some(n_sig_figs), mantissa, 2);
                            } else {
                                add_new_snapshot(Some(n_sig_figs), mantissa, 1);
                            }
                        }
                    } else {
                        add_new_snapshot(Some(n_sig_figs), None, 1);
                    }
                }
                (coin.clone(), entries.into_iter().collect::<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>())
            })
            .collect(),
    )
}

pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}
