use std::path::{Path, PathBuf};

use alloy::primitives::Address;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::{
    order_book::{Coin, Oid},
    types::{Fill, L4Order, OrderDiff},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderDiff {
    user: Address,
    oid: u64,
    px: String,
    coin: String,
    pub(crate) raw_book_diff: OrderDiff,
}

impl NodeDataOrderDiff {
    pub(crate) fn diff(&self) -> OrderDiff {
        self.raw_book_diff.clone()
    }
    pub(crate) const fn oid(&self) -> Oid {
        Oid::new(self.oid)
    }

    pub(crate) fn coin(&self) -> Coin {
        Coin::new(&self.coin)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataFill(pub Address, pub Fill);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderStatus {
    pub time: NaiveDateTime,
    pub user: Address,
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(default)]
    pub builder: Option<serde_json::Value>,
    pub status: String,
    pub order: L4Order,
}

impl NodeDataOrderStatus {
    pub(crate) fn is_inserted_into_book(&self) -> bool {
        (self.status == "open" && !self.order.is_trigger && (self.order.tif != Some("Ioc".to_string())))
            || (self.order.is_trigger && self.status == "triggered")
    }
}

#[derive(Clone, Copy, strum_macros::Display)]
pub(crate) enum EventSource {
    Fills,
    OrderStatuses,
    OrderDiffs,
}

impl EventSource {
    /// Get streaming directory (for --stream-with-block-info mode)
    #[must_use]
    pub(crate) fn event_source_dir_streaming(self, dir: &Path) -> PathBuf {
        // Uses *_streaming directories (HFT mode with --stream-with-block-info)
        match self {
            Self::Fills => dir.join("node_fills_streaming"),
            Self::OrderStatuses => dir.join("node_order_statuses_streaming"),
            Self::OrderDiffs => dir.join("node_raw_book_diffs_streaming"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Batch<E> {
    local_time: NaiveDateTime,
    block_time: NaiveDateTime,
    block_number: u64,
    events: Vec<E>,
}

impl<E> Batch<E> {
    #[allow(clippy::unwrap_used)]
    pub(crate) fn block_time(&self) -> u64 {
        self.block_time.and_utc().timestamp_millis().try_into().unwrap()
    }

    pub(crate) const fn block_number(&self) -> u64 {
        self.block_number
    }

    pub(crate) fn events(self) -> Vec<E> {
        self.events
    }
}
