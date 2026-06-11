use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use crate::{
    order_book::types::Side,
    types::node_data::{NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
};

pub(crate) mod inner;
pub(crate) mod node_data;
pub(crate) mod subscription;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Trade {
    pub coin: String,
    side: Side,
    px: String,
    sz: String,
    hash: String,
    time: u64,
    tid: u64,
    users: [Address; 2],
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub(crate) struct Level {
    px: String,
    sz: String,
    n: usize,
}

impl Level {
    pub(crate) const fn new(px: String, sz: String, n: usize) -> Self {
        Self { px, sz, n }
    }

    // Only exercised by tests since the BBO snapshot path moved to numeric dedup.
    #[cfg(test)]
    pub(crate) fn px(&self) -> &str {
        &self.px
    }

    #[cfg(test)]
    pub(crate) fn sz(&self) -> &str {
        &self.sz
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct L2Book {
    coin: String,
    time: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    n_sig_figs: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mantissa: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    n_levels: Option<usize>,
    levels: [Vec<Level>; 2],
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum L4Book {
    Snapshot { coin: String, time: u64, height: u64, levels: [Vec<L4Order>; 2] },
    Updates(L4BookUpdates),
}

/// Best Bid/Offer - top of book only
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Bbo {
    pub coin: String,
    pub time: u64,
    pub bid: Option<Level>,
    pub ask: Option<Level>,
}

impl L2Book {
    pub(crate) const fn from_l2_snapshot(
        coin: String,
        snapshot: [Vec<Level>; 2],
        time: u64,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
        n_levels: Option<usize>,
    ) -> Self {
        Self { coin, time, n_sig_figs, mantissa, n_levels, levels: snapshot }
    }

    pub(crate) const fn set_time(&mut self, time: u64) {
        self.time = time;
    }
}

impl Trade {
    /// Build one trade print from the two fill legs of a match, following the
    /// public websocket schema: `side` is the aggressing (taker) side — the
    /// leg whose `crossed` flag is set — and `users` is `[buyer, seller]`.
    ///
    /// Returns `None` if the legs do not belong to the same match (coin or
    /// trade id mismatch); callers should skip such legs rather than emit
    /// schema-breaking output.
    pub(crate) fn from_fills(bid: NodeDataFill, ask: NodeDataFill) -> Option<Self> {
        let NodeDataFill(buyer, bid_fill) = bid;
        let NodeDataFill(seller, ask_fill) = ask;
        if bid_fill.coin != ask_fill.coin || bid_fill.tid != ask_fill.tid {
            return None;
        }
        // "Side is aggressing side for trades" (public API notation): the
        // taker is the leg that crossed the spread.
        let side = if ask_fill.crossed { Side::Ask } else { Side::Bid };
        Some(Self {
            coin: ask_fill.coin,
            side,
            px: ask_fill.px,
            sz: ask_fill.sz,
            hash: ask_fill.hash,
            time: ask_fill.time,
            tid: ask_fill.tid,
            users: [buyer, seller],
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct L4BookUpdates {
    pub time: u64,
    pub height: u64,
    // Arc'd so the per-coin groupings built once in the listener are shared
    // across every subscribed connection instead of deep-cloned per send.
    // serde's "rc" feature serializes Arc<Vec<T>> exactly like Vec<T>.
    pub order_statuses: std::sync::Arc<Vec<NodeDataOrderStatus>>,
    pub book_diffs: std::sync::Arc<Vec<NodeDataOrderDiff>>,
}

// RawL4Order is the version of a L4Order we want to serialize and deserialize directly
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct L4Order {
    // when serializing, this field is found outside of this struct
    // when deserializing, we move it into this struct
    pub user: Option<Address>,
    pub coin: String,
    pub side: Side,
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    pub trigger_condition: String,
    pub is_trigger: bool,
    pub trigger_px: String,
    #[serde(default)]
    pub children: Vec<serde_json::Value>,
    pub is_position_tpsl: bool,
    pub reduce_only: bool,
    pub order_type: String,
    #[serde(default)]
    pub orig_sz: String,
    pub tif: Option<String>,
    pub cloid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OrderDiff {
    #[serde(rename_all = "camelCase")]
    New {
        sz: String,
    },
    #[serde(rename_all = "camelCase")]
    Update {
        orig_sz: String,
        new_sz: String,
    },
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Fill {
    pub coin: String,
    pub px: String,
    pub sz: String,
    pub side: Side,
    pub time: u64,
    pub start_position: String,
    pub dir: String,
    pub closed_pnl: String,
    pub hash: String,
    pub oid: u64,
    pub crossed: bool,
    pub fee: String,
    pub tid: u64,
    #[serde(default)]
    pub cloid: Option<String>,
    pub fee_token: String,
    #[serde(default)]
    pub twap_id: Option<u64>,
    pub liquidation: Option<Liquidation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Liquidation {
    pub liquidated_user: String,
    pub mark_px: String,
    pub method: String,
}
