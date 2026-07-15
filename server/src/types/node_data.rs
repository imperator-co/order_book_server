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
    pub(crate) const fn diff(&self) -> &OrderDiff {
        &self.raw_book_diff
    }
    pub(crate) const fn oid(&self) -> Oid {
        Oid::new(self.oid)
    }

    pub(crate) fn coin(&self) -> Coin {
        Coin::new(&self.coin)
    }

    pub(crate) fn px(&self) -> &str {
        &self.px
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

#[derive(Debug, Clone, Copy, strum_macros::Display)]
pub(crate) enum EventSource {
    Fills,
    OrderStatuses,
    OrderDiffs,
}

impl EventSource {
    /// Stable label used for Prometheus metric dimensions.
    #[must_use]
    pub(crate) const fn metric_label(self) -> &'static str {
        match self {
            Self::Fills => "fills",
            Self::OrderStatuses => "orders",
            Self::OrderDiffs => "diffs",
        }
    }

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
    /// Block time in unix milliseconds. Saturates at 0 for pre-1970 timestamps
    /// rather than panicking via `try_into::<u64>` — a single corrupt node
    /// record would otherwise crash the whole listener task.
    pub(crate) fn block_time(&self) -> u64 {
        self.block_time.and_utc().timestamp_millis().max(0) as u64
    }

    pub(crate) const fn block_number(&self) -> u64 {
        self.block_number
    }

    pub(crate) fn events(self) -> Vec<E> {
        self.events
    }

    /// Borrowed view of the events, for grouping without consuming the batch.
    pub(crate) fn events_ref(&self) -> &[E] {
        &self.events
    }

    /// Number of events without consuming the batch.
    pub(crate) fn events_len(&self) -> usize {
        self.events.len()
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::types::Side;

    fn make_l4_order(coin: &str, oid: u64) -> L4Order {
        L4Order {
            user: None,
            coin: coin.to_string(),
            side: Side::Bid,
            limit_px: "100.0".to_string(),
            sz: "1.0".to_string(),
            oid,
            timestamp: 1000,
            trigger_condition: "N/A".to_string(),
            is_trigger: false,
            trigger_px: "0.0".to_string(),
            children: Vec::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: "Limit".to_string(),
            orig_sz: "1.0".to_string(),
            tif: Some("Gtc".to_string()),
            cloid: None,
        }
    }

    fn make_order_status(status: &str, is_trigger: bool, tif: Option<&str>, coin: &str, oid: u64) -> NodeDataOrderStatus {
        let mut order = make_l4_order(coin, oid);
        order.is_trigger = is_trigger;
        order.tif = tif.map(String::from);
        NodeDataOrderStatus {
            time: chrono::NaiveDateTime::parse_from_str("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            user: Address::new([0; 20]),
            hash: Some("0xabc".to_string()),
            builder: None,
            status: status.to_string(),
            order,
        }
    }

    // ==================== is_inserted_into_book Tests ====================

    #[test]
    fn test_open_non_trigger_gtc_is_inserted() {
        let status = make_order_status("open", false, Some("Gtc"), "BTC", 1);
        assert!(status.is_inserted_into_book());
    }

    #[test]
    fn test_open_non_trigger_alo_is_inserted() {
        let status = make_order_status("open", false, Some("Alo"), "BTC", 1);
        assert!(status.is_inserted_into_book());
    }

    #[test]
    fn test_open_non_trigger_no_tif_is_inserted() {
        let status = make_order_status("open", false, None, "BTC", 1);
        assert!(status.is_inserted_into_book());
    }

    #[test]
    fn test_open_ioc_not_inserted() {
        let status = make_order_status("open", false, Some("Ioc"), "BTC", 1);
        assert!(!status.is_inserted_into_book());
    }

    #[test]
    fn test_open_trigger_not_inserted() {
        // is_trigger + "open" → not inserted (triggers only insert when "triggered")
        let status = make_order_status("open", true, Some("Gtc"), "BTC", 1);
        assert!(!status.is_inserted_into_book());
    }

    #[test]
    fn test_triggered_trigger_is_inserted() {
        let status = make_order_status("triggered", true, Some("Gtc"), "BTC", 1);
        assert!(status.is_inserted_into_book());
    }

    #[test]
    fn test_filled_not_inserted() {
        let status = make_order_status("filled", false, Some("Gtc"), "BTC", 1);
        assert!(!status.is_inserted_into_book());
    }

    #[test]
    fn test_canceled_not_inserted() {
        let status = make_order_status("canceled", false, Some("Gtc"), "BTC", 1);
        assert!(!status.is_inserted_into_book());
    }

    #[test]
    fn test_rejected_not_inserted() {
        let status = make_order_status("rejected", false, Some("Gtc"), "BTC", 1);
        assert!(!status.is_inserted_into_book());
    }

    // ==================== EventSource Tests ====================

    #[test]
    fn test_event_source_streaming_dirs() {
        let dir = std::path::Path::new("/data");
        assert_eq!(EventSource::Fills.event_source_dir_streaming(dir), PathBuf::from("/data/node_fills_streaming"));
        assert_eq!(EventSource::OrderStatuses.event_source_dir_streaming(dir), PathBuf::from("/data/node_order_statuses_streaming"));
        assert_eq!(EventSource::OrderDiffs.event_source_dir_streaming(dir), PathBuf::from("/data/node_raw_book_diffs_streaming"));
    }

    // ==================== NodeDataOrderDiff Tests ====================

    #[test]
    fn test_node_data_order_diff_serde_new() {
        let json = r#"{"user":"0x0000000000000000000000000000000000000001","oid":123,"px":"50000.0","coin":"BTC","raw_book_diff":{"new":{"sz":"1.5"}}}"#;
        let diff: NodeDataOrderDiff = serde_json::from_str(json).unwrap();
        assert_eq!(diff.oid(), Oid::new(123));
        assert_eq!(diff.coin(), Coin::new("BTC"));
        assert!(matches!(diff.diff(), OrderDiff::New { sz } if sz == "1.5"));
    }

    #[test]
    fn test_node_data_order_diff_serde_update() {
        let json = r#"{"user":"0x0000000000000000000000000000000000000001","oid":456,"px":"50000.0","coin":"ETH","raw_book_diff":{"update":{"origSz":"2.0","newSz":"1.0"}}}"#;
        let diff: NodeDataOrderDiff = serde_json::from_str(json).unwrap();
        assert_eq!(diff.oid(), Oid::new(456));
        assert!(matches!(diff.diff(), OrderDiff::Update { orig_sz, new_sz } if orig_sz == "2.0" && new_sz == "1.0"));
    }

    #[test]
    fn test_node_data_order_diff_serde_remove() {
        let json = r#"{"user":"0x0000000000000000000000000000000000000001","oid":789,"px":"50000.0","coin":"SOL","raw_book_diff":"remove"}"#;
        let diff: NodeDataOrderDiff = serde_json::from_str(json).unwrap();
        assert_eq!(diff.oid(), Oid::new(789));
        assert!(matches!(diff.diff(), OrderDiff::Remove));
    }

    // ==================== Batch Serde Tests ====================

    #[test]
    fn test_batch_serde() {
        let json = r#"{
            "local_time": "2024-01-15T10:30:45.123456789",
            "block_time": "2024-01-15T10:30:45.000000000",
            "block_number": 12345,
            "events": [
                {"user":"0x0000000000000000000000000000000000000001","oid":1,"px":"100.0","coin":"BTC","raw_book_diff":{"new":{"sz":"1.0"}}}
            ]
        }"#;
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_str(json).unwrap();
        assert_eq!(batch.block_number(), 12345);
        assert_eq!(batch.events().len(), 1);
    }

    #[test]
    fn test_batch_empty_events() {
        let json = r#"{
            "local_time": "2024-01-15T10:30:45.000000000",
            "block_time": "2024-01-15T10:30:45.000000000",
            "block_number": 100,
            "events": []
        }"#;
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_str(json).unwrap();
        assert_eq!(batch.block_number(), 100);
        assert_eq!(batch.events().len(), 0);
    }

    #[test]
    fn test_batch_block_time_pre_1970_saturates_to_zero() {
        // Pre-1970 timestamps would panic the old try_into::<u64>(). Verify they now saturate.
        let json = r#"{
            "local_time": "1969-06-15T00:00:00.000000000",
            "block_time": "1969-06-15T00:00:00.000000000",
            "block_number": 0,
            "events": []
        }"#;
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_str(json).unwrap();
        assert_eq!(batch.block_time(), 0);
    }

    // ==================== NodeDataFill Tests ====================

    #[test]
    fn test_node_data_fill_serde() {
        let json = r#"[
            "0x0000000000000000000000000000000000000001",
            {
                "coin": "BTC",
                "px": "50000.0",
                "sz": "0.1",
                "side": "A",
                "time": 1700000000000,
                "startPosition": "0",
                "dir": "Open Long",
                "closedPnl": "0",
                "hash": "0xabc",
                "oid": 123,
                "crossed": true,
                "fee": "0.5",
                "tid": 999,
                "feeToken": "USDC"
            }
        ]"#;
        let fill: NodeDataFill = serde_json::from_str(json).unwrap();
        assert_eq!(fill.0, Address::new([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]));
        assert_eq!(fill.1.coin, "BTC");
        assert_eq!(fill.1.tid, 999);
    }
}
