use crate::{
    listeners::order_book::L2Snapshots,
    order_book::{
        Coin, InnerOrder, Oid, Snapshot,
        multi_book::{OrderBooks, Snapshots},
    },
    prelude::*,
    types::{
        inner::{InnerL4Order, InnerOrderDiff},
        node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use std::collections::{HashMap, HashSet};

pub(super) struct OrderBookState {
    order_book: OrderBooks<InnerL4Order>,
    height: u64,
    time: u64,
    ignore_spot: bool,
    // Persistent cache of OrderStatuses waiting for their New diffs
    // Allows OrderStatus and OrderDiff to arrive in any order (HFT-compatible)
    pending_order_statuses: HashMap<Oid, NodeDataOrderStatus>,
    // Persistent cache of New diffs (block-time + sz) waiting for their OrderStatuses.
    // This is the other half of bidirectional caching - handles when Diff arrives
    // BEFORE Status. The block-time stamps the half for age-based eviction.
    pending_new_diffs: HashMap<Oid, (u64, crate::order_book::types::Sz)>,
}

impl OrderBookState {
    pub(super) fn from_snapshot(
        snapshot: Snapshots<InnerL4Order>,
        height: u64,
        time: u64,
        ignore_triggers: bool,
        ignore_spot: bool,
    ) -> Self {
        Self {
            ignore_spot,
            time,
            height,
            order_book: OrderBooks::from_snapshots(snapshot, ignore_triggers),
            pending_order_statuses: HashMap::new(),
            pending_new_diffs: HashMap::new(),
        }
    }

    pub(super) const fn height(&self) -> u64 {
        self.height
    }

    pub(super) const fn ignore_spot(&self) -> bool {
        self.ignore_spot
    }

    pub(super) const fn time(&self) -> u64 {
        self.time
    }

    /// L4 snapshot of a single coin - (time, height, snapshot). Returns None when
    /// the coin has no book. Cheap enough to run under the listener lock, unlike
    /// the old all-coins snapshot.
    pub(super) fn compute_snapshot_for_coin(&self, coin: &Coin) -> Option<(u64, u64, Snapshot<InnerL4Order>)> {
        self.order_book.snapshot_for_coin(coin).map(|snapshot| (self.time, self.height, snapshot))
    }

    /// Incremental variant: rebuilds variants only for `changed_coins` and reuses
    /// cached Arc'd entries for every other coin. The caller owns the cache so
    /// the borrow on `&self` here only touches the order book. Returns
    /// (time, snapshots, recomputed coins, whether the coin set changed).
    pub(super) fn l2_snapshots_incremental(
        &self,
        changed_coins: &HashSet<Coin>,
        active: &HashSet<crate::listeners::order_book::L2SnapshotParams>,
        cache: &mut HashMap<Coin, std::sync::Arc<HashMap<crate::listeners::order_book::L2SnapshotParams, Snapshot<crate::types::inner::InnerLevel>>>>,
    ) -> (u64, L2Snapshots, HashSet<Coin>, bool) {
        let (snapshots, recomputed, coin_set_changed) =
            crate::listeners::order_book::utils::compute_l2_snapshots_incremental(
                &self.order_book,
                changed_coins,
                active,
                cache,
            );
        (self.time, snapshots, recomputed, coin_set_changed)
    }

    pub(super) fn compute_universe(&self) -> HashSet<Coin> {
        self.order_book.as_ref().keys().cloned().collect()
    }

    /// Count of OrderStatuses waiting for their OrderDiff::New to arrive
    pub(super) fn pending_order_statuses_count(&self) -> usize {
        self.pending_order_statuses.len()
    }

    /// Count of OrderDiff::New sizes waiting for their OrderStatus to arrive  
    pub(super) fn pending_new_diffs_count(&self) -> usize {
        self.pending_new_diffs.len()
    }

    /// Total number of orders currently in the orderbook
    pub(super) fn order_count(&self) -> usize {
        self.order_book.order_count()
    }

    /// Number of coins tracked in the orderbook
    pub(super) fn coin_count(&self) -> usize {
        self.order_book.as_ref().len()
    }

    /// Evict pending order halves that can no longer pair, and report whether a
    /// genuine stream fault occurred.
    ///
    /// A resting order is reconstructed from two records carried on separate
    /// streams that share an `oid`: its OrderStatus and its `New` BookDiff.
    /// Whichever arrives first waits in a pending cache for the other. The node
    /// produces both for the *same block*, so in normal operation they pair
    /// within milliseconds. A half left unpaired for *seconds* therefore never
    /// had a partner coming: the order opened and never rested (immediate fill /
    /// IOC), or the counterpart record was filtered out (e.g. a non-perp diff
    /// under `--markets perps`). Dropping such a half leaves the book correct —
    /// it never represented a resting order.
    ///
    /// So we evict by age, with a window far larger than any real inter-stream
    /// skew (~ms). This keeps genuinely in-flight halves (whose partner is still
    /// a block or two behind) while bounding the caches, and — crucially — it is
    /// routine housekeeping, NOT data loss: it must not trigger a re-sync.
    /// (The previous behaviour cleared the whole cache at a size cap and forced a
    /// re-sync, so a steady trickle of never-pairing orphans made the node
    /// rebuild the book from a full snapshot every few seconds.)
    ///
    /// Returns `true` only if, *after* age-eviction, a cache is still above a
    /// large hard cap — a real fault (a stalled or flooding partner stream)
    /// where a re-sync is genuinely warranted.
    pub(super) fn cleanup_stale_pending(&mut self) -> bool {
        // ~100 blocks of margin over the real status<->diff arrival skew.
        const MAX_PENDING_AGE_MS: u64 = 10_000;
        // Age-eviction keeps the caches small in normal operation; exceeding this
        // means the stream itself is broken, not that orphans are accumulating.
        const HARD_CAP: usize = 200_000;

        let now = self.time;
        let stale = |ts_ms: u64| now.saturating_sub(ts_ms) > MAX_PENDING_AGE_MS;

        let before = self.pending_order_statuses.len() + self.pending_new_diffs.len();
        self.pending_order_statuses
            .retain(|_, s| !stale(s.time.and_utc().timestamp_millis().max(0) as u64));
        self.pending_new_diffs.retain(|_, (ts, _)| !stale(*ts));
        let evicted = before - (self.pending_order_statuses.len() + self.pending_new_diffs.len());
        if evicted > 0 {
            log::debug!("Evicted {evicted} unpairable pending order half(ves) by age");
        }

        let compacted = self.order_book.compact_all();
        if compacted > 0 {
            let (live, cap) = self.order_book.slab_stats();
            log::info!("Compacted {compacted} price-level slabs (live={live}, capacity={cap})");
        }

        let overflow =
            self.pending_order_statuses.len() > HARD_CAP || self.pending_new_diffs.len() > HARD_CAP;
        if overflow {
            log::warn!(
                "Pending caches over hard cap after age-eviction (statuses={}, diffs={}) - stream fault, re-syncing",
                self.pending_order_statuses.len(),
                self.pending_new_diffs.len()
            );
        }
        overflow
    }

    /// Get BBO for specific coins only - even faster for selective broadcast
    /// Only computes BBO for coins that changed, avoiding iteration over all 150+ coins
    pub(super) fn get_bbos_for_coins(
        &self,
        coins: &HashSet<Coin>,
    ) -> (
        u64,
        HashMap<
            Coin,
            (
                Option<(crate::order_book::Px, crate::order_book::Sz, u32)>,
                Option<(crate::order_book::Px, crate::order_book::Sz, u32)>,
            ),
        >,
    ) {
        let bbos = self.order_book.get_bbos_for_coins(coins);
        (self.time, bbos)
    }

    /// HFT-specific: Process OrderStatuses independently without block synchronization
    /// Uses bidirectional caching - if diff already arrived, add order immediately
    /// Returns the set of coins that were modified (for selective BBO broadcast)
    pub(super) fn apply_order_statuses_hft(&mut self, batch: Batch<NodeDataOrderStatus>) -> Result<HashSet<Coin>> {
        let height = batch.block_number();
        let time = batch.block_time();
        let mut changed_coins = HashSet::new();

        // Update height/time to track progress (>= ensures time updates even at same height)
        if height >= self.height {
            self.height = height;
            self.time = time;
        }

        for order_status in batch.events() {
            let oid = Oid::new(order_status.order.oid);

            // Check if there's a pending New diff for this order
            if let Some((_, sz)) = self.pending_new_diffs.remove(&oid) {
                // Both arrived - add order immediately!
                let time = order_status.time.and_utc().timestamp_millis();
                let order_coin = Coin::new(&order_status.order.coin);
                let mut inner_order: InnerL4Order = order_status.try_into()?;
                inner_order.modify_sz(sz);
                inner_order.convert_trigger(time.max(0) as u64);
                self.order_book.add_order(inner_order);
                changed_coins.insert(order_coin.clone());
                log::debug!("Order added (status arrived after diff): oid={:?} coin={:?}", oid, order_coin);
            } else if order_status.is_inserted_into_book() {
                // Diff hasn't arrived yet - cache the OrderStatus
                self.pending_order_statuses.insert(oid, order_status);
            }
        }
        Ok(changed_coins)
    }

    #[cfg(test)]
    pub(crate) fn pending_order_statuses_has(&self, oid: &Oid) -> bool {
        self.pending_order_statuses.contains_key(oid)
    }

    #[cfg(test)]
    pub(crate) fn pending_new_diffs_has(&self, oid: &Oid) -> bool {
        self.pending_new_diffs.contains_key(oid)
    }

    /// HFT-specific: Process OrderDiffs independently without block synchronization
    /// Uses bidirectional caching - if status already arrived, add order immediately
    /// Returns the set of coins that were modified (for selective BBO broadcast)
    pub(super) fn apply_order_diffs_hft(&mut self, batch: Batch<NodeDataOrderDiff>) -> Result<HashSet<Coin>> {
        let height = batch.block_number();
        let time = batch.block_time();
        let mut changed_coins = HashSet::new();

        // Update height/time to track progress (>= ensures time updates even at same height)
        if height >= self.height {
            self.height = height;
            self.time = time;
        }

        for diff in batch.events() {
            let oid = diff.oid();
            let coin = diff.coin();
            if coin.is_spot() && self.ignore_spot {
                continue;
            }
            let inner_diff = diff.diff().try_into()?;
            match inner_diff {
                InnerOrderDiff::New { sz } => {
                    // Check if OrderStatus already arrived
                    if let Some(order) = self.pending_order_statuses.remove(&oid) {
                        // Both arrived - add order immediately!
                        let time = order.time.and_utc().timestamp_millis();
                        let order_coin = Coin::new(&order.order.coin);
                        let mut inner_order: InnerL4Order = order.try_into()?;
                        inner_order.modify_sz(sz);
                        #[allow(clippy::unwrap_used)]
                        inner_order.convert_trigger(time.try_into().unwrap());
                        self.order_book.add_order(inner_order);
                        changed_coins.insert(order_coin.clone());
                        log::debug!("Order added (diff arrived after status): oid={:?} coin={:?}", oid, order_coin);
                    } else {
                        // Status hasn't arrived yet - cache the diff size, stamped
                        // with this batch's block-time for age-based eviction.
                        self.pending_new_diffs.insert(oid.clone(), (time, sz));
                    }
                }
                InnerOrderDiff::Update { new_sz, .. } => {
                    let _ = self.order_book.modify_sz(oid, coin.clone(), new_sz);
                    changed_coins.insert(coin);
                }
                InnerOrderDiff::Remove => {
                    let _ = self.order_book.cancel_order(oid.clone(), coin.clone());
                    changed_coins.insert(coin);
                }
            }
        }
        Ok(changed_coins)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::multi_book::Snapshots;
    use crate::types::inner::InnerL4Order;
    use crate::types::{L4Order, OrderDiff};
    use alloy::primitives::Address;
    use chrono::NaiveDateTime;

    fn empty_state() -> OrderBookState {
        let snapshots = Snapshots::new(HashMap::new());
        OrderBookState::from_snapshot(snapshots, 0, 0, true, false)
    }

    fn make_l4_order(coin: &str, oid: u64) -> L4Order {
        L4Order {
            user: None,
            coin: coin.to_string(),
            side: crate::order_book::types::Side::Bid,
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

    fn make_order_status(coin: &str, oid: u64, status: &str) -> NodeDataOrderStatus {
        NodeDataOrderStatus {
            time: NaiveDateTime::parse_from_str("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            user: Address::new([0; 20]),
            hash: Some("0xabc".to_string()),
            builder: None,
            status: status.to_string(),
            order: make_l4_order(coin, oid),
        }
    }

    fn make_order_diff(coin: &str, oid: u64, diff: OrderDiff) -> NodeDataOrderDiff {
        serde_json::from_value(serde_json::json!({
            "user": "0x0000000000000000000000000000000000000000",
            "oid": oid,
            "px": "100.0",
            "coin": coin,
            "raw_book_diff": diff
        })).unwrap()
    }

    fn make_status_batch(statuses: Vec<NodeDataOrderStatus>) -> Batch<NodeDataOrderStatus> {
        serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 100,
            "events": statuses
        })).unwrap()
    }

    fn make_diff_batch(diffs: Vec<NodeDataOrderDiff>) -> Batch<NodeDataOrderDiff> {
        serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 100,
            "events": diffs
        })).unwrap()
    }

    // ==================== Initialization Tests ====================

    #[test]
    fn test_from_snapshot_empty() {
        let state = empty_state();
        assert_eq!(state.height(), 0);
        assert_eq!(state.time(), 0);
        assert_eq!(state.order_count(), 0);
        assert_eq!(state.coin_count(), 0);
        assert_eq!(state.pending_order_statuses_count(), 0);
        assert_eq!(state.pending_new_diffs_count(), 0);
    }

    // ==================== Bidirectional Cache: Status First ====================

    #[test]
    fn test_status_first_then_diff_adds_order() {
        let mut state = empty_state();

        // 1. OrderStatus arrives first → cached
        let status = make_order_status("BTC", 42, "open");
        let batch = make_status_batch(vec![status]);
        let changed = state.apply_order_statuses_hft(batch).unwrap();
        assert!(changed.is_empty()); // not added yet
        assert_eq!(state.pending_order_statuses_count(), 1);
        assert!(state.pending_order_statuses_has(&Oid::new(42)));

        // 2. OrderDiff::New arrives → order added immediately
        let diff = make_order_diff("BTC", 42, OrderDiff::New { sz: "1.5".to_string() });
        let batch = make_diff_batch(vec![diff]);
        let changed = state.apply_order_diffs_hft(batch).unwrap();
        assert!(changed.contains(&Coin::new("BTC")));
        assert_eq!(state.pending_order_statuses_count(), 0); // consumed
        assert_eq!(state.order_count(), 1);
    }

    // ==================== Bidirectional Cache: Diff First ====================

    #[test]
    fn test_diff_first_then_status_adds_order() {
        let mut state = empty_state();

        // 1. OrderDiff::New arrives first → size cached
        let diff = make_order_diff("ETH", 99, OrderDiff::New { sz: "2.0".to_string() });
        let batch = make_diff_batch(vec![diff]);
        let changed = state.apply_order_diffs_hft(batch).unwrap();
        assert!(changed.is_empty()); // not added yet
        assert_eq!(state.pending_new_diffs_count(), 1);
        assert!(state.pending_new_diffs_has(&Oid::new(99)));

        // 2. OrderStatus arrives → order added immediately
        let status = make_order_status("ETH", 99, "open");
        let batch = make_status_batch(vec![status]);
        let changed = state.apply_order_statuses_hft(batch).unwrap();
        assert!(changed.contains(&Coin::new("ETH")));
        assert_eq!(state.pending_new_diffs_count(), 0); // consumed
        assert_eq!(state.order_count(), 1);
    }

    // ==================== OrderDiff Update/Remove ====================

    #[test]
    fn test_diff_update_changes_coin() {
        let mut state = empty_state();
        // First add an order via the bidirectional path
        let status = make_order_status("BTC", 1, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string() });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert_eq!(state.order_count(), 1);

        // Now send Update
        let update = make_order_diff("BTC", 1, OrderDiff::Update { orig_sz: "5.0".to_string(), new_sz: "3.0".to_string() });
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![update])).unwrap();
        assert!(changed.contains(&Coin::new("BTC")));
    }

    #[test]
    fn test_diff_remove_changes_coin() {
        let mut state = empty_state();
        // Add order
        let status = make_order_status("BTC", 1, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string() });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();

        // Remove
        let remove = make_order_diff("BTC", 1, OrderDiff::Remove);
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![remove])).unwrap();
        assert!(changed.contains(&Coin::new("BTC")));
        assert_eq!(state.order_count(), 0);
    }

    // ==================== Status Filtering ====================

    #[test]
    fn test_non_insertable_status_not_cached() {
        let mut state = empty_state();
        // "filled" status should NOT be cached
        let status = make_order_status("BTC", 42, "filled");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    #[test]
    fn test_ioc_not_cached() {
        let mut state = empty_state();
        let mut status = make_order_status("BTC", 42, "open");
        status.order.tif = Some("Ioc".to_string());
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    // ==================== Spot Filtering ====================

    #[test]
    fn test_spot_filtered_when_ignore_spot() {
        let snapshots = Snapshots::new(HashMap::new());
        let mut state = OrderBookState::from_snapshot(snapshots, 0, 0, true, true); // ignore_spot=true

        let diff = make_order_diff("@1", 1, OrderDiff::New { sz: "1.0".to_string() });
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert!(changed.is_empty());
        assert_eq!(state.pending_new_diffs_count(), 0); // skipped entirely
    }

    #[test]
    fn test_spot_not_filtered_when_not_ignoring() {
        let mut state = empty_state(); // ignore_spot=false
        let diff = make_order_diff("@1", 1, OrderDiff::New { sz: "1.0".to_string() });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert_eq!(state.pending_new_diffs_count(), 1); // cached
    }

    // ==================== Height/Time Tracking ====================

    #[test]
    fn test_height_updates_on_higher_block() {
        let mut state = empty_state();
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 500,
            "events": []
        })).unwrap();
        state.apply_order_diffs_hft(batch).unwrap();
        assert_eq!(state.height(), 500);
    }

    #[test]
    fn test_height_not_downgraded() {
        let mut state = empty_state();
        // Set height to 500
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:31:00.000000000",
            "block_time": "2024-01-15T10:31:00.000000000",
            "block_number": 500,
            "events": []
        })).unwrap();
        state.apply_order_diffs_hft(batch).unwrap();

        // Try to go to 200
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 200,
            "events": []
        })).unwrap();
        state.apply_order_diffs_hft(batch).unwrap();
        assert_eq!(state.height(), 500); // unchanged
    }

    // ==================== Cleanup Tests ====================

    fn ts_ms(s: &str) -> u64 {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap().and_utc().timestamp_millis() as u64
    }

    fn make_order_status_at(coin: &str, oid: u64, status: &str, ts: &str) -> NodeDataOrderStatus {
        let mut s = make_order_status(coin, oid, status);
        s.time = NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S").unwrap();
        s
    }

    // An order half left unpaired only briefly is genuinely in flight (its
    // partner is a block or two behind): it must be KEPT and must still pair.
    #[test]
    fn test_young_in_flight_half_kept_and_still_pairs() {
        let mut state = empty_state();
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_order_status_at("BTC", 1, "open", "2024-01-15 10:30:00")]))
            .unwrap();
        // Only 1s of stream has passed - the diff is delayed, not gone.
        state.time = ts_ms("2024-01-15 10:30:01");
        assert!(!state.cleanup_stale_pending(), "no fault");
        assert_eq!(state.pending_order_statuses_count(), 1, "young half must be kept");
        // The delayed diff arrives -> the order rests correctly.
        state
            .apply_order_diffs_hft(make_diff_batch(vec![make_order_diff("BTC", 1, OrderDiff::New { sz: "1.0".to_string() })]))
            .unwrap();
        assert_eq!(state.pending_order_statuses_count(), 0);
        assert_eq!(state.order_count(), 1, "delayed order is not lost");
    }

    // A half unpaired for many seconds never had a partner (immediate fill /
    // filtered counterpart). Evicting it is housekeeping, NOT data loss: it must
    // be dropped WITHOUT triggering a re-sync.
    #[test]
    fn test_aged_orphan_status_evicted_without_resync() {
        let mut state = empty_state();
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_order_status_at("BTC", 1, "open", "2024-01-15 10:30:00")]))
            .unwrap();
        assert_eq!(state.pending_order_statuses_count(), 1);
        state.time = ts_ms("2024-01-15 10:30:20"); // 20s later
        assert!(!state.cleanup_stale_pending(), "aged-orphan eviction is not a re-sync trigger");
        assert_eq!(state.pending_order_statuses_count(), 0, "aged orphan evicted");
    }

    #[test]
    fn test_aged_orphan_diff_evicted_without_resync() {
        let mut state = empty_state();
        state
            .apply_order_diffs_hft(make_diff_batch(vec![make_order_diff("BTC", 9, OrderDiff::New { sz: "1.0".to_string() })]))
            .unwrap();
        assert_eq!(state.pending_new_diffs_count(), 1);
        state.time = ts_ms("2024-01-15 10:30:20");
        assert!(!state.cleanup_stale_pending());
        assert_eq!(state.pending_new_diffs_count(), 0);
    }

    // Evicting an aged orphan must not disturb genuinely resting orders.
    #[test]
    fn test_orphan_eviction_leaves_resting_orders_intact() {
        let mut state = empty_state();
        // A real resting order (status + diff pair).
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_order_status_at("BTC", 1, "open", "2024-01-15 10:30:00")]))
            .unwrap();
        state
            .apply_order_diffs_hft(make_diff_batch(vec![make_order_diff("BTC", 1, OrderDiff::New { sz: "1.0".to_string() })]))
            .unwrap();
        assert_eq!(state.order_count(), 1);
        // An orphan status that never rests.
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_order_status_at("BTC", 2, "open", "2024-01-15 10:30:00")]))
            .unwrap();
        state.time = ts_ms("2024-01-15 10:30:20");
        assert!(!state.cleanup_stale_pending());
        assert_eq!(state.pending_order_statuses_count(), 0, "orphan gone");
        assert_eq!(state.order_count(), 1, "resting order untouched");
    }

    // The hard cap is a genuine-fault backstop: a cache still huge AFTER
    // age-eviction (a stalled/flooding partner stream) does warrant a re-sync.
    #[test]
    fn test_hard_cap_overflow_triggers_resync() {
        use crate::order_book::types::Sz;
        let mut state = empty_state();
        state.time = ts_ms("2024-01-15 10:30:00");
        // > HARD_CAP young entries (same block-time as now): age-eviction cannot
        // drop them, so this is a real fault.
        for i in 0..200_001u64 {
            state.pending_new_diffs.insert(Oid::new(i), (state.time, Sz::new(1)));
        }
        assert!(state.cleanup_stale_pending(), "over hard cap after age-eviction => re-sync");
    }

    // ==================== Per-coin L4 snapshot ====================

    #[test]
    fn test_compute_snapshot_for_coin_returns_only_that_coin() {
        let mut state = empty_state();
        for (i, coin) in ["BTC", "ETH"].iter().enumerate() {
            let status = make_order_status(coin, i as u64, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
            let diff = make_order_diff(coin, i as u64, OrderDiff::New { sz: "1.0".to_string() });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }

        let (_time, height, snapshot) = state.compute_snapshot_for_coin(&Coin::new("BTC")).unwrap();
        assert_eq!(height, 100); // batch helpers stamp block_number 100
        let [bids, asks] = snapshot.as_ref();
        assert_eq!(bids.len(), 1, "only BTC's single bid is included");
        assert!(asks.is_empty());

        assert!(state.compute_snapshot_for_coin(&Coin::new("DOGE")).is_none(), "unknown coin yields None");
    }

    // ==================== Performance Tests ====================

    #[test]
    fn test_apply_diffs_performance() {
        let mut state = empty_state();
        // Pre-populate with order statuses
        for i in 0..1000u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        }

        // Time matching diffs arrival
        let start = std::time::Instant::now();
        for i in 0..1000u64 {
            let diff = make_order_diff("BTC", i, OrderDiff::New { sz: "1.0".to_string() });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        let elapsed = start.elapsed();
        let per_event = elapsed / 1000;

        eprintln!(
            "[PERF] apply_order_diffs_hft: 1000 New diffs (with cached statuses): {:?} ({:?}/event)",
            elapsed, per_event
        );
        assert_eq!(state.order_count(), 1000);
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    #[test]
    fn test_apply_statuses_performance() {
        let mut state = empty_state();
        // Pre-populate with diffs
        for i in 0..1000u64 {
            let diff = make_order_diff("BTC", i, OrderDiff::New { sz: "1.0".to_string() });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }

        let start = std::time::Instant::now();
        for i in 0..1000u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        }
        let elapsed = start.elapsed();
        let per_event = elapsed / 1000;

        eprintln!(
            "[PERF] apply_order_statuses_hft: 1000 statuses (with cached diffs): {:?} ({:?}/event)",
            elapsed, per_event
        );
        assert_eq!(state.order_count(), 1000);
        assert_eq!(state.pending_new_diffs_count(), 0);
    }

    #[test]
    fn test_universe_computation() {
        let mut state = empty_state();
        // Add orders for multiple coins
        for (i, coin) in ["BTC", "ETH", "SOL"].iter().enumerate() {
            let status = make_order_status(coin, i as u64, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
            let diff = make_order_diff(coin, i as u64, OrderDiff::New { sz: "1.0".to_string() });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        let universe = state.compute_universe();
        assert_eq!(universe.len(), 3);
        assert!(universe.contains(&Coin::new("BTC")));
        assert!(universe.contains(&Coin::new("ETH")));
        assert!(universe.contains(&Coin::new("SOL")));
    }
}
