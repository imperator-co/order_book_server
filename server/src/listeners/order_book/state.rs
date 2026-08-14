use crate::{
    listeners::order_book::L2Snapshots,
    order_book::{
        Coin, InnerOrder, Oid, PxBand, Snapshot,
        multi_book::{OrderBooks, Snapshots},
    },
    prelude::*,
    types::{
        inner::{InnerL4Order, InnerOrderDiff},
        node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

pub(super) struct OrderBookState {
    order_book: OrderBooks<InnerL4Order>,
    height: u64,
    time: u64,
    ignore_spot: bool,
    // Persistent cache of OrderStatuses waiting for their New diffs
    // Allows OrderStatus and OrderDiff to arrive in any order (HFT-compatible).
    // Entries carry their insertion time so cleanup can evict by age instead of
    // nuking the whole map (which killed in-flight halves and forced re-syncs).
    pending_order_statuses: rustc_hash::FxHashMap<Oid, (NodeDataOrderStatus, Instant)>,
    // Persistent cache of New diffs (sz + optional insertBefore anchor) waiting for their
    // OrderStatuses. This is the other half of bidirectional caching - handles when Diff
    // arrives BEFORE Status. The anchor must survive the cache so a late-pairing priority
    // ALO order still splices into the right queue position.
    pending_new_diffs: rustc_hash::FxHashMap<Oid, (crate::order_book::types::Sz, Option<Oid>, Instant)>,
    // insertBefore anchors that were missing from the book (add fell back to the back of
    // the level). Drained per batch by the listener, which converts a nonzero count into
    // a desync mark + Prometheus counter. Sticky across snapshot replay on purpose.
    insert_before_fallbacks: u64,
    // Untriggered trigger orders (stop / TP-SL waiting for triggerPx to be crossed),
    // keyed per coin so single-coin queries and bogus-coin probes never scan the
    // whole set. These never rest on the book: the hl-node snapshot appends them to
    // both sides (extracted at install), and live "open" statuses with is_trigger
    // carry them. Removed on any non-"open" status for the oid (canceled /
    // triggered / filled / rejected / ...). Orders are Arc'd so the endpoint's
    // under-lock snapshot is a refcount bump per order, not a deep String clone.
    // Replaced wholesale on every re-sync along with the book, so a missed
    // terminal status self-heals at the next snapshot install.
    untriggered_orders: rustc_hash::FxHashMap<Coin, rustc_hash::FxHashMap<Oid, std::sync::Arc<InnerL4Order>>>,
    // False in --bbo-only mode: the map stays empty there so the documented
    // lightweight memory envelope holds (the endpoint has no consumers in a
    // BBO-only deployment anyway).
    track_untriggered: bool,
}

impl OrderBookState {
    pub(super) fn from_snapshot(
        mut snapshot: Snapshots<InnerL4Order>,
        height: u64,
        time: u64,
        ignore_triggers: bool,
        ignore_spot: bool,
        track_untriggered: bool,
    ) -> Self {
        // When triggers are excluded from the book (production), keep them in the
        // side table instead of dropping them. The oid insert dedupes the two
        // per-side copies the node snapshot emits for each trigger order.
        let mut untriggered_orders: rustc_hash::FxHashMap<
            Coin,
            rustc_hash::FxHashMap<Oid, std::sync::Arc<InnerL4Order>>,
        > = rustc_hash::FxHashMap::default();
        if ignore_triggers && track_untriggered {
            for order in snapshot.extract_triggers() {
                if ignore_spot && order.coin.is_spot() {
                    continue;
                }
                let oid = order.oid();
                let arc = std::sync::Arc::new(order);
                if let Some(prev) =
                    untriggered_orders.entry(arc.coin.clone()).or_default().insert(oid.clone(), arc.clone())
                    && *prev != *arc
                {
                    // The node emits one copy per side; they are expected to be
                    // identical. A mismatch means the dedupe silently picked one
                    // of two different views - surface it.
                    log::warn!("Trigger order snapshot copies differ across sides: oid={oid:?}");
                }
            }
        }
        Self {
            ignore_spot,
            time,
            height,
            order_book: OrderBooks::from_snapshots(snapshot, ignore_triggers),
            pending_order_statuses: rustc_hash::FxHashMap::default(),
            pending_new_diffs: rustc_hash::FxHashMap::default(),
            insert_before_fallbacks: 0,
            untriggered_orders,
            track_untriggered,
        }
    }

    /// Drain the count of adds whose insertBefore anchor was missing (the order
    /// was rested at the back of its level instead). Nonzero means the book has
    /// diverged from the stream and should be re-synced.
    pub(super) fn take_insert_before_fallbacks(&mut self) -> u64 {
        std::mem::take(&mut self.insert_before_fallbacks)
    }

    /// Record that `insert_before` could not be honored for this order.
    fn note_insert_before_fallback(&mut self, oid: &Oid, coin: &Coin) {
        self.insert_before_fallbacks += 1;
        log::warn!("insertBefore anchor missing for oid={oid:?} coin={coin:?}; order rested at back of level");
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
    pub(super) fn compute_snapshot_for_coin(
        &self,
        coin: &Coin,
        band: PxBand,
    ) -> Option<(u64, u64, Snapshot<InnerL4Order>)> {
        self.order_book.snapshot_for_coin(coin, band).map(|snapshot| (self.time, self.height, snapshot))
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

    /// Count of untriggered trigger orders in the side table
    pub(super) fn untriggered_count(&self) -> usize {
        self.untriggered_orders.values().map(rustc_hash::FxHashMap::len).sum()
    }

    /// Untriggered trigger orders - all coins, or one coin's when `coin` is
    /// given - along with (time, height). Runs under the listener lock, but
    /// only bumps an Arc refcount per order (no deep clone); an unknown coin
    /// is an O(1) map miss. Callers convert/serialize off-lock (same
    /// discipline as l4Book).
    pub(super) fn untriggered_snapshot(&self, coin: Option<&Coin>) -> (u64, u64, Vec<std::sync::Arc<InnerL4Order>>) {
        let orders = match coin {
            Some(c) => self.untriggered_orders.get(c).map(|m| m.values().cloned().collect()).unwrap_or_default(),
            None => self.untriggered_orders.values().flat_map(|m| m.values().cloned()).collect(),
        };
        (self.time, self.height, orders)
    }

    /// Number of coins tracked in the orderbook
    pub(super) fn coin_count(&self) -> usize {
        self.order_book.as_ref().len()
    }

    /// Cleanup stale pending entries to prevent unbounded memory growth.
    ///
    /// Primary mechanism is AGE-based eviction: a half that has waited longer
    /// than `PENDING_MAX_AGE` will never pair (the two streams skew by
    /// milliseconds, not minutes). The old size-only force-clear nuked
    /// genuinely in-flight young halves whenever a burst pushed the map over
    /// the cap, forcing an avoidable 10-30s snapshot re-sync.
    ///
    /// Loss semantics differ per cache:
    /// - Aged-out `pending_order_statuses` are expected orphans (statuses with
    ///   `is_inserted_into_book() == true` whose order never rested, so no New
    ///   diff ever comes) - evicted silently, NOT data loss.
    /// - An aged-out `pending_new_diffs` entry means a New diff never got its
    ///   status: the book is missing that order, which IS data loss.
    ///
    /// The size caps remain as an OOM backstop; hitting one still force-clears
    /// (fresh `HashMap::new()` so the high-water-mark bucket capacity is
    /// actually released) and counts as data loss.
    /// Also opportunistically compacts the orderbook slab allocators on the same
    /// cadence, since both are unbounded-growth vectors that the maintenance tick
    /// is responsible for bounding.
    ///
    /// Returns `true` when potentially-live data was evicted; the caller must
    /// treat this as data loss and mark the book for re-sync.
    pub(super) fn cleanup_stale_pending(&mut self) -> bool {
        const MAX_PENDING_ORDERS: usize = 50_000;
        const MAX_PENDING_DIFFS: usize = 10_000;
        const PENDING_MAX_AGE: Duration = Duration::from_secs(60);

        let mut cleared = false;

        let before = self.pending_order_statuses.len();
        self.pending_order_statuses.retain(|_, (_, at)| at.elapsed() < PENDING_MAX_AGE);
        let aged_statuses = before - self.pending_order_statuses.len();
        if aged_statuses > 0 {
            // Expected orphans (order never rested -> no New diff): not data loss.
            log::info!("Evicted {aged_statuses} aged pending_order_statuses entries (no matching BookDiff)");
        }

        let before = self.pending_new_diffs.len();
        self.pending_new_diffs.retain(|_, (_, _, at)| at.elapsed() < PENDING_MAX_AGE);
        let aged_diffs = before - self.pending_new_diffs.len();
        if aged_diffs > 0 {
            // A New diff with no status in 60s: the order is missing from the book.
            log::warn!("Evicted {aged_diffs} aged pending_new_diffs entries (status never arrived - data loss)");
            cleared = true;
        }

        if self.pending_order_statuses.len() > MAX_PENDING_ORDERS {
            log::warn!(
                "Clearing stale pending_order_statuses cache: {} entries (orphaned orders without matching BookDiffs)",
                self.pending_order_statuses.len()
            );
            self.pending_order_statuses = rustc_hash::FxHashMap::default();
            cleared = true;
        }

        if self.pending_new_diffs.len() > MAX_PENDING_DIFFS {
            log::warn!("Clearing stale pending_new_diffs cache: {} entries", self.pending_new_diffs.len());
            self.pending_new_diffs = rustc_hash::FxHashMap::default();
            cleared = true;
        }

        let compacted = self.order_book.compact_all();
        if compacted > 0 {
            let (live, cap) = self.order_book.slab_stats();
            log::info!("Compacted {compacted} price-level slabs (live={live}, capacity={cap})");
        }
        cleared
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

            // Maintain the untriggered-orders side table. "open" + is_trigger is a
            // pending trigger order (never rests on the book); any other status is
            // terminal for the untriggered phase (canceled / triggered / filled /
            // rejected / ...) and evicts the oid. The eviction probe is two hash
            // lookups (coin via Borrow<str>, then oid) with no allocation - cheap
            // enough for the hot path.
            if !self.track_untriggered {
                // gated off (--bbo-only): the map stays empty
            } else if order_status.status == "open" {
                if order_status.order.is_trigger && !(self.ignore_spot && Coin::str_is_spot(&order_status.order.coin)) {
                    match InnerL4Order::try_from((order_status.user, order_status.order.clone())) {
                        Ok(inner) => {
                            self.untriggered_orders
                                .entry(inner.coin.clone())
                                .or_default()
                                .insert(oid.clone(), std::sync::Arc::new(inner));
                        }
                        Err(err) => {
                            // The endpoint under-reports this oid until the next
                            // re-sync; count it so the gap is visible in metrics,
                            // not just a log line.
                            crate::metrics::PARSE_ERRORS_TOTAL.with_label_values(&["untriggered"]).inc();
                            log::warn!("Skipping unparseable untriggered trigger order oid={oid:?}: {err}");
                        }
                    }
                }
            } else if let Some(coin_orders) = self.untriggered_orders.get_mut(order_status.order.coin.as_str())
                && coin_orders.remove(&oid).is_some()
            {
                // Labeled by status so a future non-terminal status string that
                // starts wrongly evicting live triggers shows up in Prometheus
                // immediately (today's vocabulary is all terminal-for-the-oid).
                crate::metrics::UNTRIGGERED_EVICTIONS_TOTAL.with_label_values(&[&order_status.status]).inc();
                if coin_orders.is_empty() {
                    // Drop the per-coin map once empty so delisted coins don't
                    // accumulate empty buckets forever.
                    self.untriggered_orders.remove(order_status.order.coin.as_str());
                }
            }

            // Check if there's a pending New diff for this order
            if let Some((sz, insert_before, _)) = self.pending_new_diffs.remove(&oid) {
                // Both arrived - add order immediately!
                let time = order_status.time.and_utc().timestamp_millis();
                let order_coin = Coin::new(&order_status.order.coin);
                let mut inner_order: InnerL4Order = order_status.try_into()?;
                inner_order.modify_sz(sz);
                inner_order.convert_trigger(time.max(0) as u64);
                if self.order_book.add_order_before(inner_order, insert_before) {
                    self.note_insert_before_fallback(&oid, &order_coin);
                }
                changed_coins.insert(order_coin.clone());
                log::debug!("Order added (status arrived after diff): oid={:?} coin={:?}", oid, order_coin);
            } else if order_status.is_inserted_into_book() {
                // Diff hasn't arrived yet - cache the OrderStatus
                self.pending_order_statuses.insert(oid, (order_status, Instant::now()));
            }
        }
        Ok(changed_coins)
    }

    #[cfg(test)]
    pub(crate) fn pending_order_statuses_has(&self, oid: &Oid) -> bool {
        self.pending_order_statuses.contains_key(oid)
    }

    /// Backdate every pending entry's insertion time, so tests can exercise
    /// age-based eviction without sleeping.
    #[cfg(test)]
    pub(crate) fn age_pending_entries(&mut self, by: Duration) {
        let backdated = Instant::now().checked_sub(by).unwrap_or_else(Instant::now);
        for (_, at) in self.pending_order_statuses.values_mut() {
            *at = backdated;
        }
        for (_, _, at) in self.pending_new_diffs.values_mut() {
            *at = backdated;
        }
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
                InnerOrderDiff::New { sz, insert_before } => {
                    // Check if OrderStatus already arrived
                    if let Some((order, _)) = self.pending_order_statuses.remove(&oid) {
                        // Both arrived - add order immediately!
                        let time = order.time.and_utc().timestamp_millis();
                        let order_coin = Coin::new(&order.order.coin);
                        let mut inner_order: InnerL4Order = order.try_into()?;
                        inner_order.modify_sz(sz);
                        #[allow(clippy::unwrap_used)]
                        inner_order.convert_trigger(time.try_into().unwrap());
                        if self.order_book.add_order_before(inner_order, insert_before) {
                            self.note_insert_before_fallback(&oid, &order_coin);
                        }
                        changed_coins.insert(order_coin.clone());
                        log::debug!("Order added (diff arrived after status): oid={:?} coin={:?}", oid, order_coin);
                    } else {
                        // Status hasn't arrived yet - cache the diff size + queue anchor
                        self.pending_new_diffs.insert(oid.clone(), (sz, insert_before, Instant::now()));
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
        OrderBookState::from_snapshot(snapshots, 0, 0, true, false, true)
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

    // ==================== Untriggered Trigger Orders ====================

    fn make_inner_order(coin: &str, oid: u64, is_trigger: bool) -> InnerL4Order {
        InnerL4Order {
            user: Address::new([1; 20]),
            coin: Coin::new(coin),
            side: crate::order_book::types::Side::Bid,
            limit_px: crate::order_book::Px::new(100_000_000),
            sz: crate::order_book::Sz::new(100_000_000),
            oid,
            timestamp: 1000,
            trigger_condition: if is_trigger { "Price above 110".to_string() } else { "N/A".to_string() },
            is_trigger,
            trigger_px: if is_trigger { "110.0".to_string() } else { "0.0".to_string() },
            is_position_tpsl: false,
            reduce_only: false,
            order_type: if is_trigger { "Stop Market".to_string() } else { "Limit".to_string() },
            tif: None,
            cloid: None,
        }
    }

    fn make_trigger_status(coin: &str, oid: u64, status: &str, trigger_px: &str) -> NodeDataOrderStatus {
        let mut order = make_l4_order(coin, oid);
        order.is_trigger = true;
        order.trigger_px = trigger_px.to_string();
        order.trigger_condition = format!("Price above {trigger_px}");
        order.order_type = "Stop Market".to_string();
        NodeDataOrderStatus {
            time: NaiveDateTime::parse_from_str("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            user: Address::new([2; 20]),
            hash: Some("0xdef".to_string()),
            builder: None,
            status: status.to_string(),
            order,
        }
    }

    /// Snapshots in the hl-node CLI layout: trigger orders appended to the TAIL
    /// of BOTH sides (same oid in each), after the resting book orders.
    fn snapshot_with_triggers(coin: &str, resting_oids: &[u64], trigger_oids: &[u64]) -> Snapshots<InnerL4Order> {
        let mut bids: Vec<InnerL4Order> = resting_oids.iter().map(|&oid| make_inner_order(coin, oid, false)).collect();
        let mut asks: Vec<InnerL4Order> = Vec::new();
        for &oid in trigger_oids {
            bids.push(make_inner_order(coin, oid, true));
            asks.push(make_inner_order(coin, oid, true));
        }
        let snapshot = Snapshot::from_sides(bids, asks);
        Snapshots::new(std::iter::once((Coin::new(coin), snapshot)).collect())
    }

    #[test]
    fn test_from_snapshot_extracts_untriggered_triggers() {
        let snapshots = snapshot_with_triggers("BTC", &[1, 2], &[100, 101]);
        let state = OrderBookState::from_snapshot(snapshots, 0, 0, true, false, true);
        // Triggers are kept in the side table (deduped from the two per-side
        // copies), not dropped - and never enter the book.
        assert_eq!(state.untriggered_count(), 2);
        assert_eq!(state.order_count(), 2);
        let (_, _, orders) = state.untriggered_snapshot(None);
        let mut oids: Vec<u64> = orders.iter().map(|o| o.oid).collect();
        oids.sort_unstable();
        assert_eq!(oids, vec![100, 101]);
        assert!(orders.iter().all(|o| o.is_trigger));
    }

    #[test]
    fn test_from_snapshot_ignore_triggers_false_keeps_old_semantics() {
        // Tests that opt out of trigger extraction must not populate the table.
        let snapshots = snapshot_with_triggers("BTC", &[1], &[]);
        let state = OrderBookState::from_snapshot(snapshots, 0, 0, false, false, true);
        assert_eq!(state.untriggered_count(), 0);
    }

    #[test]
    fn test_open_trigger_status_upserts_untriggered() {
        let mut state = empty_state();
        let status = make_trigger_status("BTC", 500, "open", "110.0");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        assert_eq!(state.untriggered_count(), 1);
        // It never rests on the book and is NOT cached for diff pairing
        // (is_inserted_into_book is false for open triggers).
        assert_eq!(state.order_count(), 0);
        assert_eq!(state.pending_order_statuses_count(), 0);

        // A later "open" for the same oid (modify) replaces the entry.
        let status = make_trigger_status("BTC", 500, "open", "115.0");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        assert_eq!(state.untriggered_count(), 1);
        let (_, _, orders) = state.untriggered_snapshot(None);
        assert_eq!(orders[0].trigger_px, "115.0");
    }

    #[test]
    fn test_canceled_trigger_status_removes_untriggered() {
        let mut state = empty_state();
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_trigger_status("BTC", 500, "open", "110.0")]))
            .unwrap();
        assert_eq!(state.untriggered_count(), 1);
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_trigger_status("BTC", 500, "canceled", "110.0")]))
            .unwrap();
        assert_eq!(state.untriggered_count(), 0);
    }

    #[test]
    fn test_triggered_status_moves_order_from_untriggered_to_book() {
        let mut state = empty_state();
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_trigger_status("BTC", 500, "open", "110.0")]))
            .unwrap();
        assert_eq!(state.untriggered_count(), 1);

        // Trigger fires: "triggered" status pairs with a New diff and the order
        // rests on the book; the untriggered entry must be evicted.
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_trigger_status("BTC", 500, "triggered", "110.0")]))
            .unwrap();
        let diff = make_order_diff("BTC", 500, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert_eq!(state.untriggered_count(), 0);
        assert_eq!(state.order_count(), 1);
    }

    #[test]
    fn test_non_trigger_statuses_leave_untriggered_untouched() {
        let mut state = empty_state();
        add_resting_order(&mut state, "BTC", 42);
        state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("BTC", 42, "filled")])).unwrap();
        assert_eq!(state.untriggered_count(), 0);
        assert_eq!(state.order_count(), 1);
    }

    #[test]
    fn test_spot_triggers_skipped_when_ignore_spot() {
        // Live path: spot trigger opens are not tracked under ignore_spot,
        // matching the diff path's spot filtering.
        let mut state = OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 0, 0, true, true, true);
        state
            .apply_order_statuses_hft(make_status_batch(vec![
                make_trigger_status("@1", 1, "open", "1.0"),
                make_trigger_status("PURR/USDC", 2, "open", "1.0"),
                make_trigger_status("BTC", 3, "open", "110.0"),
            ]))
            .unwrap();
        assert_eq!(state.untriggered_count(), 1);
        let (_, _, orders) = state.untriggered_snapshot(None);
        assert_eq!(orders[0].coin, Coin::new("BTC"));

        // Snapshot path: spot triggers are likewise dropped at install.
        let spot_state =
            OrderBookState::from_snapshot(snapshot_with_triggers("@1", &[1], &[100]), 0, 0, true, true, true);
        assert_eq!(spot_state.untriggered_count(), 0);
    }

    #[test]
    fn test_untriggered_tracking_gated_off() {
        // --bbo-only: neither the snapshot extraction nor live statuses populate
        // the table (the book still strips triggers), so the lightweight memory
        // envelope holds.
        let mut state =
            OrderBookState::from_snapshot(snapshot_with_triggers("BTC", &[1], &[100]), 0, 0, true, false, false);
        assert_eq!(state.untriggered_count(), 0);
        assert_eq!(state.order_count(), 1, "book triggers must still be stripped when tracking is off");
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_trigger_status("BTC", 500, "open", "110.0")]))
            .unwrap();
        assert_eq!(state.untriggered_count(), 0);
    }

    #[test]
    fn test_untriggered_snapshot_coin_filter() {
        let mut state = empty_state();
        state
            .apply_order_statuses_hft(make_status_batch(vec![
                make_trigger_status("BTC", 1, "open", "110.0"),
                make_trigger_status("ETH", 2, "open", "110.0"),
                make_trigger_status("ETH", 3, "open", "110.0"),
            ]))
            .unwrap();
        let (_, _, all) = state.untriggered_snapshot(None);
        assert_eq!(all.len(), 3);
        let (_, _, eth) = state.untriggered_snapshot(Some(&Coin::new("ETH")));
        assert_eq!(eth.len(), 2);
        assert!(eth.iter().all(|o| o.coin == Coin::new("ETH")));
        let (_, _, none) = state.untriggered_snapshot(Some(&Coin::new("SOL")));
        assert!(none.is_empty());
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
        let diff = make_order_diff("BTC", 42, OrderDiff::New { sz: "1.5".to_string(), insert_before: None });
        let batch = make_diff_batch(vec![diff]);
        let changed = state.apply_order_diffs_hft(batch).unwrap();
        assert!(changed.contains(&Coin::new("BTC")));
        assert_eq!(state.pending_order_statuses_count(), 0); // consumed
        assert_eq!(state.order_count(), 1);
    }

    // ==================== insertBefore (ALO priority) ====================

    /// Rest an order via the paired status+diff HFT flow (all helpers use px 100.0,
    /// side Bid, so every order lands on the same level of the same book).
    fn add_resting_order(state: &mut OrderBookState, coin: &str, oid: u64) {
        let status = make_order_status(coin, oid, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff(coin, oid, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
    }

    /// Bid-side queue order (front first) for the coin's single price level.
    fn bid_queue_oids(state: &OrderBookState, coin: &str) -> Vec<Oid> {
        let (_, _, snapshot) = state.compute_snapshot_for_coin(&Coin::new(coin), PxBand::default()).unwrap();
        snapshot.as_ref()[0].iter().map(InnerOrder::oid).collect()
    }

    #[test]
    fn test_insert_before_splices_ahead_of_anchor() {
        let mut state = empty_state();
        add_resting_order(&mut state, "BTC", 1);

        // Priority order 2 jumps in front of resting order 1 (status first, then diff)
        let status = make_order_status("BTC", 2, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 2, OrderDiff::New { sz: "1.0".to_string(), insert_before: Some(1) });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();

        assert_eq!(bid_queue_oids(&state, "BTC"), vec![Oid::new(2), Oid::new(1)]);
        assert_eq!(state.take_insert_before_fallbacks(), 0);
    }

    #[test]
    fn test_insert_before_survives_pending_diff_cache() {
        let mut state = empty_state();
        add_resting_order(&mut state, "BTC", 1);

        // Diff for order 2 arrives BEFORE its status: the anchor must survive the cache
        let diff = make_order_diff("BTC", 2, OrderDiff::New { sz: "1.0".to_string(), insert_before: Some(1) });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert_eq!(state.pending_new_diffs_count(), 1);
        assert_eq!(state.order_count(), 1); // not added yet

        let status = make_order_status("BTC", 2, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();

        assert_eq!(bid_queue_oids(&state, "BTC"), vec![Oid::new(2), Oid::new(1)]);
        assert_eq!(state.take_insert_before_fallbacks(), 0);
    }

    #[test]
    fn test_insert_before_missing_anchor_falls_back() {
        let mut state = empty_state();
        add_resting_order(&mut state, "BTC", 1);

        // Anchor 999 is not on the book: the order must still rest (at the back
        // of the level) and the batch must NOT error out
        let status = make_order_status("BTC", 2, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 2, OrderDiff::New { sz: "1.0".to_string(), insert_before: Some(999) });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();

        assert_eq!(state.order_count(), 2);
        assert_eq!(bid_queue_oids(&state, "BTC"), vec![Oid::new(1), Oid::new(2)]);
        // The divergence is surfaced exactly once, then the counter resets
        assert_eq!(state.take_insert_before_fallbacks(), 1);
        assert_eq!(state.take_insert_before_fallbacks(), 0);
    }

    // ==================== Bidirectional Cache: Diff First ====================

    #[test]
    fn test_diff_first_then_status_adds_order() {
        let mut state = empty_state();

        // 1. OrderDiff::New arrives first → size cached
        let diff = make_order_diff("ETH", 99, OrderDiff::New { sz: "2.0".to_string(), insert_before: None });
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
        let diff = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
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
        let diff = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
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
        let mut state = OrderBookState::from_snapshot(snapshots, 0, 0, true, true, true); // ignore_spot=true

        let diff = make_order_diff("@1", 1, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert!(changed.is_empty());
        assert_eq!(state.pending_new_diffs_count(), 0); // skipped entirely
    }

    #[test]
    fn test_spot_not_filtered_when_not_ignoring() {
        let mut state = empty_state(); // ignore_spot=false
        let diff = make_order_diff("@1", 1, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
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

    #[test]
    fn test_cleanup_evicts_aged_statuses_silently() {
        let mut state = empty_state();
        for i in 0..100u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        }
        state.age_pending_entries(std::time::Duration::from_secs(61));
        // Aged statuses are expected orphans (order never rested) - NOT data loss.
        assert!(!state.cleanup_stale_pending(), "aged status eviction must not force a re-sync");
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    #[test]
    fn test_cleanup_evicts_aged_diffs_as_data_loss() {
        let mut state = empty_state();
        for i in 0..100u64 {
            let diff = make_order_diff("BTC", i, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        state.age_pending_entries(std::time::Duration::from_secs(61));
        // A New diff whose status never arrived means the book is missing an order.
        assert!(state.cleanup_stale_pending(), "aged diff eviction is data loss and must trigger a re-sync");
        assert_eq!(state.pending_new_diffs_count(), 0);
    }

    #[test]
    fn test_cleanup_keeps_young_entries() {
        // Regression for the burst-nuke behavior: young in-flight halves must
        // survive cleanup so they can still pair with their other half.
        let mut state = empty_state();
        for i in 0..100u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
            let diff = make_order_diff("ETH", 1_000 + i, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        assert!(!state.cleanup_stale_pending());
        assert_eq!(state.pending_order_statuses_count(), 100);
        assert_eq!(state.pending_new_diffs_count(), 100);
    }

    #[test]
    fn test_cleanup_below_threshold_no_op() {
        let mut state = empty_state();
        for i in 0..100u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        }
        assert!(!state.cleanup_stale_pending(), "below-threshold cleanup is not data loss");
        assert_eq!(state.pending_order_statuses_count(), 100); // not cleared
    }

    // ==================== Per-coin L4 snapshot ====================

    #[test]
    fn test_compute_snapshot_for_coin_returns_only_that_coin() {
        let mut state = empty_state();
        for (i, coin) in ["BTC", "ETH"].iter().enumerate() {
            let status = make_order_status(coin, i as u64, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
            let diff = make_order_diff(coin, i as u64, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }

        let (_time, height, snapshot) = state.compute_snapshot_for_coin(&Coin::new("BTC"), PxBand::default()).unwrap();
        assert_eq!(height, 100); // batch helpers stamp block_number 100
        let [bids, asks] = snapshot.as_ref();
        assert_eq!(bids.len(), 1, "only BTC's single bid is included");
        assert!(asks.is_empty());

        assert!(
            state.compute_snapshot_for_coin(&Coin::new("DOGE"), PxBand::default()).is_none(),
            "unknown coin yields None"
        );
    }

    #[test]
    fn test_compute_snapshot_for_coin_band_filters_and_keeps_time_height() {
        let mut state = empty_state();
        for (oid, px) in [(1u64, "50000.0"), (2, "60000.0"), (3, "70000.0")] {
            let mut status = make_order_status("BTC", oid, "open");
            status.order.limit_px = px.to_string();
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
            let diff = make_order_diff("BTC", oid, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        let (full_time, full_height, _) =
            state.compute_snapshot_for_coin(&Coin::new("BTC"), PxBand::default()).unwrap();

        let band = PxBand::parse(Some("55000"), Some("65000")).unwrap();
        let (time, height, snapshot) = state.compute_snapshot_for_coin(&Coin::new("BTC"), band).unwrap();
        assert_eq!((time, height), (full_time, full_height), "band must not change time/height stamping");
        let [bids, asks] = snapshot.as_ref();
        assert_eq!(bids.iter().map(|o| o.oid).collect::<Vec<_>>(), vec![2], "only the in-band bid survives");
        assert!(asks.is_empty());

        // A band matching nothing still yields a (time, height, empty snapshot),
        // not None - only a missing coin book is an error to the subscriber.
        let empty_band = PxBand::parse(Some("80000"), Some("90000")).unwrap();
        let (_, _, snapshot) = state.compute_snapshot_for_coin(&Coin::new("BTC"), empty_band).unwrap();
        let [bids, asks] = snapshot.as_ref();
        assert!(bids.is_empty() && asks.is_empty());
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
            let diff = make_order_diff("BTC", i, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
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
            let diff = make_order_diff("BTC", i, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
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
            let diff = make_order_diff(coin, i as u64, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        let universe = state.compute_universe();
        assert_eq!(universe.len(), 3);
        assert!(universe.contains(&Coin::new("BTC")));
        assert!(universe.contains(&Coin::new("ETH")));
        assert!(universe.contains(&Coin::new("SOL")));
    }
}
