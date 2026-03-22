use crate::{
    listeners::order_book::{L2Snapshots, TimedSnapshots, utils::compute_l2_snapshots},
    order_book::{
        Coin, InnerOrder, Oid,
        multi_book::{OrderBooks, Snapshots},
    },
    prelude::*,
    types::{
        inner::{InnerL4Order, InnerOrderDiff},
        node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub(super) struct OrderBookState {
    order_book: OrderBooks<InnerL4Order>,
    height: u64,
    time: u64,
    ignore_spot: bool,
    // Persistent cache of OrderStatuses waiting for their New diffs
    // Allows OrderStatus and OrderDiff to arrive in any order (HFT-compatible)
    pending_order_statuses: HashMap<Oid, NodeDataOrderStatus>,
    // Persistent cache of New diffs (sz values) waiting for their OrderStatuses
    // This is the other half of bidirectional caching - handles when Diff arrives BEFORE Status
    pending_new_diffs: HashMap<Oid, crate::order_book::types::Sz>,
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

    pub(super) const fn time(&self) -> u64 {
        self.time
    }

    // forcibly take snapshot - (time, height, snapshot)
    pub(super) fn compute_snapshot(&self) -> TimedSnapshots {
        TimedSnapshots { time: self.time, height: self.height, snapshot: self.order_book.to_snapshots_par() }
    }

    // Always returns fresh L2 snapshots (no caching/flag check)
    // Used for real-time streaming updates to L2/BBO subscribers
    pub(super) fn l2_snapshots_uncached(&self) -> (u64, L2Snapshots) {
        (self.time, compute_l2_snapshots(&self.order_book))
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

    /// Cleanup stale pending entries to prevent unbounded memory growth
    /// Orphaned entries occur when OrderStatuses have is_inserted_into_book() = true
    /// but their matching BookDiff never arrives (network issues, bugs, etc.)
    /// This is a simple size-based eviction - when cache exceeds limit, clear oldest half
    pub(super) fn cleanup_stale_pending(&mut self) {
        const MAX_PENDING_ORDERS: usize = 10_000;
        const MAX_PENDING_DIFFS: usize = 1_000;

        // Clear oldest entries by just clearing the entire cache when too large
        // This is simpler than tracking insertion order
        if self.pending_order_statuses.len() > MAX_PENDING_ORDERS {
            log::warn!(
                "Clearing stale pending_order_statuses cache: {} entries (orphaned orders without matching BookDiffs)",
                self.pending_order_statuses.len()
            );
            self.pending_order_statuses.clear();
        }

        if self.pending_new_diffs.len() > MAX_PENDING_DIFFS {
            log::warn!("Clearing stale pending_new_diffs cache: {} entries", self.pending_new_diffs.len());
            self.pending_new_diffs.clear();
        }
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
            if let Some(sz) = self.pending_new_diffs.remove(&oid) {
                // Both arrived - add order immediately!
                let time = order_status.time.and_utc().timestamp_millis();
                let order_coin = Coin::new(&order_status.order.coin);
                let mut inner_order: InnerL4Order = order_status.try_into()?;
                inner_order.modify_sz(sz);
                #[allow(clippy::unwrap_used)]
                inner_order.convert_trigger(time.try_into().unwrap());
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
                        // Status hasn't arrived yet - cache the diff size
                        self.pending_new_diffs.insert(oid.clone(), sz);
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
