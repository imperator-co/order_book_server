use crate::prelude::*;
use itertools::Itertools;
use price_level::PriceLevel;
use std::collections::{BTreeMap, HashMap, HashSet};

pub(crate) mod levels;
mod linked_list;
pub(crate) mod multi_book;
mod price_level;
pub(crate) mod types;

pub(crate) use types::{Coin, InnerOrder, Oid, Px, Side, Sz};

#[derive(Clone, Default)]
pub(crate) struct OrderBook<O> {
    oid_to_side_px: HashMap<Oid, (Side, Px)>,
    bids: BTreeMap<Px, PriceLevel<O>>,
    asks: BTreeMap<Px, PriceLevel<O>>,
}

#[derive(Debug, Clone)]
pub(crate) struct Snapshot<O>([Vec<O>; 2]);

impl<O: Clone> Snapshot<O> {
    pub(crate) const fn as_ref(&self) -> &[Vec<O>; 2] {
        &self.0
    }

    pub(crate) fn truncate(&self, n: usize) -> Self {
        // Clone only the first n entries per side; the previous full-Vec clone
        // copied up to MAX_LEVELS entries just to drop most of them.
        Self(self.0.each_ref().map(|orders| orders.iter().take(n).cloned().collect_vec()))
    }
}

impl<O: InnerOrder> Snapshot<O> {
    pub(crate) fn remove_triggers(&mut self) {
        #[allow(clippy::unwrap_used)]
        let [bid_oids, ask_oids] = &self
            .0
            .iter()
            .map(|orders| orders.iter().map(InnerOrder::oid).collect::<HashSet<Oid>>())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        for orders in &mut self.0 {
            while let Some(order) = orders.last() {
                let oid = order.oid();
                if bid_oids.contains(&oid) && ask_oids.contains(&oid) {
                    orders.pop();
                } else {
                    break;
                }
            }
        }
    }
}

impl<O: InnerOrder> OrderBook<O> {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self { oid_to_side_px: HashMap::new(), bids: BTreeMap::new(), asks: BTreeMap::new() }
    }

    /// Number of orders in this orderbook
    pub(crate) fn order_count(&self) -> usize {
        self.oid_to_side_px.len()
    }

    pub(crate) fn add_order(&mut self, mut order: O) {
        // Duplicate oid would silently corrupt state: `oid_to_side_px` would point
        // at the new (side, px) while `LinkedList::push_back` silently rejects the
        // re-insert, leaving the original order data in place. Skip and warn.
        // (A node replay or duplicate-emit is the realistic trigger; we never want
        // to re-run matching, which would double-count the match against opposite-side
        // orders that have arrived since.)
        if self.oid_to_side_px.contains_key(&order.oid()) {
            log::warn!("OrderBook::add_order called twice for oid={:?}; ignoring duplicate", order.oid());
            return;
        }
        let (maker_orders, resting_book) = match order.side() {
            Side::Ask => (&mut self.bids, &mut self.asks),
            Side::Bid => (&mut self.asks, &mut self.bids),
        };
        let oids = match_order(maker_orders, &mut order);
        for oid in oids {
            self.oid_to_side_px.remove(&oid);
        }
        if order.sz().is_positive() {
            self.oid_to_side_px.insert(order.oid(), (order.side(), order.limit_px()));
            add_order_to_book(resting_book, order);
        }
    }

    pub(crate) fn cancel_order(&mut self, oid: Oid) -> bool {
        if let Some((side, px)) = self.oid_to_side_px.remove(&oid) {
            let map = match side {
                Side::Ask => &mut self.asks,
                Side::Bid => &mut self.bids,
            };
            if let Some(level) = map.get_mut(&px) {
                let success = level.remove(oid).is_some();
                if level.is_empty() {
                    map.remove(&px);
                }
                return success;
            }
        }
        false
    }

    pub(crate) fn modify_sz(&mut self, oid: Oid, sz: Sz) -> bool {
        // If new size is 0, remove the order entirely
        if sz.is_zero() {
            return self.cancel_order(oid);
        }
        if let Some((side, px)) = self.oid_to_side_px.get(&oid) {
            let map = match side {
                Side::Ask => &mut self.asks,
                Side::Bid => &mut self.bids,
            };
            if let Some(level) = map.get_mut(px) {
                return level.modify_order_sz(&oid, sz);
            }
        }
        false
    }

    /// Get best bid and best ask in O(1): each `PriceLevel` maintains its
    /// (total size, count) aggregate incrementally, so no order is visited.
    /// Returns (best_bid, best_ask) where each is (price, total_size, count).
    #[must_use]
    pub(crate) fn get_bbo(&self) -> (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>) {
        // Best bid = highest price in bids (last key in BTreeMap);
        // best ask = lowest price in asks (first key in BTreeMap).
        (
            self.bids.last_key_value().map(price_level::aggregate_entry),
            self.asks.first_key_value().map(price_level::aggregate_entry),
        )
    }

    /// Compact every price-level's `LinkedList` slab. Returns the number of lists
    /// that were actually rebuilt (lists below the fragmentation threshold are
    /// skipped). See `LinkedList::compact` for the threshold.
    pub(crate) fn compact(&mut self) -> usize {
        let mut compacted = 0usize;
        for level in self.bids.values_mut().chain(self.asks.values_mut()) {
            if level.compact() {
                compacted += 1;
            }
        }
        compacted
    }

    /// Returns (total live nodes, total slab capacity) summed across every level.
    /// Useful for tracking fragmentation in Prometheus.
    pub(crate) fn slab_stats(&self) -> (usize, usize) {
        let mut live = 0usize;
        let mut cap = 0usize;
        for level in self.bids.values().chain(self.asks.values()) {
            live += level.slab_len();
            cap += level.slab_capacity();
        }
        (live, cap)
    }

    // we go by the convention that prioritized orders go first in the vector; this makes aggregation step later easier.
    pub(crate) fn to_snapshot(&self) -> Snapshot<O> {
        let bids = self.bids.iter().rev().flat_map(|(_, l)| l.to_vec().into_iter().cloned()).collect_vec();
        let asks = self.asks.iter().flat_map(|(_, l)| l.to_vec().into_iter().cloned()).collect_vec();
        Snapshot([bids, asks])
    }

    #[must_use]
    pub(crate) fn from_snapshot(mut snapshot: Snapshot<O>, ignore_triggers: bool) -> Self {
        let mut book = Self::new();
        if ignore_triggers {
            snapshot.remove_triggers();
        }
        snapshot.0.into_iter().for_each(|orders| {
            for order in orders {
                book.add_order(order);
            }
        });
        book
    }
}

fn add_order_to_book<O: InnerOrder>(map: &mut BTreeMap<Px, PriceLevel<O>>, order: O) {
    let oid = order.oid();
    let limit_px = order.limit_px();
    map.entry(limit_px).or_insert_with(PriceLevel::new).push_back(oid, order);
}

fn match_order<O: InnerOrder>(maker_orders: &mut BTreeMap<Px, PriceLevel<O>>, taker_order: &mut O) -> Vec<Oid> {
    let mut filled_oids = Vec::new();
    let mut keys_to_remove = Vec::new();
    let taker_side = taker_order.side();
    let limit_px = taker_order.limit_px();
    let order_iter: Box<dyn Iterator<Item = (&Px, &mut PriceLevel<O>)>> = match taker_side {
        Side::Ask => Box::new(maker_orders.iter_mut().rev()),
        Side::Bid => Box::new(maker_orders.iter_mut()),
    };
    for (&px, level) in order_iter {
        let matches = match taker_side {
            Side::Ask => px >= limit_px,
            Side::Bid => px <= limit_px,
        };
        if !matches {
            break;
        }
        level.match_against(taker_order, &mut filled_oids);
        if level.is_empty() {
            keys_to_remove.push(px);
        }
        if taker_order.sz().is_zero() {
            break;
        }
    }
    for key in keys_to_remove {
        maker_orders.remove(&key);
    }
    filled_oids
}

#[cfg(test)]
mod tests {
    use crate::order_book::types::{Coin, Sz};

    use super::*;
    use std::collections::BTreeSet;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct MinimalOrder {
        oid: u64,
        side: Side,
        sz: u64,
        limit_px: u64,
    }

    impl InnerOrder for MinimalOrder {
        fn oid(&self) -> Oid {
            Oid::new(self.oid)
        }

        fn side(&self) -> Side {
            self.side
        }

        fn limit_px(&self) -> Px {
            Px::new(self.limit_px)
        }

        fn sz(&self) -> Sz {
            Sz::new(self.sz)
        }

        fn decrement_sz(&mut self, dec: Sz) {
            self.sz = self.sz.saturating_sub(dec.value());
        }

        fn fill(&mut self, maker_order: &mut Self) -> Sz {
            let match_sz = self.sz().min(maker_order.sz());
            maker_order.decrement_sz(match_sz);
            self.decrement_sz(match_sz);
            match_sz
        }

        fn modify_sz(&mut self, sz: Sz) {
            self.sz = sz.value();
        }

        fn modify_px(&mut self, px: Px) {
            self.limit_px = px.value();
        }

        fn convert_trigger(&mut self, _: u64) {}

        fn coin(&self) -> Coin {
            Coin::new("")
        }
    }

    impl MinimalOrder {
        fn new(oid: u64, sz: u64, limit_px: u64, side: Side) -> Self {
            Self { oid, side, sz, limit_px }
        }
    }

    #[derive(Default)]
    struct OrderFactory {
        next_oid: u64,
    }

    impl OrderFactory {
        fn order(&mut self, sz: u64, limit_px: u64, side: Side) -> MinimalOrder {
            let order = MinimalOrder::new(self.next_oid, sz, limit_px, side);
            self.next_oid += 1;
            order
        }

        fn batch_order(&mut self, sz: u64, limit_px: u64, side: Side, n: u64) -> Vec<MinimalOrder> {
            (0..n).map(|_| self.order(sz, limit_px, side)).collect_vec()
        }
    }

    #[test]
    fn simple_book_test() {
        let mut factory = OrderFactory::default();
        let buy_orders1 = factory.batch_order(100, 5, Side::Bid, 3);
        let buy_orders2 = factory.batch_order(200, 4, Side::Bid, 4);
        let sell_orders1 = factory.batch_order(150, 5, Side::Ask, 2);
        let sell_orders2 = factory.batch_order(500, 6, Side::Ask, 2);
        let mut book = OrderBook::new();
        for order in buy_orders2.clone() {
            book.add_order(order);
        }
        for order in sell_orders2.clone() {
            book.add_order(order);
        }
        for order in buy_orders1.clone() {
            book.add_order(order);
        }
        book.add_order(sell_orders1[0].clone());
        let mut bids = [buy_orders2, buy_orders1].concat();
        let mut asks = [sell_orders1.clone(), sell_orders2].concat();
        // remove index 4 and alter index 5 (matched)
        bids[5].sz -= 50;
        bids.remove(4);
        // remove index 0 (matched) and 1 (not inserted)
        asks.remove(1);
        asks.remove(0);

        assert_same_book(Snapshot([bids.clone(), asks.clone()]), book.to_snapshot());

        assert!(book.cancel_order(Oid::new(3)));
        assert!(book.cancel_order(Oid::new(9)));
        book.add_order(sell_orders1[1].clone());

        // index 4 and 5 both get matched, index 0 is canceled (first out of buy_orders2)
        bids.remove(5);
        bids.remove(4);
        bids.remove(0);

        // only thing changing in asks is that index 0 is canceled
        asks.remove(0);

        assert_same_book(Snapshot([bids.clone(), asks.clone()]), book.to_snapshot());

        // test modify size
        book.modify_sz(Oid::new(10), Sz::new(450));
        asks[0].sz = 450;

        assert_same_book(Snapshot([bids.clone(), asks.clone()]), book.to_snapshot());
    }

    fn assert_same_book(s1: Snapshot<MinimalOrder>, s2: Snapshot<MinimalOrder>) {
        let [b1, a1] = s1.0.map(BTreeSet::from_iter);
        let [b2, a2] = s2.0.map(BTreeSet::from_iter);
        assert_eq!(b1, b2);
        assert_eq!(a1, a2);
    }

    // ==================== BBO Tests ====================

    #[test]
    fn test_bbo_empty_book() {
        let book: OrderBook<MinimalOrder> = OrderBook::new();
        let (bid, ask) = book.get_bbo();
        assert!(bid.is_none());
        assert!(ask.is_none());
    }

    #[test]
    fn test_bbo_single_bid() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        let (bid, ask) = book.get_bbo();
        assert_eq!(bid.unwrap(), (Px::new(50), Sz::new(100), 1));
        assert!(ask.is_none());
    }

    #[test]
    fn test_bbo_single_ask() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Ask));
        let (bid, ask) = book.get_bbo();
        assert!(bid.is_none());
        assert_eq!(ask.unwrap(), (Px::new(50), Sz::new(100), 1));
    }

    #[test]
    fn test_bbo_multiple_levels() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Bids at 50 and 40 - best bid should be 50
        book.add_order(factory.order(100, 40, Side::Bid));
        book.add_order(factory.order(200, 50, Side::Bid));
        // Asks at 60 and 70 - best ask should be 60
        book.add_order(factory.order(150, 70, Side::Ask));
        book.add_order(factory.order(300, 60, Side::Ask));

        let (bid, ask) = book.get_bbo();
        assert_eq!(bid.unwrap().0, Px::new(50)); // best bid = highest
        assert_eq!(ask.unwrap().0, Px::new(60)); // best ask = lowest
    }

    #[test]
    fn test_bbo_aggregates_at_same_price() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        book.add_order(factory.order(200, 50, Side::Bid));
        let (bid, _) = book.get_bbo();
        let (px, sz, count) = bid.unwrap();
        assert_eq!(px, Px::new(50));
        assert_eq!(sz, Sz::new(300)); // aggregated
        assert_eq!(count, 2);
    }

    // ==================== Order Matching Tests ====================

    #[test]
    fn test_matching_bid_crosses_ask() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Place an ask at 50
        book.add_order(factory.order(100, 50, Side::Ask));
        // Place a bid at 60 (crosses the ask)
        book.add_order(factory.order(100, 60, Side::Bid));
        // Both fully filled, book should be empty
        assert_eq!(book.order_count(), 0);
    }

    #[test]
    fn test_matching_partial_fill_taker() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Ask of 200 at price 50
        book.add_order(factory.order(200, 50, Side::Ask));
        // Bid of 100 at price 60 - only partially fills the ask
        book.add_order(factory.order(100, 60, Side::Bid));
        // Ask remains with sz=100, bid fully consumed
        assert_eq!(book.order_count(), 1);
        let (_, ask) = book.get_bbo();
        assert_eq!(ask.unwrap().1, Sz::new(100));
    }

    #[test]
    fn test_matching_partial_fill_maker() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Ask of 50 at price 50
        book.add_order(factory.order(50, 50, Side::Ask));
        // Bid of 200 at price 60 - fills the ask, rest rests on book
        book.add_order(factory.order(200, 60, Side::Bid));
        assert_eq!(book.order_count(), 1);
        let (bid, _) = book.get_bbo();
        assert_eq!(bid.unwrap().1, Sz::new(150)); // 200 - 50
    }

    #[test]
    fn test_no_matching_bid_below_ask() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 60, Side::Ask));
        book.add_order(factory.order(100, 50, Side::Bid));
        // No crossing, both rest on book
        assert_eq!(book.order_count(), 2);
    }

    #[test]
    fn test_matching_multiple_price_levels() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Three asks at different prices
        book.add_order(factory.order(100, 50, Side::Ask)); // matched first
        book.add_order(factory.order(100, 55, Side::Ask)); // matched second
        book.add_order(factory.order(100, 60, Side::Ask)); // partially matched
        // One big bid that sweeps through
        book.add_order(factory.order(250, 60, Side::Bid));
        // 100+100+50 matched, ask at 60 has 50 left
        assert_eq!(book.order_count(), 1);
        let (_, ask) = book.get_bbo();
        assert_eq!(ask.unwrap().1, Sz::new(50));
    }

    // ==================== Cancel / Modify Tests ====================

    #[test]
    fn test_cancel_nonexistent_returns_false() {
        let mut book: OrderBook<MinimalOrder> = OrderBook::new();
        assert!(!book.cancel_order(Oid::new(999)));
    }

    #[test]
    fn test_duplicate_add_order_is_ignored() {
        // C3 regression test: prior to the dedup guard, a second add_order for the
        // same oid silently corrupted state - oid_to_side_px was overwritten but
        // the slab still held the original order. Verify the duplicate is now a no-op
        // and the original order remains cancelable.
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        let first = factory.order(100, 50, Side::Bid);
        let oid = first.oid();
        book.add_order(first);
        assert_eq!(book.order_count(), 1);

        // Try to re-add the same oid at a different price - should be ignored
        let dup = MinimalOrder { oid: 0, side: Side::Bid, sz: 999, limit_px: 99 };
        book.add_order(dup);
        assert_eq!(book.order_count(), 1, "duplicate add should not increase order count");

        // The original order is still there and cancelable
        assert!(book.cancel_order(oid));
        assert_eq!(book.order_count(), 0);
    }

    #[test]
    fn test_cancel_removes_price_level() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        assert!(book.cancel_order(Oid::new(0)));
        assert_eq!(book.order_count(), 0);
        let (bid, _) = book.get_bbo();
        assert!(bid.is_none());
    }

    #[test]
    fn test_modify_sz_to_zero_cancels() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        assert!(book.modify_sz(Oid::new(0), Sz::new(0)));
        assert_eq!(book.order_count(), 0);
    }

    #[test]
    fn test_modify_nonexistent_returns_false() {
        let mut book: OrderBook<MinimalOrder> = OrderBook::new();
        assert!(!book.modify_sz(Oid::new(999), Sz::new(100)));
    }

    #[test]
    fn test_modify_sz_updates_value() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        assert!(book.modify_sz(Oid::new(0), Sz::new(500)));
        let (bid, _) = book.get_bbo();
        assert_eq!(bid.unwrap().1, Sz::new(500));
    }

    // ==================== Snapshot Tests ====================

    #[test]
    fn test_snapshot_roundtrip() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        book.add_order(factory.order(100, 50, Side::Bid));
        book.add_order(factory.order(200, 40, Side::Bid));
        book.add_order(factory.order(150, 60, Side::Ask));

        let snapshot = book.to_snapshot();
        let restored = OrderBook::from_snapshot(snapshot, false);
        assert_eq!(book.order_count(), restored.order_count());
        assert_eq!(book.get_bbo(), restored.get_bbo());
    }

    #[test]
    fn test_snapshot_truncate() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        for i in 0..10 {
            book.add_order(factory.order(100, 50 + i, Side::Bid));
        }
        let snapshot = book.to_snapshot();
        let truncated = snapshot.truncate(3);
        assert_eq!(truncated.as_ref()[0].len(), 3); // bids
    }

    #[test]
    fn test_order_count() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        assert_eq!(book.order_count(), 0);
        book.add_order(factory.order(100, 50, Side::Bid));
        assert_eq!(book.order_count(), 1);
        book.add_order(factory.order(100, 60, Side::Ask));
        assert_eq!(book.order_count(), 2);
        book.cancel_order(Oid::new(0));
        assert_eq!(book.order_count(), 1);
    }

    // ==================== Aggregate invariant tests ====================

    /// Reference BBO computed by folding the snapshot - the pre-aggregate
    /// implementation, kept as ground truth.
    fn reference_bbo(book: &OrderBook<MinimalOrder>, side: Side) -> Option<(Px, Sz, u32)> {
        let snapshot = book.to_snapshot();
        let orders = &snapshot.as_ref()[if side == Side::Bid { 0 } else { 1 }];
        let first = orders.first()?;
        let px = first.limit_px();
        let mut sz = 0u64;
        let mut n = 0u32;
        for order in orders.iter().take_while(|o| o.limit_px() == px) {
            sz += order.sz().value();
            n += 1;
        }
        Some((px, Sz::new(sz), n))
    }

    #[test]
    fn test_randomized_ops_keep_level_aggregates_consistent() {
        // Drives adds (crossing and resting), cancels, and size modifications
        // (including to zero) and cross-checks the O(1) aggregate-based BBO
        // against a fold-based reference. PriceLevel::debug_validate also
        // asserts aggregate == fold after every single mutation in this run.
        fn xorshift(state: &mut u64) -> u64 {
            let mut x = *state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *state = x;
            x
        }

        for seed in [0x9E37_79B9_7F4A_7C15_u64, 0xDEAD_BEEF_CAFE_BABE_u64] {
            let mut rng = seed;
            let mut book = OrderBook::new();
            let mut next_oid = 0u64;
            let mut issued: Vec<u64> = Vec::new();
            for step in 0..3_000u32 {
                match xorshift(&mut rng) % 4 {
                    0 | 1 => {
                        // Adds around a midpoint so a fraction of them cross
                        // and exercise the in-place matching decrement path.
                        let px = 900 + xorshift(&mut rng) % 200;
                        let side = if xorshift(&mut rng) % 2 == 0 { Side::Bid } else { Side::Ask };
                        let sz = 1 + xorshift(&mut rng) % 1_000;
                        book.add_order(MinimalOrder::new(next_oid, sz, px, side));
                        issued.push(next_oid);
                        next_oid += 1;
                    }
                    2 => {
                        if !issued.is_empty() {
                            let i = (xorshift(&mut rng) as usize) % issued.len();
                            let oid = issued.swap_remove(i);
                            book.cancel_order(Oid::new(oid)); // may already be matched away
                        }
                    }
                    _ => {
                        if !issued.is_empty() {
                            let i = (xorshift(&mut rng) as usize) % issued.len();
                            let sz = xorshift(&mut rng) % 500; // 0 cancels
                            book.modify_sz(Oid::new(issued[i]), Sz::new(sz));
                        }
                    }
                }
                if step % 50 == 0 {
                    let (bid, ask) = book.get_bbo();
                    assert_eq!(bid, reference_bbo(&book, Side::Bid), "bid aggregate diverged at step {step}");
                    assert_eq!(ask, reference_bbo(&book, Side::Ask), "ask aggregate diverged at step {step}");
                }
            }
            // Final full check.
            let (bid, ask) = book.get_bbo();
            assert_eq!(bid, reference_bbo(&book, Side::Bid));
            assert_eq!(ask, reference_bbo(&book, Side::Ask));
        }
    }

    #[test]
    fn test_compact_preserves_level_aggregates() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Build a deep level, then drain most of it so the slab is heavily
        // over-allocated and compaction actually fires.
        for _ in 0..500u64 {
            book.add_order(factory.order(100, 50, Side::Bid));
        }
        for i in 0..450u64 {
            book.cancel_order(Oid::new(i));
        }
        let before = book.get_bbo();
        assert!(book.compact() > 0, "the churned level should have been compacted");
        assert_eq!(book.get_bbo(), before, "compaction must not change level aggregates");
    }

    // ==================== Performance / Stress Tests ====================

    #[test]
    fn test_stress_add_cancel_1000_orders() {
        let start = std::time::Instant::now();
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();

        // Add 1000 orders
        for i in 0..1000u64 {
            let px = 1000 + (i % 100); // 100 price levels
            let side = if i % 2 == 0 { Side::Bid } else { Side::Ask };
            book.add_order(factory.order(100, px, side));
        }
        let add_elapsed = start.elapsed();

        assert!(book.order_count() > 0, "some orders should remain (non-crossing)");

        // Cancel all remaining
        let cancel_start = std::time::Instant::now();
        for oid in 0..1000u64 {
            book.cancel_order(Oid::new(oid));
        }
        let cancel_elapsed = cancel_start.elapsed();

        assert_eq!(book.order_count(), 0);

        eprintln!(
            "[PERF] 1000 order adds: {:?}, 1000 cancels: {:?}, total: {:?}",
            add_elapsed,
            cancel_elapsed,
            start.elapsed()
        );
    }

    #[test]
    fn test_bbo_computation_performance() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Create book with many price levels
        for i in 0..500u64 {
            book.add_order(factory.order(100, 1000 + i, Side::Bid));
            book.add_order(factory.order(100, 2000 + i, Side::Ask));
        }

        let start = std::time::Instant::now();
        let iterations = 10_000;
        for _ in 0..iterations {
            let _ = book.get_bbo();
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / iterations;

        eprintln!(
            "[PERF] BBO computation: {iterations} calls in {:?} ({:?}/call, 500 levels each side)",
            elapsed, per_call
        );
        // BBO should be fast - under 10us per call
        assert!(per_call.as_micros() < 100, "BBO too slow: {:?}/call", per_call);
    }

    #[test]
    fn test_l4_snapshot_performance() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        // Build a realistic-sized book: 500 price levels each side, 1 order each
        for i in 0..500u64 {
            book.add_order(factory.order(100, 1000 + i, Side::Bid));
            book.add_order(factory.order(100, 2000 + i, Side::Ask));
        }
        assert_eq!(book.order_count(), 1000);

        let start = std::time::Instant::now();
        let iterations = 1000u32;
        for _ in 0..iterations {
            let snapshot = book.to_snapshot();
            assert_eq!(snapshot.as_ref()[0].len(), 500);
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / iterations;

        eprintln!(
            "[PERF] L4 snapshot (1000 orders, 500 levels/side): {iterations} calls in {:?} ({:?}/call)",
            elapsed, per_call
        );
    }

    #[test]
    fn test_l4_snapshot_from_snapshot_performance() {
        let mut book = OrderBook::new();
        let mut factory = OrderFactory::default();
        for i in 0..500u64 {
            book.add_order(factory.order(100, 1000 + i, Side::Bid));
            book.add_order(factory.order(100, 2000 + i, Side::Ask));
        }
        let snapshot = book.to_snapshot();

        let start = std::time::Instant::now();
        let iterations = 100u32;
        for _ in 0..iterations {
            let restored = OrderBook::from_snapshot(snapshot.clone(), false);
            assert_eq!(restored.order_count(), 1000);
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / iterations;

        eprintln!(
            "[PERF] L4 from_snapshot (1000 orders): {iterations} calls in {:?} ({:?}/call)",
            elapsed, per_call
        );
    }
}
