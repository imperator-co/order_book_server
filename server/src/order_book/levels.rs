use crate::order_book::{InnerOrder, OrderBook, Px, Side, Snapshot, price_level::PriceLevel};
use crate::types::Level;
use crate::types::inner::InnerLevel;
use std::collections::BTreeMap;

#[must_use]
fn bucket(px: Px, side: Side, n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Px {
    let m = mantissa.unwrap_or(1);
    n_sig_figs.map_or(px, |n| {
        let digs = px.num_digits();
        let p = digs.saturating_sub(n);
        let inc = m * 10u64.pow(p);
        match side {
            Side::Ask => Px::new(px.value().div_ceil(inc) * inc),
            Side::Bid => Px::new((px.value() / inc) * inc),
        }
    })
}

impl<O: InnerOrder> OrderBook<O> {
    #[must_use]
    pub(crate) fn to_l2_snapshot(
        &self,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Snapshot<InnerLevel> {
        let bids = &self.bids;
        let asks = &self.asks;
        let bids = map_to_l2_levels(bids, Side::Bid, n_levels, n_sig_figs, mantissa);
        let asks = map_to_l2_levels(asks, Side::Ask, n_levels, n_sig_figs, mantissa);
        Snapshot([bids, asks])
    }
}

impl Snapshot<InnerLevel> {
    /// Re-aggregate an existing L2 snapshot. Only valid on an UNTRUNCATED
    /// input: re-aggregating a level-capped snapshot collapses coarse
    /// groupings to the few buckets its near-mid levels span (the l2Book
    /// coarse-grouping bug). The live path aggregates straight from the full
    /// book (`OrderBook::to_l2_snapshot`); this stays test-only to prove the
    /// two are equivalent on full-information input.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn to_l2_snapshot(
        &self,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Self {
        let [bids, asks] = &self.0;
        let bids = l2_levels_to_l2_levels(bids, Side::Bid, n_levels, n_sig_figs, mantissa);
        let asks = l2_levels_to_l2_levels(asks, Side::Ask, n_levels, n_sig_figs, mantissa);
        Self([bids, asks])
    }

    pub(crate) fn export_inner_snapshot(self) -> [Vec<Level>; 2] {
        self.0.map(|b| b.into_iter().map(Level::from).collect())
    }
}

#[cfg(test)]
#[must_use]
fn l2_levels_to_l2_levels(
    levels: &[InnerLevel],
    side: Side,
    n_levels: Option<usize>,
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
) -> Vec<InnerLevel> {
    let mut new_levels = Vec::new();
    if n_levels == Some(0) {
        return new_levels;
    }
    let mut cur_level: Option<InnerLevel> = None;
    for level in levels {
        if build_l2_level(&mut cur_level, &mut new_levels, n_levels, n_sig_figs, mantissa, side, level.clone()) {
            break;
        }
    }
    new_levels.extend(cur_level.take());
    new_levels
}

#[must_use]
fn map_to_l2_levels<O: InnerOrder>(
    orders: &BTreeMap<Px, PriceLevel<O>>,
    side: Side,
    n_levels: Option<usize>,
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
) -> Vec<InnerLevel> {
    let mut levels = Vec::new();
    if n_levels == Some(0) {
        return levels;
    }
    let mut cur_level: Option<InnerLevel> = None;
    let order_iter: Box<dyn Iterator<Item = (&Px, &PriceLevel<O>)>> = match side {
        Side::Ask => Box::new(orders.iter()),
        Side::Bid => Box::new(orders.iter().rev()),
    };
    for (px, level) in order_iter {
        // O(1) per level: PriceLevel maintains (total size, count) incrementally,
        // so the base L2 build is O(levels) instead of O(orders).
        if build_l2_level(
            &mut cur_level,
            &mut levels,
            n_levels,
            n_sig_figs,
            mantissa,
            side,
            InnerLevel { px: *px, sz: level.total_sz(), n: level.len() },
        ) {
            break;
        }
    }
    levels.extend(cur_level.take());
    levels
}

pub(super) fn build_l2_level(
    cur_level: &mut Option<InnerLevel>,
    levels: &mut Vec<InnerLevel>,
    n_levels: Option<usize>,
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
    side: Side,
    level: InnerLevel,
) -> bool {
    let new_bucket = cur_level.as_ref().is_none_or(|c| match side {
        Side::Ask => level.px.value() > c.px.value(),
        Side::Bid => level.px.value() < c.px.value(),
    });
    if new_bucket {
        let bucket = bucket(level.px, side, n_sig_figs, mantissa);
        levels.extend(cur_level.take());
        if n_levels == Some(levels.len()) {
            return true;
        }
        *cur_level = Some(InnerLevel { px: bucket, sz: level.sz, n: level.n });
    } else if let Some(c) = cur_level.as_mut() {
        c.sz = level.sz + c.sz;
        c.n += level.n;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::{OrderBook, Sz, types::InnerOrder};

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct TestOrder {
        oid: u64,
        side: Side,
        sz: u64,
        limit_px: u64,
    }

    impl InnerOrder for TestOrder {
        fn oid(&self) -> crate::order_book::Oid { crate::order_book::Oid::new(self.oid) }
        fn side(&self) -> Side { self.side }
        fn limit_px(&self) -> Px { Px::new(self.limit_px) }
        fn sz(&self) -> Sz { Sz::new(self.sz) }
        fn decrement_sz(&mut self, dec: Sz) { self.sz = self.sz.saturating_sub(dec.value()); }
        fn fill(&mut self, maker: &mut Self) -> Sz {
            let m = self.sz().min(maker.sz());
            self.decrement_sz(m);
            maker.decrement_sz(m);
            m
        }
        fn modify_sz(&mut self, sz: Sz) { self.sz = sz.value(); }
        fn convert_trigger(&mut self, _: u64) {}
        fn coin(&self) -> crate::order_book::Coin { crate::order_book::Coin::new("") }
    }

    fn make_book(bids: &[(u64, u64, u64)], asks: &[(u64, u64, u64)]) -> OrderBook<TestOrder> {
        let mut book = OrderBook::new();
        let mut oid = 0u64;
        for &(px, sz, count) in bids {
            for _ in 0..count {
                book.add_order(TestOrder { oid, side: Side::Bid, sz, limit_px: px });
                oid += 1;
            }
        }
        for &(px, sz, count) in asks {
            for _ in 0..count {
                book.add_order(TestOrder { oid, side: Side::Ask, sz, limit_px: px });
                oid += 1;
            }
        }
        book
    }

    fn to_levels(snapshot: Snapshot<InnerLevel>) -> [Vec<(u64, u64, usize)>; 2] {
        snapshot.0.map(|levels| levels.into_iter().map(|l| (l.px.value(), l.sz.value(), l.n)).collect())
    }

    #[test]
    fn test_l2_no_aggregation() {
        let book = make_book(
            &[(500, 100, 2), (400, 200, 1)],
            &[(600, 150, 1), (700, 300, 1)],
        );
        let snapshot = book.to_l2_snapshot(None, None, None);
        let [bids, asks] = to_levels(snapshot);
        assert_eq!(bids, vec![(500, 200, 2), (400, 200, 1)]);
        assert_eq!(asks, vec![(600, 150, 1), (700, 300, 1)]);
    }

    #[test]
    fn test_l2_n_levels_truncation() {
        let book = make_book(
            &[(500, 100, 1), (400, 100, 1), (300, 100, 1)],
            &[(600, 100, 1), (700, 100, 1), (800, 100, 1)],
        );
        let snapshot = book.to_l2_snapshot(Some(2), None, None);
        let [bids, asks] = to_levels(snapshot);
        assert_eq!(bids.len(), 2);
        assert_eq!(asks.len(), 2);
    }

    #[test]
    fn test_l2_zero_levels() {
        let book = make_book(&[(500, 100, 1)], &[(600, 100, 1)]);
        let snapshot = book.to_l2_snapshot(Some(0), None, None);
        let [bids, asks] = to_levels(snapshot);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
    }

    #[test]
    fn test_l2_sig_figs_aggregation() {
        let book = make_book(
            &[
                (340_100_000_000, 100, 1), // 3401.0
                (340_500_000_000, 100, 1), // 3405.0
            ],
            &[
                (341_000_000_000, 100, 1), // 3410.0
                (342_000_000_000, 100, 1), // 3420.0
            ],
        );
        let snapshot = book.to_l2_snapshot(None, Some(2), None);
        let [bids, asks] = to_levels(snapshot);
        // With 2 sig figs, bids at 3401 and 3405 both bucket to 3400
        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].1, 200);
        assert_eq!(bids[0].2, 2);
    }

    #[test]
    fn test_bucket_ask_rounds_up() {
        let px = Px::new(345_000_000_000);
        let bucketed = bucket(px, Side::Ask, Some(2), None);
        assert_eq!(bucketed.value(), 350_000_000_000);
    }

    #[test]
    fn test_bucket_bid_rounds_down() {
        let px = Px::new(345_000_000_000);
        let bucketed = bucket(px, Side::Bid, Some(2), None);
        assert_eq!(bucketed.value(), 340_000_000_000);
    }

    #[test]
    fn test_bucket_no_sig_figs_passthrough() {
        let px = Px::new(12345);
        assert_eq!(bucket(px, Side::Ask, None, None).value(), 12345);
    }

    #[test]
    fn test_l2_from_l2_matches_l2_from_l4() {
        let book = make_book(
            &[(500, 100, 3), (490, 200, 2), (480, 50, 1)],
            &[(510, 100, 2), (520, 200, 1), (530, 50, 1)],
        );
        let raw_l2 = book.to_l2_snapshot(None, None, None);
        let from_l4 = book.to_l2_snapshot(Some(2), Some(2), None);
        let from_l2 = raw_l2.to_l2_snapshot(Some(2), Some(2), None);
        assert_eq!(to_levels(from_l4), to_levels(from_l2));
    }

    #[test]
    fn test_export_inner_snapshot_converts_to_level() {
        let book = make_book(&[(500, 100, 1)], &[(600, 200, 1)]);
        let snapshot = book.to_l2_snapshot(None, None, None);
        let exported = snapshot.export_inner_snapshot();
        assert_eq!(exported[0].len(), 1);
        assert_eq!(exported[1].len(), 1);
        assert_eq!(exported[0][0].px(), Px::new(500).to_str());
        assert_eq!(exported[1][0].sz(), Sz::new(200).to_str());
    }

    #[test]
    fn test_l2_snapshot_performance() {
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for i in 0..500u64 {
            bids.push((1000 + i, 100, 1));
            asks.push((2000 + i, 100, 1));
        }
        let book = make_book(&bids, &asks);

        let start = std::time::Instant::now();
        let iterations = 1000u32;
        for _ in 0..iterations {
            let _ = book.to_l2_snapshot(Some(20), Some(3), None);
        }
        let elapsed = start.elapsed();
        let per_call = elapsed / iterations;

        eprintln!(
            "[PERF] L2 snapshot (500 levels, 20 output, sig_figs=3): {iterations} calls in {:?} ({:?}/call)",
            elapsed, per_call
        );
    }
}
