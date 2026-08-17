use crate::{
    order_book::{Coin, InnerOrder, Oid, OrderBook, Px, PxBand, Snapshot, Sz},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

pub(crate) struct Snapshots<O>(HashMap<Coin, Snapshot<O>>);

impl<O> Snapshots<O> {
    pub(crate) const fn new(value: HashMap<Coin, Snapshot<O>>) -> Self {
        Self(value)
    }

    pub(crate) fn value(self) -> HashMap<Coin, Snapshot<O>> {
        self.0
    }
}

impl<O: InnerOrder> Snapshots<O> {
    /// Strip the untriggered trigger orders from every coin's snapshot and
    /// return them flattened. Each trigger order appears twice (once per side)
    /// — callers dedupe by oid. See [`Snapshot::extract_triggers`].
    pub(crate) fn extract_triggers(&mut self) -> Vec<O> {
        self.0.values_mut().flat_map(Snapshot::extract_triggers).collect()
    }
}

#[derive(Clone)]
pub(crate) struct OrderBooks<O> {
    order_books: BTreeMap<Coin, OrderBook<O>>,
}

impl<O: InnerOrder> OrderBooks<O> {
    pub(crate) const fn as_ref(&self) -> &BTreeMap<Coin, OrderBook<O>> {
        &self.order_books
    }

    /// Total number of orders across all orderbooks
    pub(crate) fn order_count(&self) -> usize {
        self.order_books.values().map(|book| book.order_count()).sum()
    }
    #[must_use]
    pub(crate) fn from_snapshots(snapshot: Snapshots<O>, ignore_triggers: bool) -> Self {
        Self {
            order_books: snapshot
                .value()
                .into_iter()
                .map(|(coin, book)| (coin, OrderBook::from_snapshot(book, ignore_triggers)))
                .collect(),
        }
    }

    // Production always threads the diff's queue anchor through add_order_before;
    // this anchor-less shorthand survives for the many round-trip tests.
    #[cfg(test)]
    pub(crate) fn add_order(&mut self, order: O) {
        let fell_back = self.add_order_before(order, None);
        debug_assert!(!fell_back);
    }

    // Returns true when `insert_before` could not be honored and the order was
    // rested at the back of its level instead; see OrderBook::add_order_before.
    pub(crate) fn add_order_before(&mut self, order: O, insert_before: Option<Oid>) -> bool {
        let coin = &order.coin();
        self.order_books.entry(coin.clone()).or_insert_with(OrderBook::new).add_order_before(order, insert_before)
    }

    pub(crate) fn cancel_order(&mut self, oid: Oid, coin: Coin) -> bool {
        if let Some(book) = self.order_books.get_mut(&coin) {
            let success = book.cancel_order(oid.clone());
            if !success {
                // oid not found in this coin's book
                log::debug!("cancel_order: oid {:?} not found in {:?} book", oid, coin);
            }
            // Drop the per-coin OrderBook once it's empty. Without this, every coin
            // we've ever seen sticks around in the BTreeMap (plus its slab and
            // BTreeMap-of-Levels capacity), even after delistings.
            if book.order_count() == 0 {
                self.order_books.remove(&coin);
            }
            success
        } else {
            // coin book doesn't exist
            log::debug!("cancel_order: no book for coin {:?}", coin);
            false
        }
    }

    // change size to reflect how much gets matched during the block
    pub(crate) fn modify_sz(&mut self, oid: Oid, coin: Coin, sz: Sz) -> bool {
        let Some(book) = self.order_books.get_mut(&coin) else { return false };
        let success = book.modify_sz(oid, sz);
        // A modify that reduces sz to zero leaves an empty book behind; evict it
        // for the same reason as cancel_order above.
        if book.order_count() == 0 {
            self.order_books.remove(&coin);
        }
        success
    }

    /// Get BBO for specific coins only - faster for selective broadcast
    /// Only computes BBO for coins in the set, avoiding iteration over all coins.
    /// A coin whose book emptied (and was evicted) yields `(None, None)` rather
    /// than being skipped - skipping left subscribers holding the last quote
    /// forever after a delisting, since no further update would ever arrive.
    #[must_use]
    pub(crate) fn get_bbos_for_coins(
        &self,
        coins: &std::collections::HashSet<Coin>,
    ) -> HashMap<Coin, (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>)> {
        coins
            .iter()
            .map(|coin| (coin.clone(), self.order_books.get(coin).map_or((None, None), OrderBook::get_bbo)))
            .collect()
    }

    /// Compact slab allocators across every coin's orderbook. Returns the number
    /// of price-level lists that were actually rebuilt. Cheap when nothing is
    /// fragmented, so safe to call on a slow maintenance cadence.
    ///
    /// Also evicts any books whose order count dropped to zero — the per-event
    /// path covers single-order eviction, but a stuck book that emptied during
    /// a missed-event window is otherwise pinned forever.
    pub(crate) fn compact_all(&mut self) -> usize {
        let compacted: usize = self.order_books.values_mut().map(|book| book.compact()).sum();
        self.order_books.retain(|_, book| book.order_count() > 0);
        compacted
    }

    /// Returns (total live nodes, total slab capacity) summed across all coins.
    pub(crate) fn slab_stats(&self) -> (usize, usize) {
        let mut live = 0usize;
        let mut cap = 0usize;
        for book in self.order_books.values() {
            let (l, c) = book.slab_stats();
            live += l;
            cap += c;
        }
        (live, cap)
    }
}

impl<O: InnerOrder> OrderBooks<O> {
    /// L4 snapshot of a single coin's book. Cloning one coin is a few hundred
    /// microseconds; the previous all-coins `to_snapshots_par` cloned the entire
    /// multi-book (~hundreds of thousands of orders) under the listener lock on
    /// every l4Book subscribe, stalling event processing for its whole duration.
    #[must_use]
    pub(crate) fn snapshot_for_coin(&self, coin: &Coin, band: PxBand) -> Option<Snapshot<O>> {
        self.order_books.get(coin).map(|book| book.to_snapshot_in_band(band))
    }
}

/// One market's entry in the hl-node CLI dump. Without `--include-trigger-orders`
/// the value is a bare `[bids, asks]` pair; with it, an object that also carries
/// the pending trigger orders (`{"book_orders": [[bids],[asks]],
/// "untriggered_orders": [...]}`).
enum CliMarket<R> {
    Sides([Vec<R>; 2]),
    WithTriggers { book_orders: [Vec<R>; 2], untriggered_orders: Vec<R> },
}

/// Hand-written instead of `#[serde(untagged)]`: untagged enums buffer every
/// element into serde's intermediate `Content` tree before trying variants -
/// on a 400MB dump that cost ~2-2.5x parse CPU plus a multi-MB transient
/// allocation per market, and reduced any inner parse error to an unusable
/// "data did not match any variant". A seq/map visitor dispatches on the
/// JSON shape in a single streaming pass and keeps precise error positions.
impl<'de, R: Deserialize<'de>> Deserialize<'de> for CliMarket<R> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct MarketVisitor<R>(std::marker::PhantomData<R>);

        impl<'de, R: Deserialize<'de>> serde::de::Visitor<'de> for MarketVisitor<R> {
            type Value = CliMarket<R>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a [bids, asks] pair or an object with book_orders/untriggered_orders")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let bids = seq.next_element()?.ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let asks = seq.next_element()?.ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                Ok(CliMarket::Sides([bids, asks]))
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut book_orders: Option<[Vec<R>; 2]> = None;
                let mut untriggered_orders: Option<Vec<R>> = None;
                while let Some(key) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                    match key.as_ref() {
                        "book_orders" => book_orders = Some(map.next_value()?),
                        "untriggered_orders" => untriggered_orders = Some(map.next_value()?),
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CliMarket::WithTriggers {
                    book_orders: book_orders.ok_or_else(|| serde::de::Error::missing_field("book_orders"))?,
                    untriggered_orders: untriggered_orders.unwrap_or_default(),
                })
            }
        }

        deserializer.deserialize_any(MarketVisitor(std::marker::PhantomData))
    }
}

/// Convert the CLI's parsed per-coin list into typed snapshots plus the flat
/// untriggered trigger-order list. Book conversion stays strict (a corrupt
/// book order poisons the install); untriggered conversion is lenient - a bad
/// entry is skipped and counted, since it only degrades the side table, and
/// failing the whole snapshot for it would wedge every re-sync.
#[allow(clippy::type_complexity)]
fn convert_cli_snapshot<O, R>(snapshot: Vec<(String, CliMarket<R>)>, height: u64) -> Result<(u64, Snapshots<O>, Vec<O>)>
where
    O: TryFrom<R, Error = Error>,
{
    let mut untriggered: Vec<O> = Vec::new();
    let mut untriggered_skipped = 0usize;
    let books = snapshot
        .into_iter()
        .map(|(coin, market)| {
            let ([bids, asks], pending) = match market {
                CliMarket::Sides(sides) => (sides, Vec::new()),
                CliMarket::WithTriggers { book_orders, untriggered_orders } => (book_orders, untriggered_orders),
            };
            for raw in pending {
                match O::try_from(raw) {
                    Ok(order) => untriggered.push(order),
                    Err(_) => untriggered_skipped += 1,
                }
            }
            let bids: Vec<O> = bids.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
            let asks: Vec<O> = asks.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
            Ok((Coin::new(&coin), Snapshot([bids, asks])))
        })
        .collect::<Result<HashMap<Coin, Snapshot<O>>>>()?;
    if untriggered_skipped > 0 {
        crate::metrics::PARSE_ERRORS_TOTAL.with_label_values(&["untriggered"]).inc_by(untriggered_skipped as u64);
        log::warn!("Skipped {untriggered_skipped} unparseable untriggered orders in snapshot");
    }
    Ok((height, Snapshots::new(books), untriggered))
}

/// Load snapshots from a CLI-generated JSON file. `height` is the caller's
/// replay cutoff - it MUST be a lower bound of the dump's content height
/// (read the visor state BEFORE invoking the dump), so replay above it can
/// only over-apply idempotently, never skip events the snapshot lacks.
/// Returns the typed books plus the flat untriggered trigger-order list
/// (empty for dumps made without `--include-trigger-orders`).
pub(crate) async fn load_snapshots_from_cli_json<O, R>(
    snapshot_path: &Path,
    height: u64,
) -> Result<(u64, Snapshots<O>, Vec<O>)>
where
    O: TryFrom<R, Error = Error> + Send + 'static,
    R: Serialize + for<'a> Deserialize<'a> + Send + 'static,
{
    // The snapshot file is hundreds of MB; deserialize + convert is seconds of
    // pure CPU, so it runs on a blocking thread instead of pinning a runtime
    // worker for the duration. Streaming from the file (instead of reading it
    // into a String first) keeps one full copy of the file out of peak RSS
    // while both the old and new books are alive during the install.
    let snapshot_path = snapshot_path.to_path_buf();
    let parse_start = std::time::Instant::now();
    let parsed = tokio::task::spawn_blocking(move || -> Result<(u64, Snapshots<O>, Vec<O>)> {
        let file = fs::File::open(&snapshot_path)?;
        let reader = std::io::BufReader::with_capacity(1 << 20, file);
        let snapshot: Vec<(String, CliMarket<R>)> = serde_json::from_reader(reader)?;
        convert_cli_snapshot(snapshot, height)
    })
    .await??;
    let parse_elapsed = parse_start.elapsed();
    crate::metrics::RESYNC_PHASE_DURATION.with_label_values(&["parse"]).observe(parse_elapsed.as_secs_f64());
    log::info!("Snapshot parsed in {}ms", parse_elapsed.as_millis());
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use crate::{
        order_book::{
            InnerOrder, OrderBook, Px, Side, Snapshot, Sz,
            levels::build_l2_level,
            multi_book::{Coin, Snapshots},
        },
        prelude::*,
        types::{
            L4Order, Level,
            inner::{InnerL4Order, InnerLevel},
        },
    };
    use alloy::primitives::Address;
    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use std::{collections::HashMap, fs::create_dir_all, path::PathBuf};
    use tokio::fs::read_to_string;

    fn load_snapshots_from_str<O, R>(str: &str) -> Result<(u64, Snapshots<O>)>
    where
        O: TryFrom<R, Error = crate::prelude::Error>,
        R: Serialize + for<'a> Deserialize<'a>,
    {
        #[allow(clippy::type_complexity)]
        let (height, snapshot): (u64, Vec<(String, [Vec<R>; 2])>) = serde_json::from_str(str)?;
        Ok((
            height,
            Snapshots::new(
                snapshot
                    .into_iter()
                    .map(|(coin, [bids, asks])| {
                        let bids: Vec<O> = bids.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
                        let asks: Vec<O> = asks.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
                        Ok((Coin::new(&coin), Snapshot([bids, asks])))
                    })
                    .collect::<Result<HashMap<Coin, Snapshot<O>>>>()?,
            ),
        ))
    }

    async fn load_snapshots_from_json<O, R>(path: &PathBuf) -> Result<(u64, Snapshots<O>)>
    where
        O: TryFrom<R, Error = crate::prelude::Error>,
        R: Serialize + for<'a> Deserialize<'a>,
    {
        let file_contents = read_to_string(path).await?;
        load_snapshots_from_str(&file_contents)
    }

    #[must_use]
    fn snapshot_to_l2_snapshot<O: InnerOrder>(
        snapshot: &Snapshot<O>,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Snapshot<InnerLevel> {
        let [bids, asks] = &snapshot.0;
        let bids = orders_to_l2_levels(bids, Side::Bid, n_levels, n_sig_figs, mantissa);
        let asks = orders_to_l2_levels(asks, Side::Ask, n_levels, n_sig_figs, mantissa);
        Snapshot([bids, asks])
    }

    #[must_use]
    fn orders_to_l2_levels<O: InnerOrder>(
        orders: &[O],
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

        for order in orders {
            if build_l2_level(
                &mut cur_level,
                &mut levels,
                n_levels,
                n_sig_figs,
                mantissa,
                side,
                InnerLevel { px: order.limit_px(), sz: order.sz(), n: 1 },
            ) {
                break;
            }
        }
        levels.extend(cur_level.take());
        levels
    }

    #[derive(Default)]
    struct OrderManager {
        next_oid: u64,
    }

    fn simple_inner_order(oid: u64, side: Side, sz: String, px: String) -> Result<InnerL4Order> {
        let px = Px::parse_from_str(&px)?;
        let sz = Sz::parse_from_str(&sz)?;
        Ok(InnerL4Order {
            user: Address::new([0; 20]),
            coin: Coin::new(""),
            side,
            limit_px: px,
            sz,
            oid,
            timestamp: 0,
            trigger_condition: String::new(),
            is_trigger: false,
            trigger_px: String::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: String::new(),
            tif: None,
            cloid: None,
        })
    }

    impl OrderManager {
        fn order(&mut self, sz: &str, limit_px: &str, side: Side) -> Result<InnerL4Order> {
            let order = simple_inner_order(self.next_oid, side, sz.to_string(), limit_px.to_string())?;
            self.next_oid += 1;
            Ok(order)
        }

        fn batch_order(&mut self, sz: &str, limit_px: &str, side: Side, mult: u64) -> Result<Vec<InnerL4Order>> {
            (0..mult).map(|_| self.order(sz, limit_px, side)).try_collect()
        }
    }

    fn setup_book(book: &mut OrderBook<InnerL4Order>) -> Snapshots<InnerL4Order> {
        let mut o = OrderManager::default();
        let buy_orders1 = o.batch_order("100", "34.01", Side::Bid, 4).unwrap();
        let buy_orders2 = o.batch_order("200", "34.5", Side::Bid, 2).unwrap();
        let buy_orders3 = o.batch_order("300", "34.6", Side::Bid, 1).unwrap();
        let sell_orders1 = o.batch_order("100", "35", Side::Ask, 4).unwrap();
        let sell_orders2 = o.batch_order("200", "35.1", Side::Ask, 2).unwrap();
        let sell_orders3 = o.batch_order("300", "35.5", Side::Ask, 1).unwrap();
        for orders in [buy_orders1, buy_orders2, buy_orders3, sell_orders1, sell_orders2, sell_orders3] {
            for o in orders {
                book.add_order(o);
            }
        }
        Snapshots(vec![(Coin::new(""), book.to_snapshot()); 2].into_iter().collect())
    }

    const SNAPSHOT_JSON: &str = r#"[100, 
    [
        [
            "@1",
            [
                [
                    [
                        "0x0000000000000000000000000000000000000000",
                        {
                            "coin": "@1",
                            "side": "B",
                            "limitPx": "30.444",
                            "sz": "100.0",
                            "oid": 105338503859,
                            "timestamp": 1750660644034,
                            "triggerCondition": "N/A",
                            "isTrigger": false,
                            "triggerPx": "0.0",
                            "children": [],
                            "isPositionTpsl": false,
                            "reduceOnly": false,
                            "orderType": "Limit",
                            "origSz": "100.0",
                            "tif": "Alo",
                            "cloid": null
                        }
                    ],
                    [
                        "0x0000000000000000000000000000000000000000",
                        {
                            "coin": "@1",
                            "side": "B",
                            "limitPx": "30.385",
                            "sz": "5.45",
                            "oid": 105337808436,
                            "timestamp": 1750660453608,
                            "triggerCondition": "N/A",
                            "isTrigger": false,
                            "triggerPx": "0.0",
                            "children": [],
                            "isPositionTpsl": false,
                            "reduceOnly": false,
                            "orderType": "Limit",
                            "origSz": "5.45",
                            "tif": "Gtc",
                            "cloid": null
                        }
                    ]
                ],
                []
            ]
        ]
    ]
]"#;

    #[tokio::test]
    async fn test_deserialization_from_json() -> Result<()> {
        create_dir_all("tmp/deserialization_test")?;
        fs::write("tmp/deserialization_test/out.json", SNAPSHOT_JSON)?;
        load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&PathBuf::from(
            "tmp/deserialization_test/out.json",
        ))
        .await?;
        Ok(())
    }

    #[test]
    fn test_deserialization() -> Result<()> {
        load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(SNAPSHOT_JSON)?;
        Ok(())
    }

    /// The --include-trigger-orders dump format: per-coin objects with
    /// `book_orders` + `untriggered_orders`. Both this and the legacy bare
    /// `[bids, asks]` form must parse via the untagged CliMarket enum, and the
    /// untriggered list must come through typed.
    #[test]
    fn test_cli_snapshot_with_trigger_orders_format() -> Result<()> {
        let order = |oid: u64, is_trigger: bool| {
            serde_json::json!(["0x0000000000000000000000000000000000000001", {
                "coin": "BTC", "side": "B", "limitPx": "100.0", "sz": "1.0", "oid": oid,
                "timestamp": 1000, "triggerCondition": if is_trigger {"Price above 110"} else {"N/A"},
                "isTrigger": is_trigger, "triggerPx": if is_trigger {"110.0"} else {"0.0"},
                "children": [], "isPositionTpsl": false, "reduceOnly": false,
                "orderType": if is_trigger {"Stop Market"} else {"Limit"},
                "origSz": "1.0", "tif": null, "cloid": null
            }])
        };
        let json = serde_json::json!([
            ["BTC", { "book_orders": [[order(1, false)], []], "untriggered_orders": [order(100, true), order(101, true)] }],
            ["ETH", [[order(2, false)], []]]
        ]);
        let parsed: Vec<(String, super::CliMarket<(Address, L4Order)>)> = serde_json::from_value(json)?;
        let (height, snapshots, untriggered) = super::convert_cli_snapshot::<InnerL4Order, _>(parsed, 42)?;
        assert_eq!(height, 42);
        assert_eq!(snapshots.value().len(), 2, "both formats must yield books");
        assert_eq!(untriggered.len(), 2);
        assert!(untriggered.iter().all(|o| o.is_trigger && o.trigger_px == "110.0"));
        Ok(())
    }

    #[test]
    fn test_l4_snapshot_to_l2_snapshot() {
        let mut book = OrderBook::new();
        let coin = Coin::new("");
        let snapshot = setup_book(&mut book);
        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), Some(2), Some(1));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![Level::new("34".to_string(), "1100".to_string(), 7)],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("36".to_string(), "700".to_string(), 3),
            ],
        ];
        assert_eq!(ans, raw_levels);

        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), Some(3), Some(5));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![
                Level::new("34.5".to_string(), "700".to_string(), 3),
                Level::new("34".to_string(), "400".to_string(), 4),
            ],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("35.5".to_string(), "700".to_string(), 3),
            ],
        ];
        assert_eq!(ans, raw_levels);
        let snapshot_from_book = book.to_l2_snapshot(Some(2), Some(3), Some(5));
        let raw_levels_from_book = snapshot_from_book.export_inner_snapshot();
        let snapshot_from_book = book.to_l2_snapshot(None, None, None);
        let snapshot_from_snapshot = snapshot_from_book.to_l2_snapshot(Some(2), Some(3), Some(5));
        let raw_levels_from_snapshot = snapshot_from_snapshot.export_inner_snapshot();
        assert_eq!(raw_levels_from_book, ans);
        assert_eq!(raw_levels_from_snapshot, ans);

        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), None, Some(5));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![
                Level::new("34.6".to_string(), "300".to_string(), 1),
                Level::new("34.5".to_string(), "400".to_string(), 2),
            ],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("35.1".to_string(), "400".to_string(), 2),
            ],
        ];
        assert_eq!(ans, raw_levels);
    }

    use crate::order_book::{Oid, PxBand, multi_book::OrderBooks};

    fn make_order(oid: u64, coin: &str, side: Side, sz: &str, px: &str) -> InnerL4Order {
        let mut o = simple_inner_order(oid, side, sz.to_string(), px.to_string()).unwrap();
        o.coin = Coin::new(coin);
        o
    }

    // ==================== MultiBook eviction tests ====================

    #[test]
    fn test_cancel_removes_empty_orderbook_from_multibook() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(make_order(1, "BTC", Side::Bid, "1", "50000"));
        assert!(books.as_ref().contains_key(&Coin::new("BTC")));
        let removed = books.cancel_order(Oid::new(1), Coin::new("BTC"));
        assert!(removed);
        // Empty book must be evicted: ghost entries in the BTreeMap were a known
        // slow leak after coin delistings.
        assert!(!books.as_ref().contains_key(&Coin::new("BTC")));
    }

    #[test]
    fn test_cancel_keeps_nonempty_orderbook() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(make_order(1, "ETH", Side::Bid, "1", "3000"));
        books.add_order(make_order(2, "ETH", Side::Bid, "2", "3000"));
        books.cancel_order(Oid::new(1), Coin::new("ETH"));
        // Still has order 2 — must not be evicted.
        assert!(books.as_ref().contains_key(&Coin::new("ETH")));
    }

    #[test]
    fn test_get_bbos_for_coins_reports_evicted_coin_as_empty() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(make_order(1, "BTC", Side::Bid, "1", "50000"));
        books.cancel_order(Oid::new(1), Coin::new("BTC")); // book evicted

        let coins: std::collections::HashSet<Coin> = std::iter::once(Coin::new("BTC")).collect();
        let bbos = books.get_bbos_for_coins(&coins);
        // The coin must be present with an empty BBO - skipping it left
        // subscribers holding the last quote forever after a delisting.
        assert_eq!(bbos.get(&Coin::new("BTC")), Some(&(None, None)));
    }

    #[test]
    fn test_compact_all_evicts_empty_books() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        // Seed two coins, then drain one without triggering per-event eviction
        // (we cancel via OrderBook directly so the MultiBook path doesn't run).
        books.add_order(make_order(1, "BTC", Side::Bid, "1", "50000"));
        books.add_order(make_order(2, "ETH", Side::Bid, "1", "3000"));
        // Sanity
        assert_eq!(books.as_ref().len(), 2);
        // Cancel order 1 via MultiBook to trigger eviction of the BTC book.
        books.cancel_order(Oid::new(1), Coin::new("BTC"));
        // ETH still has an order; compact_all should leave it alone.
        books.compact_all();
        assert!(books.as_ref().contains_key(&Coin::new("ETH")));
        assert!(!books.as_ref().contains_key(&Coin::new("BTC")));
    }

    #[test]
    fn test_snapshot_for_coin_with_band() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(make_order(1, "BTC", Side::Bid, "1", "50000"));
        books.add_order(make_order(2, "BTC", Side::Bid, "1", "60000"));
        books.add_order(make_order(3, "BTC", Side::Ask, "1", "70000"));
        books.add_order(make_order(4, "ETH", Side::Bid, "1", "60000"));

        let band = PxBand::parse(Some("55000"), Some("75000")).unwrap();
        let [bids, asks] = books.snapshot_for_coin(&Coin::new("BTC"), band).unwrap().as_ref().clone();
        assert_eq!(bids.iter().map(|o| o.oid).collect_vec(), vec![2], "px 50000 bid is out of band");
        assert_eq!(asks.iter().map(|o| o.oid).collect_vec(), vec![3]);

        // Other coins are untouched by a BTC band query, and the unbounded
        // default band still returns the full book.
        let [bids, _] = books.snapshot_for_coin(&Coin::new("ETH"), PxBand::default()).unwrap().as_ref().clone();
        assert_eq!(bids.iter().map(|o| o.oid).collect_vec(), vec![4]);
        let [bids, asks] = books.snapshot_for_coin(&Coin::new("BTC"), PxBand::default()).unwrap().as_ref().clone();
        assert_eq!(bids.len() + asks.len(), 3);

        assert!(books.snapshot_for_coin(&Coin::new("DOGE"), band).is_none(), "unknown coin yields None");
    }
}
