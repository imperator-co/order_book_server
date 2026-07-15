use crate::{
    listeners::order_book::state::OrderBookState,
    metrics::{
        BBO_BROADCAST_LATENCY, EVENT_PROCESSING_LATENCY, EVENTS_PROCESSED_TOTAL, FILE_EVENTS_TOTAL,
        FILE_LINES_PARSED_TOTAL, L2_BROADCAST_LATENCY, L2_CONFLATION_BATCH_SIZE, ORDERBOOK_COINS_COUNT,
        ORDERBOOK_DESYNCS_TOTAL, ORDERBOOK_HEIGHT, ORDERBOOK_ORDERS_TOTAL, ORDERBOOK_TIME_MS, PARSE_ERRORS_TOTAL,
        PENDING_DIFFS_CACHE, PENDING_ORDERS_CACHE, TRADES_UNPAIRED_FILLS_TOTAL,
    },
    order_book::{
        Coin, Px, Side, Snapshot, Sz,
        multi_book::{Snapshots, load_snapshots_from_cli_json},
    },
    prelude::*,
    types::{
        L2Book, L4Order, Trade,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use log::{error, info};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::{Instant, MissedTickBehavior, interval},
};
use utils::{EventBatch, SnapshotConfig, get_visor_path, process_rmp_file, read_visor_height};

/// Minimum interval between L2 broadcasts. Caps the broadcast rate at 20/sec; the
/// conflation buffer accumulates dirty coins between broadcasts.
const L2_BROADCAST_THROTTLE_MS: u64 = 50;
/// How often the main loop polls to flush the conflation buffer. Must be << the
/// throttle so a quiet node between block flushes can never starve the L2 feed
/// for more than ~throttle + tick.
const L2_FLUSH_TICK_MS: u64 = 10;
/// Cap on events cached for replay while a snapshot fetch is in flight. A fetch
/// normally completes in 10-30s (tens of thousands of events); hitting this cap
/// means something is pathologically wrong, so we drop the cache and schedule
/// another re-sync rather than risk OOM.
const MAX_CACHED_EVENTS: usize = 1_000_000;

mod parallel;
mod state;
mod utils;

fn fetch_snapshot(
    snapshot_config: SnapshotConfig,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    _ignore_spot: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        // CRITICAL: Start caching BEFORE generating the snapshot. Every
        // book-affecting batch that arrives while hl-node dumps state is cached,
        // and init_from_snapshot replays the ones above the snapshot height -
        // so the handoff from snapshot to live stream is gapless. (We don't clone
        // the existing state here: it's discarded by init_from_snapshot below,
        // and cloning the whole BTreeMap/Slab tree temporarily doubles peak RSS.)
        {
            let mut listener = listener.lock().await;
            listener.begin_caching();
        }

        // Now generate snapshot - any events during this time are cached
        let visor_path = get_visor_path(&snapshot_config);
        let res = match process_rmp_file(&snapshot_config).await {
            Ok(output_fln) => {
                let snapshot =
                    load_snapshots_from_cli_json::<InnerL4Order, (Address, L4Order)>(&output_fln, &visor_path).await;
                info!("Snapshot fetched");
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        info!("Snapshot loaded at height {}", height);
                        // Reinitialize from the snapshot and replay the cached
                        // events above its height in one lock acquisition, so no
                        // event can slip between the replay and going live.
                        listener.lock().await.init_from_snapshot(expected_snapshot, height);
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        };
        let _unused = tx.send(res);
        Ok::<(), Error>(())
    });
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    // (include_perps, include_spot, include_hip3) - used to filter the universe
    // handed to connections for subscription validation.
    market_filter: (bool, bool, bool),
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    // Some while a snapshot fetch is in flight (initial startup and re-syncs):
    // every book-affecting batch is cached here and replayed above the snapshot
    // height by init_from_snapshot, making the snapshot -> live-stream handoff
    // gapless. None in steady state (no caching cost per event).
    fetched_snapshot_cache: Option<VecDeque<EventBatch>>,
    // Total events across all cached batches; bounded by cache_event_cap.
    cached_event_count: usize,
    cache_event_cap: usize,
    // Set whenever events were provably lost (oversize-batch drop, watcher
    // partial-line discard, pending-cache eviction, apply errors). The main loop
    // reacts by re-fetching a snapshot, which rebuilds the book and clears this.
    needs_resync: bool,
    // When true, mark_desynced still counts the desync metric but does NOT set
    // needs_resync, so the book keeps serving live events through drift instead
    // of re-fetching a snapshot. Operator opt-in (--no-resync) for environments
    // where a continuously-non-converging re-sync loop is worse than a knowingly
    // incomplete book. Drift is NOT self-healed while this is set.
    tolerate_drift: bool,
    // Upper bound on the height of any unrecovered data loss (0 = none).
    // init_from_snapshot may only clear needs_resync when the snapshot height
    // covers this bound - a loss that occurred DURING a fetch can sit above the
    // snapshot's height (the snapshot source lags the stream), and clearing the
    // flag unconditionally would erase the signal and leave permanent drift.
    max_loss_height: u64,
    // Highest block height observed on the live stream; the best "now" proxy
    // for bounding losses whose exact height is unknown (watcher discards).
    last_seen_height: u64,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    // Pairs fill legs into public-schema trades; holds at most the one leg
    // awaiting its counterpart across Fills batches (single-event batches in
    // --stream-with-block-info mode).
    trade_pairer: TradePairer,
    // Throttle L2 broadcasts to prevent flooding clients
    last_l2_broadcast: Option<Instant>,
    // Incremental L2 snapshot cache. Each per-coin entry is Arc'd and shared with
    // the broadcast Arc, so unchanged coins cost an atomic bump rather than a
    // full level-vector clone. Invalidated in `init_from_snapshot`.
    l2_snapshot_cache: HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>,
    // Coin-level conflation buffer for throttled L2 broadcasts. Every event unions
    // its changed_coins here; each L2 broadcast drains the full set so no coin
    // starves during throttle-suppressed windows. Without this, a coin that changed
    // during a suppressed 50ms window was never marked for rebuild and served a
    // stale cached snapshot until a later event for that exact coin happened to land
    // on an open throttle slot. Mutated only under the listener lock (like
    // last_l2_broadcast / l2_snapshot_cache). Bounded by the universe size.
    pending_dirty_l2_coins: HashSet<Coin>,
    // Shared registry of L2 variant shapes any live connection wants. Read at flush
    // time so we compute only subscribed variants per coin instead of all 7.
    active_l2_params: ActiveL2Params,
    // The active variant set used for the last L2 build. When it changes (a new
    // shape is subscribed, or the last subscriber of a shape leaves), the cache holds
    // the wrong shapes, so we clear it to force a full rebuild against the new set -
    // this is what lets a brand-new subscriber be served within one throttle window
    // even on an otherwise-quiet coin.
    last_active_l2_params: HashSet<L2SnapshotParams>,
}

impl OrderBookListener {
    pub(crate) fn new(
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
        ignore_spot: bool,
        active_l2_params: ActiveL2Params,
        market_filter: (bool, bool, bool),
    ) -> Self {
        Self {
            ignore_spot,
            market_filter,
            order_book_state: None,
            // Cache from the very first event: the initial snapshot fetch hasn't
            // started yet, and anything arriving before it completes must be
            // replayable or it is lost (the pre-existing startup drift window).
            fetched_snapshot_cache: Some(VecDeque::new()),
            cached_event_count: 0,
            cache_event_cap: MAX_CACHED_EVENTS,
            needs_resync: false,
            tolerate_drift: false,
            max_loss_height: 0,
            last_seen_height: 0,
            internal_message_tx,
            trade_pairer: TradePairer::default(),
            last_l2_broadcast: None,
            l2_snapshot_cache: HashMap::new(),
            pending_dirty_l2_coins: HashSet::new(),
            active_l2_params,
            last_active_l2_params: HashSet::new(),
        }
    }

    /// Clone of the shared active-variant registry, for handing to connections.
    pub(crate) fn active_l2_params(&self) -> ActiveL2Params {
        self.active_l2_params.clone()
    }

    /// Opt out of snapshot re-fetch on data loss. The book keeps applying live
    /// events through drift; desyncs are still counted in metrics but never
    /// trigger a re-sync. See `tolerate_drift`.
    pub(crate) fn set_tolerate_drift(&mut self, tolerate: bool) {
        self.tolerate_drift = tolerate;
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    /// Coin universe filtered by the configured market types, for subscription
    /// validation. Connections receive refreshed copies via `Snapshot` broadcasts
    /// whenever the coin set changes.
    pub(crate) fn universe(&self) -> Arc<HashSet<String>> {
        let market_filter = self.market_filter;
        Arc::new(self.order_book_state.as_ref().map_or_else(HashSet::new, |state| {
            state
                .compute_universe()
                .into_iter()
                .filter(|coin| coin_in_market_filter(coin, market_filter))
                .map(|coin| coin.value())
                .collect()
        }))
    }

    /// Start caching book-affecting batches for replay. Idempotent: an already
    /// active cache (e.g. the one running since construction) is kept, so events
    /// cached before the snapshot fetch was triggered are not thrown away.
    fn begin_caching(&mut self) {
        if self.fetched_snapshot_cache.is_none() {
            self.fetched_snapshot_cache = Some(VecDeque::new());
            self.cached_event_count = 0;
        }
    }

    /// Record that the in-memory book may have diverged from the node (events
    /// were dropped or discarded somewhere). The main-loop ticker reacts by
    /// re-fetching a full snapshot; the flag is cleared only by an
    /// init_from_snapshot whose height covers the recorded loss bound.
    pub(crate) fn mark_desynced(&mut self, reason: &'static str) {
        /// Slack added to the loss bound: an unknown-height loss (e.g. a
        /// watcher discard) can be slightly ahead of the last parsed height.
        /// Costs at most one extra re-fetch cycle; prevents clearing the flag
        /// on a snapshot that misses the tail of the loss by a few blocks.
        const LOSS_HEIGHT_MARGIN: u64 = 100;

        ORDERBOOK_DESYNCS_TOTAL.with_label_values(&[reason]).inc();
        // Operator opted to ride out drift: count the desync so it stays visible
        // in metrics, but do NOT schedule a re-fetch. The book keeps serving live
        // events and will not self-heal until restarted or the flag is cleared.
        if self.tolerate_drift {
            return;
        }
        if !self.needs_resync {
            error!("Order book marked out-of-sync ({reason}); scheduling snapshot re-fetch");
        }
        self.needs_resync = true;
        let state_height = self.order_book_state.as_ref().map_or(0, OrderBookState::height);
        let observed = state_height.max(self.last_seen_height);
        // No height observed yet (loss before any event parsed): conservative
        // bound - only an informed downgrade in init_from_snapshot can clear it.
        let bound = if observed == 0 { u64::MAX } else { observed.saturating_add(LOSS_HEIGHT_MARGIN) };
        self.max_loss_height = self.max_loss_height.max(bound);
    }

    pub(crate) const fn needs_resync(&self) -> bool {
        self.needs_resync
    }

    /// Shrink the replay-cache cap so overflow behavior is testable without
    /// constructing a million events.
    #[cfg(test)]
    const fn set_cache_event_cap(&mut self, cap: usize) {
        self.cache_event_cap = cap;
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("Initializing from snapshot at height {}", height);
        let mut new_state = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);

        // Replay every cached batch above the snapshot height. Batches at or
        // below the height are already reflected in the snapshot; newer ones
        // arrived while hl-node was dumping state and would otherwise be lost
        // (the old behavior discarded the whole cache, so every add/cancel
        // during the 10-30s snapshot window silently corrupted the book).
        let cache = self.fetched_snapshot_cache.take().unwrap_or_default();
        self.cached_event_count = 0;
        let mut replayed = 0usize;
        let mut replay_failed = false;
        for batch in cache {
            let res = match batch {
                EventBatch::Orders(b) if b.block_number() > height => {
                    replayed += b.events_len();
                    new_state.apply_order_statuses_hft(b).map(|_| ())
                }
                EventBatch::BookDiffs(b) if b.block_number() > height => {
                    replayed += b.events_len();
                    new_state.apply_order_diffs_hft(b).map(|_| ())
                }
                _ => Ok(()),
            };
            if let Err(err) = res {
                log::warn!("Replay apply error after snapshot at height {height}: {err}");
                replay_failed = true;
            }
        }
        info!("Replayed {replayed} cached events above snapshot height {height}");
        let prior_loss_height = self.max_loss_height;
        self.order_book_state = Some(new_state);

        // A fresh snapshot plus a complete replay is in sync by construction -
        // but only for data at or below the snapshot height. A loss recorded
        // above it (events dropped while the fetch was running) is NOT covered;
        // clearing the flag in that case would erase the signal permanently.
        self.needs_resync = false;
        self.max_loss_height = 0;
        if replay_failed {
            self.mark_desynced("replay_apply_error");
        } else if prior_loss_height > height {
            error!(
                "Data loss bounded by height {prior_loss_height} is above snapshot height {height}; \
                 keeping re-sync scheduled"
            );
            self.needs_resync = true;
            // Downgrade an uninformed (u64::MAX) bound now that real heights
            // have been observed, so the next fetch can actually clear it.
            self.max_loss_height = if prior_loss_height == u64::MAX {
                self.last_seen_height.max(height)
            } else {
                prior_loss_height
            };
        }

        // The incremental L2 cache references the previous book's coins/levels;
        // drop it so the next broadcast does a full rebuild against the new state.
        self.l2_snapshot_cache = HashMap::new();
        // The conflation buffer references the previous universe; clear it too. The
        // cache reset above makes every present coin uncached, so the next broadcast
        // recomputes all of them fresh regardless.
        self.pending_dirty_l2_coins.clear();
        // Force the next flush to treat the active variant set as "changed" so the
        // empty cache is rebuilt against whatever shapes are currently subscribed.
        self.last_active_l2_params.clear();
        info!("Order book ready at height {}", height);
    }

    /// L4 snapshot of one coin's book - (time, height, snapshot). Replaces the
    /// old all-coins compute_snapshot, which cloned the entire multi-book under
    /// the listener lock on every l4Book subscribe and stalled event processing
    /// for hundreds of milliseconds.
    pub(crate) fn compute_snapshot_for_coin(&self, coin: &Coin) -> Option<(u64, u64, Snapshot<InnerL4Order>)> {
        self.order_book_state.as_ref().and_then(|state| state.compute_snapshot_for_coin(coin))
    }
}

/// Does `coin` belong to one of the enabled market types?
fn coin_in_market_filter(coin: &Coin, (include_perps, include_spot, include_hip3): (bool, bool, bool)) -> bool {
    (coin.is_perp() && include_perps) || (coin.is_spot() && include_spot) || (coin.is_hip3() && include_hip3)
}

/// Parse one streaming line into (block height, typed event batch). Runs WITHOUT
/// the listener lock: sonic-rs parsing of a multi-KB block batch is the most
/// expensive step of the event path, and doing it under the lock serialized it
/// against everything else that needs the listener (connection setup, L4
/// snapshots, the L2 flush ticker). Returns None for empty or malformed lines
/// (counted in `PARSE_ERRORS_TOTAL`).
fn parse_event_line(line: &str, event_source: EventSource) -> Option<(u64, EventBatch)> {
    // Count events for debugging
    static HFT_EVENT_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let count = HFT_EVENT_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if count % 1000 == 0 {
        log::debug!("parse_event_line event #{}, source: {}, line_len: {}", count, event_source, line.len());
    }

    if line.is_empty() {
        return None;
    }

    // Parse the batch
    let res = match event_source {
        EventSource::Fills => sonic_rs::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
            let height = batch.block_number();
            (height, EventBatch::Fills(batch))
        }),
        EventSource::OrderStatuses => sonic_rs::from_str(line)
            .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
        EventSource::OrderDiffs => sonic_rs::from_str(line)
            .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
    };

    match res {
        Ok((height, event_batch)) => {
            // Record file watcher metrics
            FILE_EVENTS_TOTAL.with_label_values(&[event_source.metric_label()]).inc();
            FILE_LINES_PARSED_TOTAL.with_label_values(&[event_source.metric_label()]).inc_by(line.len() as u64);

            // Log successful parses periodically
            static PARSE_OK_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let ok_count = PARSE_OK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if ok_count % 10_000 == 0 {
                log::debug!("parse OK #{}: height={}, source={}", ok_count, height, event_source);
            }
            Some((height, event_batch))
        }
        Err(err) => {
            // Log ALL parse errors for debugging
            PARSE_ERRORS_TOTAL.with_label_values(&[event_source.metric_label()]).inc();
            static PARSE_ERR_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let err_count = PARSE_ERR_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if err_count % 1000 == 0 {
                error!("parse error #{}: {}, source: {}, line_len: {}", err_count, err, event_source, line.len());
            }
            None
        }
    }
}


/// Lazily-serialized wire frame shared by every subscribed connection. The
/// first connection that needs the frame pays the `serde_json` cost once; every
/// other connection clones the refcounted bytes. (The old path re-serialized
/// the identical payload once per subscribed connection per message, so fan-out
/// CPU scaled with the subscriber count.)
pub(crate) struct SharedFrame(std::sync::OnceLock<bytes::Bytes>);

impl SharedFrame {
    const fn new() -> Self {
        Self(std::sync::OnceLock::new())
    }

    /// The serialized frame, building it on first use. A serialization failure
    /// (our bug, not the client's) is logged once and yields an empty frame,
    /// which the send path skips.
    pub(crate) fn get_or_serialize<T: serde::Serialize>(&self, build: impl FnOnce() -> T) -> bytes::Bytes {
        self.0
            .get_or_init(|| match serde_json::to_string(&build()) {
                Ok(json) => bytes::Bytes::from(json),
                Err(err) => {
                    error!("Server response serialization error: {err}");
                    bytes::Bytes::new()
                }
            })
            .clone()
    }
}

/// One coin's trades plus the shared `trades` wire frame.
pub(crate) struct CoinTrades {
    pub(crate) trades: Arc<Vec<Trade>>,
    pub(crate) frame: SharedFrame,
}

/// One coin's order diffs plus the shared wire frames derived from them
/// (`bookDiffs` for `BookDiffs` subscribers, `l4Book` updates for `L4Book` ones).
pub(crate) struct CoinDiffs {
    pub(crate) diffs: Arc<Vec<NodeDataOrderDiff>>,
    pub(crate) book_diffs_frame: SharedFrame,
    pub(crate) l4_frame: SharedFrame,
}

/// One coin's order statuses plus the shared `l4Book` updates wire frame.
/// `OrderUpdates` subscribers filter the raw `statuses` per user instead.
pub(crate) struct CoinStatuses {
    pub(crate) statuses: Arc<Vec<NodeDataOrderStatus>>,
    pub(crate) l4_frame: SharedFrame,
}

/// Raw fixed-point (px, sz, n) for the best bid and ask of one coin.
pub(crate) type RawBbo = (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>);

/// One coin's BBO: the raw fixed-point values (connections dedup on these
/// without allocating) plus the shared `bbo` wire frame, rendered/serialized
/// once per coin per broadcast instead of once per subscribed connection.
pub(crate) struct CoinBbo {
    pub(crate) raw: RawBbo,
    pub(crate) frame: SharedFrame,
}

/// Cache key for one fully-rendered L2 variant: the aggregation shape plus the
/// send-time `n_levels` truncation (two subscriptions differing only in
/// `n_levels` produce different payloads).
#[derive(Hash, Eq, PartialEq)]
pub(crate) struct L2FrameKey {
    coin: String,
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
    n_levels: usize,
}

impl L2FrameKey {
    pub(crate) fn new(coin: &str, n_sig_figs: Option<u32>, mantissa: Option<u64>, n_levels: usize) -> Self {
        Self { coin: coin.to_string(), n_sig_figs, mantissa, n_levels }
    }
}

/// One rendered L2 variant: (dedup hash over the exported levels, wire frame,
/// payload struct for heartbeat-enabled connections).
pub(crate) type L2BuiltFrame = (u64, bytes::Bytes, L2Book);

/// Per-broadcast lazy cache of rendered L2 variants, shared by every connection
/// through the message `Arc`. The first connection needing a given
/// (coin, shape, `n_levels`) pays the truncate/export/hash/serialize cost once;
/// every other connection reuses the dedup hash, the wire frame (refcounted
/// bytes), and - for heartbeat-enabled connections - the payload struct.
/// The old path repeated all of that work per subscribed connection.
pub(crate) struct L2FrameCache(std::sync::Mutex<rustc_hash::FxHashMap<L2FrameKey, Arc<L2BuiltFrame>>>);

impl L2FrameCache {
    fn new() -> Self {
        Self(std::sync::Mutex::new(rustc_hash::FxHashMap::default()))
    }

    /// Cached (dedup hash, frame, payload) for `key`, building on first use.
    /// `build` runs OUTSIDE the lock so a slow render never blocks other
    /// connections; on a lost insert race the first entry wins (both builds
    /// produce identical bytes).
    pub(crate) fn get_or_build(&self, key: L2FrameKey, build: impl FnOnce() -> L2BuiltFrame) -> Arc<L2BuiltFrame> {
        if let Ok(map) = self.0.lock()
            && let Some(hit) = map.get(&key)
        {
            return Arc::clone(hit);
        }
        let fresh = Arc::new(build());
        match self.0.lock() {
            Ok(mut map) => Arc::clone(map.entry(key).or_insert(fresh)),
            // Poisoned lock (a panicked builder elsewhere): serve the local build.
            Err(_) => fresh,
        }
    }
}

/// Group a fills batch into per-coin trade vectors. Consumes the batch (fills
/// are never applied to the book), so this is move-only - no event is cloned.
/// Pairs the two fill legs of each trade match into one public-schema print,
/// grouped per coin for broadcast.
///
/// A match produces two fill records (buyer + seller) sharing a `tid`. The
/// node emits them as immediate neighbours in the fills stream — adjacent
/// within a block, and in `--stream-with-block-info` mode as two consecutive
/// single-event `Fills` batches. So pairing only needs to remember the single
/// previous leg: when the next leg shares its `tid` they form a trade;
/// otherwise the previous leg was unpairable and is dropped. This is O(1) per
/// fill, allocates nothing beyond the output, and cannot leak — at most one
/// leg is ever held.
///
/// Dropped legs are counted in `TRADES_UNPAIRED_FILLS_TOTAL` — ~0 in steady
/// state; growth means the node stopped emitting the two legs of a match
/// adjacently and pairing should be revisited.
#[derive(Default)]
struct TradePairer {
    prev: Option<NodeDataFill>,
}

impl TradePairer {
    fn group(&mut self, batch: Batch<NodeDataFill>) -> HashMap<String, CoinTrades> {
        let mut by_coin: HashMap<String, Vec<Trade>> = HashMap::new();
        for fill in batch.events() {
            match self.prev.take() {
                Some(prev) if prev.1.tid == fill.1.tid => {
                    let (bid, ask) = if fill.1.side == Side::Bid { (fill, prev) } else { (prev, fill) };
                    if let Some(trade) = Trade::from_fills(bid, ask) {
                        by_coin.entry(trade.coin.clone()).or_default().push(trade);
                    } else {
                        // Same tid but mismatched coin/sides: both legs are
                        // unpairable, count them so this stays observable.
                        TRADES_UNPAIRED_FILLS_TOTAL.inc_by(2);
                    }
                }
                Some(_) => {
                    // Previous leg never met its counterpart: unpairable, drop it.
                    TRADES_UNPAIRED_FILLS_TOTAL.inc();
                    self.prev = Some(fill);
                }
                None => self.prev = Some(fill),
            }
        }
        by_coin
            .into_iter()
            .map(|(coin, trades)| (coin, CoinTrades { trades: Arc::new(trades), frame: SharedFrame::new() }))
            .collect()
    }
}

/// Group order diffs per coin (one clone per event - the batch itself is
/// consumed by the state apply). `skip_spot` folds the `--markets` filtering
/// in, replacing the old whole-batch `filter_events` clone.
fn group_diffs_by_coin(events: &[NodeDataOrderDiff], skip_spot: bool) -> HashMap<String, CoinDiffs> {
    let mut by_coin: HashMap<String, Vec<NodeDataOrderDiff>> = HashMap::new();
    for diff in events {
        let coin = diff.coin();
        if skip_spot && coin.is_spot() {
            continue;
        }
        by_coin.entry(coin.value()).or_default().push(diff.clone());
    }
    by_coin
        .into_iter()
        .map(|(coin, diffs)| {
            (
                coin,
                CoinDiffs { diffs: Arc::new(diffs), book_diffs_frame: SharedFrame::new(), l4_frame: SharedFrame::new() },
            )
        })
        .collect()
}

/// Group order statuses per coin (one clone per event).
fn group_statuses_by_coin(events: &[NodeDataOrderStatus]) -> HashMap<String, CoinStatuses> {
    let mut by_coin: HashMap<String, Vec<NodeDataOrderStatus>> = HashMap::new();
    for status in events {
        by_coin.entry(status.order.coin.clone()).or_default().push(status.clone());
    }
    by_coin
        .into_iter()
        .map(|(coin, statuses)| (coin, CoinStatuses { statuses: Arc::new(statuses), l4_frame: SharedFrame::new() }))
        .collect()
}

impl OrderBookListener {
    /// Append a batch to the replay cache, enforcing the event cap. On overflow
    /// the cache is dropped and the book is marked for re-sync (the cap means a
    /// snapshot fetch is taking pathologically long; better to re-sync again
    /// than to risk OOM).
    fn push_to_replay_cache(&mut self, event_batch: EventBatch) {
        let events_len = match &event_batch {
            EventBatch::Orders(b) => b.events_len(),
            EventBatch::BookDiffs(b) => b.events_len(),
            EventBatch::Fills(b) => b.events_len(),
        };
        if self.cached_event_count.saturating_add(events_len) > self.cache_event_cap {
            error!(
                "Replay cache overflow ({} + {events_len} events > cap {}); dropping cache and scheduling re-sync",
                self.cached_event_count, self.cache_event_cap
            );
            self.fetched_snapshot_cache = None;
            self.cached_event_count = 0;
            // The recorded loss bound (~current height) sits above the pending
            // snapshot's height, so init_from_snapshot keeps the book marked.
            self.mark_desynced("event_cache_overflow");
            return;
        }
        self.cached_event_count += events_len;
        if let Some(cache) = self.fetched_snapshot_cache.as_mut() {
            cache.push_back(event_batch);
        }
    }

    /// While a snapshot fetch is pending, cache book-affecting batches for replay.
    /// Returns the batch to apply live, or None when it was moved into the cache
    /// (book not ready yet) or must be dropped (not ready and the cache is gone).
    fn cache_for_replay(&mut self, event_batch: EventBatch) -> Option<EventBatch> {
        let is_ready = self.order_book_state.is_some();
        // Fills never mutate the book - nothing to replay. A missing cache means
        // steady state (no fetch pending) or the post-overflow window where the
        // book is already marked for re-sync.
        if matches!(event_batch, EventBatch::Fills(_)) || self.fetched_snapshot_cache.is_none() {
            return is_ready.then_some(event_batch);
        }
        if is_ready {
            // Applied live by the caller AND replayed onto the incoming snapshot.
            self.push_to_replay_cache(event_batch.clone());
            Some(event_batch)
        } else {
            self.push_to_replay_cache(event_batch);
            None
        }
    }

    /// Cache a startup-backfill batch for snapshot replay. Backfill batches are
    /// NEVER applied to a live book: they are older than the live stream by
    /// construction, and applying e.g. a stale size update on top of newer
    /// state would corrupt the book. If the replay cache is already gone (the
    /// snapshot landed before the backfill drained, or the cache overflowed),
    /// the batch cannot be used safely - mark the book for re-sync instead; the
    /// re-fetched snapshot's height supersedes everything the backfill carried.
    fn cache_backfill_batch(&mut self, event_batch: EventBatch) {
        if matches!(event_batch, EventBatch::Fills(_)) {
            return;
        }
        if self.fetched_snapshot_cache.is_some() {
            self.push_to_replay_cache(event_batch);
        } else {
            self.mark_desynced("late_backfill");
        }
    }

    /// Apply a parsed event batch to the book and run the fast broadcast paths.
    /// HFT mode: events are applied the instant they arrive, without block-level
    /// synchronization, with order-level caching for status/diff pairing.
    /// Runs under the listener lock; everything here must stay cheap.
    fn apply_event_batch(&mut self, height: u64, event_batch: EventBatch, event_source: EventSource) {
        /// Largest batch we'll process. Each event is a few hundred bytes; a 100k-event
        /// batch would already block the listener for seconds and pin hundreds of MB.
        /// In normal operation a single block's batch is tens to low thousands of events.
        const MAX_EVENTS_PER_BATCH: usize = 100_000;
        let source_label = event_source.metric_label();

        // Track the highest live-stream height: it bounds the height of any
        // data loss whose exact position is unknown (see mark_desynced).
        self.last_seen_height = self.last_seen_height.max(height);

        // Sanity cap on batch size. A malformed/malicious line could otherwise
        // pin hundreds of MB and freeze the listener for seconds.
        let events_len = match &event_batch {
            EventBatch::Orders(b) => b.events_len(),
            EventBatch::BookDiffs(b) => b.events_len(),
            EventBatch::Fills(b) => b.events_len(),
        };
        if events_len > MAX_EVENTS_PER_BATCH {
            PARSE_ERRORS_TOTAL.with_label_values(&[source_label]).inc();
            error!(
                "Dropping oversize batch from {source_label}: {events_len} events (cap {MAX_EVENTS_PER_BATCH}), height={height}"
            );
            // The dropped events are gone for good; without a re-sync every
            // affected coin would serve a silently wrong book forever.
            self.mark_desynced("oversize_batch");
            return;
        }

        let process_start = Instant::now();

        if height % 100 == 0 {
            log::debug!("{event_source} block: {height}");
        }

        // Cache for replay while a snapshot fetch is in flight; bail out when the
        // batch was consumed by the cache (book not ready to apply it yet).
        let Some(event_batch) = self.cache_for_replay(event_batch) else {
            return;
        };

        // Collected here and applied after the state borrow ends.
        let mut desync_reason: Option<&'static str> = None;

        let changed_coins: HashSet<Coin> = if let Some(state) = self.order_book_state.as_mut() {
            let result = match event_batch {
                EventBatch::Orders(batch) => {
                    // Broadcast L4 order statuses for L4Book / orderUpdates
                    // subscribers: grouped per coin ONCE here (one clone per
                    // event), shared via Arc by every connection - the old path
                    // cloned the whole batch per subscribed connection.
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 && batch.events_len() > 0 {
                            let msg = Arc::new(InternalMessage::L4OrderStatuses {
                                time: batch.block_time(),
                                height: batch.block_number(),
                                statuses_by_coin: group_statuses_by_coin(batch.events_ref()),
                            });
                            drop(tx.send(msg));
                        }
                    }
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["orders"]).inc();
                    state.apply_order_statuses_hft(batch)
                }
                EventBatch::BookDiffs(batch) => {
                    // Broadcast L4 order diffs for L4Book / BookDiffs subscribers,
                    // grouped per coin once and Arc-shared across connections.
                    // Defense-in-depth: when running with `ignore_spot=true`, the
                    // grouping also strips spot diffs - otherwise `bookDiffs`
                    // clients would see events for coins whose state we never
                    // applied locally.
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 {
                            let diffs_by_coin = group_diffs_by_coin(batch.events_ref(), state.ignore_spot());
                            if !diffs_by_coin.is_empty() {
                                let msg = Arc::new(InternalMessage::L4OrderDiffs {
                                    time: batch.block_time(),
                                    height: batch.block_number(),
                                    diffs_by_coin,
                                });
                                drop(tx.send(msg));
                            }
                        }
                    }
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["diffs"]).inc();
                    state.apply_order_diffs_hft(batch)
                }
                EventBatch::Fills(batch) => {
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["fills"]).inc();
                    // Broadcast fills grouped per coin (move-only - the batch is
                    // never applied to the book, so no event is cloned).
                    if self.internal_message_tx.as_ref().is_some_and(|tx| tx.receiver_count() > 0) {
                        let trades_by_coin = self.trade_pairer.group(batch);
                        if !trades_by_coin.is_empty() {
                            if let Some(tx) = &self.internal_message_tx {
                                drop(tx.send(Arc::new(InternalMessage::Fills { trades_by_coin })));
                            }
                        }
                    }
                    Ok(HashSet::new())
                }
            };

            match result {
                Ok(coins) => coins,
                Err(err) => {
                    // Per-event errors (malformed Px/Sz, unrecognized diff variant) are
                    // recoverable: skip the offending batch and keep serving every other
                    // coin's state. Discarding `order_book_state` here used to take down
                    // the entire feed for ~10s on a single malformed line. The skipped
                    // batch is still lost data, so schedule a background re-sync.
                    PARSE_ERRORS_TOTAL.with_label_values(&[source_label]).inc();
                    log::warn!(
                        "Skipping event batch at height={} source={} due to apply error: {err}",
                        height, source_label
                    );
                    desync_reason = Some("apply_error");
                    HashSet::new()
                }
            }
        } else {
            HashSet::new()
        };
        EVENT_PROCESSING_LATENCY.with_label_values(&[source_label]).observe(process_start.elapsed().as_secs_f64());

        // Log HFT state progress periodically
        static HFT_STATE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sc = HFT_STATE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if sc % 1000 == 0 {
            if let Some(state) = &mut self.order_book_state {
                // Record health metrics
                ORDERBOOK_HEIGHT.set(state.height() as i64);
                ORDERBOOK_TIME_MS.set(state.time() as i64);
                PENDING_ORDERS_CACHE.set(state.pending_order_statuses_count() as i64);
                PENDING_DIFFS_CACHE.set(state.pending_new_diffs_count() as i64);

                // Record orderbook stats
                ORDERBOOK_ORDERS_TOTAL.set(state.order_count() as i64);
                ORDERBOOK_COINS_COUNT.set(state.coin_count() as i64);

                // Cleanup stale pending entries to prevent unbounded memory growth.
                // A force-clear may evict genuinely in-flight order halves, so it
                // counts as data loss and the book must re-sync.
                if state.cleanup_stale_pending() {
                    desync_reason = Some("pending_cache_cleared");
                }

                info!(
                    "State progress #{}: height={}, pending_statuses={}, pending_diffs={}",
                    sc,
                    state.height(),
                    state.pending_order_statuses_count(),
                    state.pending_new_diffs_count()
                );
            }
        }
        if let Some(reason) = desync_reason {
            self.mark_desynced(reason);
        }

        // Fast BBO broadcast - ONLY for coins that changed AND only when someone is
        // listening. Without the receiver-count gate we'd `get_bbos_for_coins` and
        // spawn a tokio task per change even with zero subscribers, wasting CPU.
        if !changed_coins.is_empty() {
            if let Some(state) = &self.order_book_state {
                if let Some(tx) = &self.internal_message_tx {
                    if tx.receiver_count() > 0 {
                        let bbo_start = Instant::now();
                        let (time, bbos) = state.get_bbos_for_coins(&changed_coins);
                        static BBO_BROADCAST_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                        let bc = BBO_BROADCAST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if bc % 1000 == 0 {
                            log::debug!("Fast BBO broadcast #{} at time {} for {} coins", bc, time, changed_coins.len());
                        }
                        // broadcast::Sender::send is non-blocking; the previous
                        // tokio::spawn wrapper added task overhead with no benefit.
                        let bbos = bbos
                            .into_iter()
                            .map(|(coin, raw)| (coin, CoinBbo { raw, frame: SharedFrame::new() }))
                            .collect();
                        let msg = Arc::new(InternalMessage::BboUpdate { bbos, time });
                        drop(tx.send(msg));
                        BBO_BROADCAST_LATENCY.observe(bbo_start.elapsed().as_secs_f64());
                    }
                }
            }
        }

        // Throttled L2 snapshot broadcast for L2Book subscribers.
        // l2_snapshots_incremental() walks every changed coin x every aggregation
        // variant, so limit to 20 broadcasts/sec max (50ms).
        // (Heartbeat resend for quiet coins is handled per-connection in handle_socket.)
        //
        // Conflation: every event accumulates its changed coins into a persistent
        // buffer. Because L2 is throttled to one broadcast / 50ms, the buffer holds
        // EVERY coin that changed since the last broadcast - not just the coins in the
        // triggering event. Without this, a coin that changed during a throttle-
        // suppressed window was never marked for rebuild and served a stale cached
        // snapshot until a later event for that exact coin happened to land on an open
        // throttle slot (165-2260ms L2 update gaps with many active coins, while BBO -
        // which reads live state every event - stayed fresh).
        //
        // CRITICAL: the receiver_count gate must wrap the compute, not sit between
        // compute and send. A prior version updated last_l2_broadcast only when
        // receivers existed, so with zero subscribers the throttle reset never fired
        // and the par_iter ran on every event - tens of GB of allocator churn per hour
        // and a pinned listener mutex. We still set last_l2_broadcast unconditionally
        // below. The buffer is only drained inside the has_receivers + Some(state)
        // branch where we actually rebuild: draining anywhere else would clear the
        // coins without refreshing the cache, so a later-connecting subscriber would
        // be served their stale snapshots. With no subscribers the buffer keeps
        // accumulating (deduped by coin, bounded by the universe size).
        self.pending_dirty_l2_coins.extend(changed_coins.iter().cloned());
        // The L2 broadcast is NOT done inline here. The main event loop calls
        // flush_l2_if_due() right after this (and a flush ticker backstops quiet
        // periods), so the broadcast fires the moment the throttle window expires
        // instead of waiting for the next tick. The event path stays minimal:
        // apply + BBO + accumulate.
    }

    /// Flush the L2 conflation buffer if the throttle window has elapsed and there
    /// are dirty coins. Driven by the main-loop flush ticker so the L2 feed has a
    /// guaranteed maximum interval (~throttle + tick) regardless of event arrival.
    /// Safe to call on every tick: O(1) early-return when not due. Runs under the
    /// listener lock.
    pub(crate) fn flush_l2_if_due(&mut self) {
        let should_broadcast_l2 = !self.pending_dirty_l2_coins.is_empty()
            && self
                .last_l2_broadcast
                .map(|t| t.elapsed() >= Duration::from_millis(L2_BROADCAST_THROTTLE_MS))
                .unwrap_or(true);
        if !should_broadcast_l2 {
            return;
        }

        let has_receivers = self.internal_message_tx.as_ref().is_some_and(|tx| tx.receiver_count() > 0);
        // Mark the throttle as fired regardless of receivers so we don't re-run the
        // par_iter path on every subsequent tick when nobody is listening.
        self.last_l2_broadcast = Some(Instant::now());
        if !has_receivers {
            return;
        }

        // Compute only the variant shapes some connection currently wants. With no
        // L2 subscribers there is nothing to build or send.
        let active = self.active_l2_params.snapshot();
        if active.is_empty() {
            return;
        }
        // When the requested shape set changes, the cache holds the wrong shapes;
        // clear it so every present coin is rebuilt with the new set on this flush
        // (bounds a new subscriber's wait to one throttle window even on quiet coins).
        if active != self.last_active_l2_params {
            self.l2_snapshot_cache.clear();
            self.last_active_l2_params.clone_from(&active);
        }

        if let Some(state) = &self.order_book_state {
            // Drain the conflation buffer only now that we will actually rebuild.
            // mem::take yields an owned set, releasing the borrow on
            // self.pending_dirty_l2_coins before the disjoint co-borrow of
            // order_book_state (&) and l2_snapshot_cache (&mut).
            let dirty = std::mem::take(&mut self.pending_dirty_l2_coins);
            L2_CONFLATION_BATCH_SIZE.observe(dirty.len() as f64);
            let l2_start = Instant::now();
            let (time, l2_snapshots, recomputed, coin_set_changed) =
                state.l2_snapshots_incremental(&dirty, &active, &mut self.l2_snapshot_cache);

            static L2_BROADCAST_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let bc = L2_BROADCAST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if bc % 100 == 0 {
                log::debug!("L2 broadcast #{} at time {} for {} dirty coins", bc, time, dirty.len());
            }

            // Rebuild the shared universe only when the coin set actually changed.
            // Built once here instead of once per connection per broadcast (the
            // old per-connection derivation allocated the full coin-name set for
            // every connection on every flush).
            let universe = if coin_set_changed {
                let market_filter = self.market_filter;
                Some(Arc::new(
                    self.l2_snapshot_cache
                        .keys()
                        .filter(|coin| coin_in_market_filter(coin, market_filter))
                        .map(Coin::value)
                        .collect::<HashSet<String>>(),
                ))
            } else {
                None
            };

            if let Some(tx) = &self.internal_message_tx {
                let msg = Arc::new(InternalMessage::Snapshot {
                    l2_snapshots,
                    time,
                    dirty: recomputed,
                    universe,
                    l2_frames: L2FrameCache::new(),
                });
                drop(tx.send(msg));
            }
            L2_BROADCAST_LATENCY.observe(l2_start.elapsed().as_secs_f64());
        }
    }
}

/// Per-coin L2 snapshots, one inner map per coin holding all aggregation variants.
/// The inner maps are Arc'd so the listener-side cache and the broadcast Arc can
/// share unchanged coins' data without deep-cloning their level vectors.
pub(crate) struct L2Snapshots(HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>);

impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>> {
        &self.0
    }
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    Snapshot {
        l2_snapshots: L2Snapshots,
        time: u64,
        /// Coins whose snapshots were rebuilt in this flush. Connections skip
        /// L2 subscriptions whose coin is absent (their previously-sent payload
        /// is still current), avoiding per-coin truncate/export/hash work for
        /// quiet coins on every broadcast.
        dirty: HashSet<Coin>,
        /// Refreshed market-filtered universe; Some only when the coin set
        /// changed since the previous broadcast.
        universe: Option<Arc<HashSet<String>>>,
        /// Lazy per-broadcast cache of rendered L2 frames, shared by every
        /// connection (see [`L2FrameCache`]).
        l2_frames: L2FrameCache,
    },
    /// Trades grouped per coin ONCE in the listener; connections share the
    /// Arc'd vectors AND the lazily-serialized wire frame per coin.
    Fills {
        trades_by_coin: HashMap<String, CoinTrades>,
    },
    /// Fast BBO-only broadcast path - bypasses expensive L2 snapshot computation.
    /// Connections dedup on the raw values and share the per-coin wire frame.
    BboUpdate {
        bbos: HashMap<Coin, CoinBbo>,
        time: u64,
    },
    /// HFT L4 streaming - order diffs without waiting for status pairing,
    /// grouped per coin once (shared by l4Book and bookDiffs subscribers).
    L4OrderDiffs {
        time: u64,
        height: u64,
        diffs_by_coin: HashMap<String, CoinDiffs>,
    },
    /// HFT L4 streaming - order statuses without waiting for diff pairing,
    /// grouped per coin once (shared by l4Book and orderUpdates subscribers).
    L4OrderStatuses {
        time: u64,
        height: u64,
        statuses_by_coin: HashMap<String, CoinStatuses>,
    },
}

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}

/// Refcounted set of L2 aggregation variant *shapes* that some live connection
/// currently wants. The listener reads it at flush time and computes only those
/// variants (instead of all 7) for every dirty coin. Bounded to the handful of
/// supported shapes. Shared (Arc) between the listener and every connection.
///
/// A plain `std::sync::Mutex` is used deliberately: every access is O(1) and never
/// spans an `.await`, and the RAII guard's `Drop` (which must run on abnormal
/// disconnects too) cannot be async.
#[derive(Clone, Default)]
pub(crate) struct ActiveL2Params {
    inner: Arc<std::sync::Mutex<HashMap<L2SnapshotParams, usize>>>,
}

impl ActiveL2Params {
    pub(crate) fn new() -> Self {
        Self { inner: Arc::new(std::sync::Mutex::new(HashMap::new())) }
    }

    /// Increment the refcount for `params` and return a guard that decrements on
    /// drop. Holding the guard keeps the shape in the active set.
    #[must_use]
    pub(crate) fn acquire(&self, params: L2SnapshotParams) -> L2ParamGuard {
        if let Ok(mut map) = self.inner.lock() {
            *map.entry(params).or_insert(0) += 1;
        }
        L2ParamGuard { registry: self.inner.clone(), params }
    }

    /// Snapshot the currently-active variant shapes. Cloned under the lock; the
    /// lock is released before returning.
    pub(crate) fn snapshot(&self) -> HashSet<L2SnapshotParams> {
        self.inner.lock().map(|m| m.keys().copied().collect()).unwrap_or_default()
    }
}

/// RAII guard returned by [`ActiveL2Params::acquire`]. Decrements the refcount on
/// drop and removes the shape at zero. Cleanup is panic/disconnect-safe because
/// `Drop` runs during normal scope exit, early `return`, and unwinding alike.
pub(crate) struct L2ParamGuard {
    registry: Arc<std::sync::Mutex<HashMap<L2SnapshotParams, usize>>>,
    params: L2SnapshotParams,
}

impl Drop for L2ParamGuard {
    fn drop(&mut self) {
        if let Ok(mut map) = self.registry.lock() {
            if let Some(count) = map.get_mut(&self.params) {
                *count -= 1;
                if *count == 0 {
                    map.remove(&self.params);
                }
            }
        }
    }
}

// ============================================================================
// HFT-OPTIMIZED VERSION
// Uses parallel file watchers and immediate OrderDiff processing
// ============================================================================

/// HFT-optimized listener using parallel file watchers
/// Key differences from hl_listen:
/// 1. 3 dedicated threads for file watching (parallel I/O)
/// 2. Processes OrderDiffs immediately (doesn't wait for OrderStatuses)
/// 3. Uses process time instead of block time for lowest latency
pub(crate) async fn hl_listen_hft(listener: Arc<Mutex<OrderBookListener>>, config: crate::ServerConfig) -> Result<()> {
    let dir = match config.data_dir.clone() {
        Some(d) => d,
        None => dirs::home_dir().ok_or(
            "Could not resolve a data directory: pass --data-dir explicitly. The default \
             requires a usable HOME environment variable, which was not set or is invalid.",
        )?,
    };

    info!("Starting HFT-optimized listener");
    info!("Data directory: {:?}", dir);

    // Create SnapshotConfig from ServerConfig
    let snapshot_config = SnapshotConfig {
        mode: config.snapshot_mode,
        docker_container: config.docker_container.clone(),
        hlnode_binary: config.hlnode_binary.clone(),
        abci_state_path: config.abci_state_path.clone(),
        snapshot_output_path: config.snapshot_output_path.clone(),
        visor_state_path: config.visor_state_path.clone(),
        data_dir: dir.clone(),
    };

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    // Startup-backfill floor: the node's currently persisted height. The initial
    // snapshot is generated after this point, so its height is >= the floor and
    // every line at or below it is already covered by the snapshot. The watchers
    // backfill on-disk lines above the floor that the old seek-to-EOF behavior
    // skipped. 0 (visor unreadable) disables the backfill - the snapshot load
    // would fail on the same file anyway.
    let backfill_min_height = read_visor_height(&get_visor_path(&snapshot_config)).unwrap_or(0);
    info!("Startup backfill floor height: {backfill_min_height}");

    // Start parallel file watchers. They send straight into the bounded tokio
    // channel via blocking_send (backpressure parks the reader threads; the
    // events sit on disk meanwhile). The join handles and health timestamps
    // feed the watchdog in the periodic ticker below - a dead or wedged
    // watcher must not let the server keep serving a silently frozen book.
    let (mut tokio_rx, watcher_handles, last_order_statuses, _last_fills, last_order_diffs) =
        parallel::start_parallel_file_watchers(dir, backfill_min_height);

    // Snapshot fetch channel
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = tokio::time::interval_at(start, Duration::from_secs(10));
    let mut snapshot_fetch_pending = false;

    // Drives L2 broadcasts on a fixed cadence so the feed has a guaranteed maximum
    // interval even when no events arrive. Skip missed ticks so a busy loop resumes
    // on the next aligned tick rather than firing a catch-up burst.
    let mut l2_flush_ticker = interval(Duration::from_millis(L2_FLUSH_TICK_MS));
    l2_flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // How many watcher events to drain per loop iteration. During block bursts
    // the old one-line-per-lock pattern paid a listener lock/unlock plus a
    // task wake per event; draining a batch amortizes that to one acquisition.
    const EVENT_RECV_BATCH: usize = 64;
    let mut event_buf: Vec<parallel::FileEvent> = Vec::with_capacity(EVENT_RECV_BATCH);

    // Book-critical watchers (diffs/statuses) stream continuously on mainnet;
    // silence this long means the node stream or the watcher is wedged.
    const WATCHER_STALL_ALARM_MS: u64 = 120_000;

    /// One parsed watcher event, ready to apply under the listener lock.
    enum Action {
        Apply(u64, EventBatch, EventSource),
        Backfill(EventBatch),
        Desync,
    }

    info!("Main event loop starting");

    loop {
        tokio::select! {
            biased;

            // L2 flush FIRST under `biased`: guarantees the throttle window is
            // serviced even under continuous event load. During a burst tokio_rx is
            // perpetually ready, so a later-placed flush arm would be starved and the
            // multi-second gaps would return. flush_l2_if_due() is O(1) when not due
            // and the tick is only Ready every L2_FLUSH_TICK_MS, so it cannot starve
            // the event arm in return.
            _ = l2_flush_ticker.tick() => {
                listener.lock().await.flush_l2_if_due();
            }

            // Process events from the file watchers, draining up to
            // EVENT_RECV_BATCH per iteration (recv_many is cancel-safe).
            count = tokio_rx.recv_many(&mut event_buf, EVENT_RECV_BATCH) => {
                if count == 0 {
                    // Every watcher thread dropped its sender: the book can only
                    // go stale from here. Exit so the supervisor restarts us.
                    return Err("file watcher channel closed (all watcher threads exited)".into());
                }
                // Parse the whole batch outside the listener lock (sonic-rs
                // parsing is the most expensive step of the event path), then
                // apply everything + flush under a single lock acquisition.
                // Arrival order is preserved across the batch.
                let mut actions = Vec::with_capacity(count);
                for event in event_buf.drain(..) {
                    let (line, source, is_backfill) = match event {
                        // OrderDiffs are the BBO-critical path; statuses and fills
                        // are less latency-sensitive but share the same flow.
                        parallel::FileEvent::OrderDiff(line) => (line, EventSource::OrderDiffs, false),
                        parallel::FileEvent::OrderStatus(line) => (line, EventSource::OrderStatuses, false),
                        parallel::FileEvent::Fill(line) => (line, EventSource::Fills, false),
                        parallel::FileEvent::BackfillOrderDiff(line) => (line, EventSource::OrderDiffs, true),
                        parallel::FileEvent::BackfillOrderStatus(line) => (line, EventSource::OrderStatuses, true),
                        parallel::FileEvent::Desync(source) => {
                            // The watcher discarded data (oversized partial line);
                            // the book can no longer be trusted - trigger a re-sync.
                            error!("{source} watcher reported data loss");
                            actions.push(Action::Desync);
                            continue;
                        }
                    };
                    if let Some((height, batch)) = parse_event_line(&line, source) {
                        actions.push(if is_backfill {
                            // Backfill lines are cache-only: replayed above the
                            // snapshot height, never applied to a live book.
                            Action::Backfill(batch)
                        } else {
                            Action::Apply(height, batch, source)
                        });
                    }
                }
                if !actions.is_empty() {
                    let mut guard = listener.lock().await;
                    for action in actions {
                        match action {
                            Action::Apply(height, batch, source) => guard.apply_event_batch(height, batch, source),
                            Action::Backfill(batch) => guard.cache_backfill_batch(batch),
                            Action::Desync => guard.mark_desynced("watcher_data_loss"),
                        }
                    }
                    // The inline flush broadcasts the instant the L2 throttle
                    // window expires (O(1) when not due) instead of waiting for
                    // the next flush tick.
                    guard.flush_l2_if_due();
                }
            }

            // Snapshot fetch result
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                snapshot_fetch_pending = false;
                match snapshot_fetch_res {
                    None => {
                        return Err("Snapshot fetch task sender dropped".into());
                    }
                    Some(Err(err)) => {
                        if listener.lock().await.is_ready() {
                            // A re-sync fetch failed (e.g. transient docker /
                            // hl-node hiccup). Keep serving the current book;
                            // needs_resync is still set so the ticker retries.
                            error!("Snapshot re-fetch failed; will retry: {err}");
                        } else {
                            // Initial snapshot is required to serve anything.
                            return Err(format!("Abci state reading error: {err}").into());
                        }
                    }
                    Some(Ok(())) => {}
                }
            }

            // Periodic snapshot fetch: initial startup, plus whenever the book
            // was marked out-of-sync (events were provably lost). The re-fetch
            // rebuilds the book from a fresh snapshot + cached-event replay.
            _ = ticker.tick() => {
                // Watchdog: a dead watcher thread means its stream is gone for
                // good - exit (the process supervisor restarts us into a clean
                // re-sync) instead of serving a silently frozen book as ready.
                if watcher_handles.iter().any(std::thread::JoinHandle::is_finished) {
                    return Err("a file watcher thread exited; restarting to recover".into());
                }
                // A wedged-but-alive stream (node stopped writing) is loud-logged;
                // restarting us would not fix the node, so don't exit for it.
                let now_ms = parallel::now_unix_ms();
                for (name, last) in [("OrderDiffs", &last_order_diffs), ("OrderStatuses", &last_order_statuses)] {
                    let ts = last.load(std::sync::atomic::Ordering::Relaxed);
                    if ts > 0 && now_ms.saturating_sub(ts) > WATCHER_STALL_ALARM_MS {
                        error!(
                            "{name} watcher has produced no events for {}s - node stream stalled?",
                            now_ms.saturating_sub(ts) / 1000
                        );
                    }
                }

                let (is_ready, needs_resync) = {
                    let guard = listener.lock().await;
                    (guard.is_ready(), guard.needs_resync())
                };
                info!("Ticker: is_ready={is_ready}, needs_resync={needs_resync}, snapshot_fetch_pending={snapshot_fetch_pending}");
                if (!is_ready || needs_resync) && !snapshot_fetch_pending {
                    snapshot_fetch_pending = true;
                    let listener = listener.clone();
                    let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                    fetch_snapshot(snapshot_config.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Build a ready listener (state initialized from an empty snapshot) with a held
    /// broadcast receiver so `receiver_count() > 0`.
    fn ready_listener() -> (OrderBookListener, tokio::sync::broadcast::Receiver<Arc<InternalMessage>>) {
        let (tx, rx) = tokio::sync::broadcast::channel(32);
        let mut listener = OrderBookListener::new(Some(tx), true, ActiveL2Params::new(), (true, true, true));
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 0);
        (listener, rx)
    }

    #[test]
    fn test_flush_l2_if_due_broadcasts_and_drains_when_due() {
        let (mut listener, mut rx) = ready_listener();
        // A subscriber must want at least one variant shape, else flush is skipped.
        let _guard = listener.active_l2_params().acquire(L2SnapshotParams::new(None, None));
        listener.pending_dirty_l2_coins.insert(Coin::new("BTC"));
        listener.last_l2_broadcast = None; // due (no prior broadcast)

        listener.flush_l2_if_due();

        let msg = rx.try_recv().expect("a Snapshot must be broadcast when due and dirty");
        assert!(matches!(msg.as_ref(), InternalMessage::Snapshot { .. }));
        assert!(listener.pending_dirty_l2_coins.is_empty(), "buffer is drained on flush");
        assert!(listener.last_l2_broadcast.is_some(), "throttle timestamp is set");
    }

    #[test]
    fn test_flush_l2_if_due_noop_inside_throttle_window() {
        let (mut listener, mut rx) = ready_listener();
        listener.pending_dirty_l2_coins.insert(Coin::new("BTC"));
        listener.last_l2_broadcast = Some(Instant::now()); // just fired -> not due

        listener.flush_l2_if_due();

        assert!(rx.try_recv().is_err(), "nothing is broadcast inside the throttle window");
        assert!(!listener.pending_dirty_l2_coins.is_empty(), "buffer is retained when not due");
    }

    #[test]
    fn test_flush_l2_if_due_noop_when_buffer_empty() {
        let (mut listener, mut rx) = ready_listener();
        listener.last_l2_broadcast = None; // due, but nothing dirty

        listener.flush_l2_if_due();

        assert!(rx.try_recv().is_err(), "no broadcast when there are no dirty coins");
    }

    #[test]
    fn test_flush_l2_if_due_noop_when_no_active_variants() {
        // Due + dirty + receiver, but no connection wants any L2 variant.
        let (mut listener, mut rx) = ready_listener();
        listener.pending_dirty_l2_coins.insert(Coin::new("BTC"));
        listener.last_l2_broadcast = None;

        listener.flush_l2_if_due();

        assert!(rx.try_recv().is_err(), "no broadcast when no variant shape is subscribed");
    }

    // ==================== Event helpers ====================

    fn make_l4_order_json(coin: &str, oid: u64) -> serde_json::Value {
        serde_json::json!({
            "coin": coin,
            "side": "B",
            "limitPx": "100.0",
            "sz": "1.0",
            "oid": oid,
            "timestamp": 1000,
            "triggerCondition": "N/A",
            "isTrigger": false,
            "triggerPx": "0.0",
            "children": [],
            "isPositionTpsl": false,
            "reduceOnly": false,
            "orderType": "Limit",
            "origSz": "1.0",
            "tif": "Gtc",
            "cloid": null
        })
    }

    fn make_status_batch(coin: &str, oid: u64, height: u64) -> Batch<NodeDataOrderStatus> {
        serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": height,
            "events": [{
                "time": "2024-01-15T10:30:00.000000000",
                "user": "0x0000000000000000000000000000000000000000",
                "hash": "0xabc",
                "status": "open",
                "order": make_l4_order_json(coin, oid),
            }]
        }))
        .unwrap()
    }

    fn make_diff_batch(coin: &str, oid: u64, height: u64, raw_book_diff: serde_json::Value) -> Batch<NodeDataOrderDiff> {
        serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": height,
            "events": [{
                "user": "0x0000000000000000000000000000000000000000",
                "oid": oid,
                "side": "B",
                "px": "100.0",
                "coin": coin,
                "raw_book_diff": raw_book_diff,
            }]
        }))
        .unwrap()
    }

    /// Feed a paired status + New diff (both halves of an order add) at `height`.
    fn feed_order(listener: &mut OrderBookListener, coin: &str, oid: u64, height: u64) {
        listener.apply_event_batch(
            height,
            EventBatch::Orders(make_status_batch(coin, oid, height)),
            EventSource::OrderStatuses,
        );
        listener.apply_event_batch(
            height,
            EventBatch::BookDiffs(make_diff_batch(coin, oid, height, serde_json::json!({"new": {"sz": "1.0"}}))),
            EventSource::OrderDiffs,
        );
    }

    /// Drain the broadcast receiver until the next Snapshot message; panics if
    /// none was sent.
    fn next_snapshot_msg(
        rx: &mut tokio::sync::broadcast::Receiver<Arc<InternalMessage>>,
    ) -> (HashSet<Coin>, Option<Arc<HashSet<String>>>) {
        while let Ok(msg) = rx.try_recv() {
            if let InternalMessage::Snapshot { dirty, universe, .. } = msg.as_ref() {
                return (dirty.clone(), universe.clone());
            }
        }
        panic!("no Snapshot message was broadcast");
    }

    // ==================== Gapless snapshot handoff (drift fix) ====================

    #[test]
    fn test_startup_events_cached_and_replayed_above_snapshot_height() {
        let (tx, _rx) = tokio::sync::broadcast::channel(32);
        let mut listener = OrderBookListener::new(Some(tx), false, ActiveL2Params::new(), (true, true, true));
        assert!(!listener.is_ready());

        // Events stream in while the snapshot is being generated (book not ready).
        feed_order(&mut listener, "NEW", 1, 200);
        feed_order(&mut listener, "OLD", 2, 100);

        // Snapshot lands at height 150: the height-200 events must be replayed;
        // the height-100 events are already reflected in the snapshot state.
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 150);
        assert!(listener.is_ready());
        let universe = listener.universe();
        assert!(universe.contains("NEW"), "events above the snapshot height must be replayed");
        assert!(!universe.contains("OLD"), "events at/below the snapshot height must not be double-applied");
        assert!(!listener.needs_resync(), "a clean snapshot + replay is in sync");
    }

    #[test]
    fn test_resync_caches_while_serving_and_replays_onto_new_snapshot() {
        let (mut listener, _rx) = ready_listener();
        // Steady state: applied live, not cached.
        feed_order(&mut listener, "BTC", 1, 10);
        assert!(listener.universe().contains("BTC"));

        // A re-sync starts: events keep applying to the live book AND are cached.
        listener.begin_caching();
        feed_order(&mut listener, "ETH", 2, 20);
        assert!(listener.universe().contains("ETH"), "events during a re-sync still apply to the live book");

        // The fresh snapshot (empty, height 15) supersedes the old state; the
        // cached ETH events (height 20 > 15) are replayed on top of it.
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 15);
        let universe = listener.universe();
        assert!(universe.contains("ETH"), "post-snapshot events must survive the re-init");
        assert!(!universe.contains("BTC"), "pre-snapshot state must come from the snapshot alone");
        assert!(!listener.needs_resync());
    }

    #[test]
    fn test_cache_overflow_keeps_book_desynced_until_clean_resync() {
        let (tx, _rx) = tokio::sync::broadcast::channel(32);
        let mut listener = OrderBookListener::new(Some(tx), true, ActiveL2Params::new(), (true, true, true));
        listener.set_cache_event_cap(1);

        feed_order(&mut listener, "AAA", 1, 10); // 2 single-event batches: second one overflows
        assert!(listener.needs_resync(), "cache overflow must mark the book for re-sync");

        // The snapshot that triggered the (overflowed) caching lands below the
        // loss bound: replay is incomplete, the book must stay marked.
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 5);
        assert!(listener.needs_resync(), "incomplete replay must keep the book marked");

        // A later clean fetch whose height covers the loss bound clears it.
        listener.set_cache_event_cap(MAX_CACHED_EVENTS);
        listener.begin_caching();
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 1_000);
        assert!(!listener.needs_resync());
    }

    #[test]
    fn test_mark_desynced_cleared_only_by_covering_snapshot() {
        let (mut listener, _rx) = ready_listener();
        // Establish a known stream height, then lose data at it.
        feed_order(&mut listener, "BTC", 1, 100);
        assert!(!listener.needs_resync());
        listener.mark_desynced("test_reason");
        assert!(listener.needs_resync());

        // A snapshot BELOW the loss bound must not clear the flag: the lost
        // data is above its height and would stay missing forever.
        listener.begin_caching();
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 50);
        assert!(listener.needs_resync(), "a snapshot below the loss height must not clear the desync");

        // A snapshot covering the loss bound (height + margin) clears it.
        listener.begin_caching();
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 1_000);
        assert!(!listener.needs_resync());
    }

    #[test]
    fn test_loss_with_no_observed_height_converges_via_extra_resync() {
        // A loss before ANY height was observed gets a conservative (unknown)
        // bound: the first init keeps the flag, downgrades the bound to real
        // observed heights, and the next covering snapshot clears it.
        let (mut listener, _rx) = ready_listener(); // ready at height 0, nothing observed
        listener.mark_desynced("test_reason");
        listener.begin_caching();
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 1);
        assert!(listener.needs_resync(), "an unknown-height loss is never cleared by the first snapshot");

        listener.begin_caching();
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 2);
        assert!(!listener.needs_resync(), "the downgraded bound lets the next snapshot clear it");
    }

    #[test]
    fn test_backfill_batches_cached_for_replay_never_applied_live() {
        let (mut listener, _rx) = ready_listener(); // ready at height 0, cache None
        listener.begin_caching();

        // Backfill arrives for a coin the live book has never seen.
        listener.cache_backfill_batch(EventBatch::Orders(make_status_batch("ZED", 7, 99)));
        listener.cache_backfill_batch(EventBatch::BookDiffs(make_diff_batch(
            "ZED",
            7,
            99,
            serde_json::json!({"new": {"sz": "1.0"}}),
        )));
        assert!(!listener.universe().contains("ZED"), "backfill must never touch the live book");

        // Snapshot at height 50: the backfilled height-99 events are replayed.
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 50);
        assert!(listener.universe().contains("ZED"), "backfill above the snapshot height must be replayed");
        assert!(!listener.needs_resync());
    }

    #[test]
    fn test_backfill_below_snapshot_height_not_replayed() {
        let (mut listener, _rx) = ready_listener();
        listener.begin_caching();
        listener.cache_backfill_batch(EventBatch::Orders(make_status_batch("ZED", 7, 40)));
        listener.cache_backfill_batch(EventBatch::BookDiffs(make_diff_batch(
            "ZED",
            7,
            40,
            serde_json::json!({"new": {"sz": "1.0"}}),
        )));
        // Snapshot at height 50 already contains everything at height 40.
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 50);
        assert!(!listener.universe().contains("ZED"), "backfill at/below the snapshot height is already covered");
    }

    #[test]
    fn test_late_backfill_with_no_cache_marks_desync() {
        let (mut listener, _rx) = ready_listener(); // cache already consumed by init
        assert!(!listener.needs_resync());
        listener.cache_backfill_batch(EventBatch::Orders(make_status_batch("BTC", 1, 99)));
        assert!(
            listener.needs_resync(),
            "a backfill batch arriving after the replay cache is gone cannot be applied safely"
        );
    }

    #[test]
    fn test_tolerate_drift_suppresses_resync() {
        let (mut listener, _rx) = ready_listener();
        listener.set_tolerate_drift(true);
        // A loss that would normally schedule a re-fetch...
        listener.mark_desynced("pending_cache_cleared");
        assert!(
            !listener.needs_resync(),
            "with tolerate_drift set, a desync must not schedule a snapshot re-fetch"
        );
        // ...and the higher-level late-backfill path stays quiet too.
        listener.cache_backfill_batch(EventBatch::Orders(make_status_batch("BTC", 1, 99)));
        assert!(!listener.needs_resync(), "tolerate_drift must suppress the late-backfill desync path as well");
    }

    // ==================== Per-coin fan-out grouping ====================

    /// One complete match per listed coin: two adjacent fill legs (buyer bid +
    /// seller ask) sharing a `tid`. The pairer reduces each pair to one trade.
    fn make_fills_batch(coins: &[&str], height: u64) -> Batch<NodeDataFill> {
        let leg = |coin: &str, tid: usize, side: &str, crossed: bool, user: &str| {
            serde_json::json!([
                user,
                {
                    "coin": coin, "px": "100.0", "sz": "1.0", "side": side,
                    "time": 1_700_000_000_000_u64, "startPosition": "0", "dir": "Open Long",
                    "closedPnl": "0", "hash": "0xabc", "oid": tid, "crossed": crossed,
                    "fee": "0.5", "tid": tid, "feeToken": "USDC"
                }
            ])
        };
        let mut events: Vec<serde_json::Value> = Vec::new();
        for (i, coin) in coins.iter().enumerate() {
            // Buyer is the taker (bid crossed); legs adjacent, sharing tid = i.
            events.push(leg(coin, i, "B", true, "0x0000000000000000000000000000000000000001"));
            events.push(leg(coin, i, "A", false, "0x0000000000000000000000000000000000000002"));
        }
        serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": height,
            "events": events
        }))
        .unwrap()
    }

    fn make_multi_diff_batch(coins: &[&str], height: u64) -> Batch<NodeDataOrderDiff> {
        let events: Vec<serde_json::Value> = coins
            .iter()
            .enumerate()
            .map(|(i, coin)| {
                serde_json::json!({
                    "user": "0x0000000000000000000000000000000000000000",
                    "oid": i,
                    "side": "B",
                    "px": "100.0",
                    "coin": coin,
                    "raw_book_diff": {"new": {"sz": "1.0"}},
                })
            })
            .collect();
        serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": height,
            "events": events
        }))
        .unwrap()
    }

    #[test]
    fn test_fills_broadcast_grouped_by_coin() {
        let (mut listener, mut rx) = ready_listener();
        listener.apply_event_batch(1, EventBatch::Fills(make_fills_batch(&["BTC", "ETH", "BTC"], 1)), EventSource::Fills);

        let mut found = false;
        while let Ok(msg) = rx.try_recv() {
            if let InternalMessage::Fills { trades_by_coin } = msg.as_ref() {
                assert_eq!(trades_by_coin.len(), 2);
                assert_eq!(trades_by_coin.get("BTC").map(|t| t.trades.len()), Some(2));
                assert_eq!(trades_by_coin.get("ETH").map(|t| t.trades.len()), Some(1));
                // Public schema: aggressing side (buyer taker => "B") + users
                // [buyer, seller], no per-leg `user`.
                let v = serde_json::to_value(&trades_by_coin["ETH"].trades[0]).unwrap();
                assert_eq!(v["side"], "B");
                assert_eq!(v["users"][0], "0x0000000000000000000000000000000000000001");
                assert_eq!(v["users"][1], "0x0000000000000000000000000000000000000002");
                assert!(v.get("user").is_none());
                found = true;
            }
        }
        assert!(found, "a grouped Fills message must be broadcast");
    }

    #[test]
    fn test_fills_pair_across_batches_hft_mode() {
        // HFT mode: each leg is its own single-event batch. The first holds its
        // lone leg; the second completes exactly one trade with the aggressor
        // side taken from `crossed`.
        let leg = |tid: u64, side: &str, crossed: bool, user: &str| -> Batch<NodeDataFill> {
            serde_json::from_value(serde_json::json!({
                "local_time": "2024-01-15T10:30:00.000000000",
                "block_time": "2024-01-15T10:30:00.000000000",
                "block_number": 1,
                "events": [[user, {
                    "coin": "BTC", "px": "100.0", "sz": "1.0", "side": side,
                    "time": 1_700_000_000_000_u64, "startPosition": "0", "dir": "Open Long",
                    "closedPnl": "0", "hash": "0xabc", "oid": tid, "crossed": crossed,
                    "fee": "0.5", "tid": tid, "feeToken": "USDC"
                }]]
            })).unwrap()
        };
        let mut p = TradePairer::default();
        // Seller (ask) crossed => taker => aggressor side "A".
        assert!(p.group(leg(5, "A", true, "0x0000000000000000000000000000000000000002")).is_empty());
        let grouped = p.group(leg(5, "B", false, "0x0000000000000000000000000000000000000001"));
        assert_eq!(grouped["BTC"].trades.len(), 1);
        assert_eq!(serde_json::to_value(&grouped["BTC"].trades[0]).unwrap()["side"], "A");
    }

    #[test]
    fn test_shared_frame_matches_per_connection_serialization() {
        // The wire format is public API: the shared frame must be byte-identical
        // to what the old per-connection serde_json::to_string path produced.
        use crate::types::subscription::ServerResponse;
        let grouped = TradePairer::default().group(make_fills_batch(&["BTC"], 1));
        let ct = grouped.get("BTC").unwrap();
        let expected = serde_json::to_string(&ServerResponse::Trades(Arc::clone(&ct.trades))).unwrap();
        let frame = ct.frame.get_or_serialize(|| ServerResponse::Trades(Arc::clone(&ct.trades)));
        assert_eq!(frame.as_ref(), expected.as_bytes());
        // A second call returns the cached frame without re-serializing.
        let again = ct.frame.get_or_serialize(|| -> ServerResponse { panic!("frame must be cached") });
        assert_eq!(again, frame);
    }

    #[test]
    fn test_l2_frame_cache_builds_once_and_is_byte_identical() {
        // The wire format is public API: the cached frame must be byte-identical
        // to what the old per-connection serde_json::to_string path produced,
        // and a second lookup must NOT re-run the build closure.
        use crate::types::subscription::ServerResponse;
        let cache = L2FrameCache::new();
        let build = || {
            let levels = [vec![crate::types::Level::new("100.0".to_string(), "1.5".to_string(), 2)], Vec::new()];
            let l2_book = L2Book::from_l2_snapshot("BTC".to_string(), levels, 1000, Some(5), None, Some(20));
            let frame = bytes::Bytes::from(serde_json::to_string(&ServerResponse::L2Book(l2_book.clone())).unwrap());
            (42_u64, frame, l2_book)
        };
        let key = || L2FrameKey::new("BTC", Some(5), None, 20);

        let first = cache.get_or_build(key(), build);
        let (_, expected_frame, expected_payload) = build();
        assert_eq!(first.1, expected_frame, "cached frame must match direct serialization byte-for-byte");
        assert_eq!(first.0, 42, "dedup hash is carried through");

        let second = cache.get_or_build(key(), || panic!("the build closure must not run on a cache hit"));
        assert!(Arc::ptr_eq(&first, &second), "the cached Arc is shared");
        assert_eq!(serde_json::to_string(&second.2).unwrap(), serde_json::to_string(&expected_payload).unwrap());

        // A different n_levels is a different payload - distinct cache slot.
        let other = cache.get_or_build(L2FrameKey::new("BTC", Some(5), None, 50), build);
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[test]
    fn test_diffs_broadcast_grouped_by_coin_with_spot_filter() {
        // ready_listener runs with ignore_spot=true: the spot coin's diff must
        // be stripped from the broadcast grouping too.
        let (mut listener, mut rx) = ready_listener();
        listener.apply_event_batch(
            1,
            EventBatch::BookDiffs(make_multi_diff_batch(&["BTC", "@1", "BTC"], 1)),
            EventSource::OrderDiffs,
        );

        let mut found = false;
        while let Ok(msg) = rx.try_recv() {
            if let InternalMessage::L4OrderDiffs { time, height, diffs_by_coin } = msg.as_ref() {
                assert_eq!(*height, 1);
                assert!(*time > 0);
                assert_eq!(diffs_by_coin.len(), 1, "spot diffs are filtered out of the broadcast");
                assert_eq!(diffs_by_coin.get("BTC").map(|d| d.diffs.len()), Some(2));
                found = true;
            }
        }
        assert!(found, "a grouped L4OrderDiffs message must be broadcast");
    }

    #[test]
    fn test_all_spot_diff_batch_broadcasts_nothing() {
        let (mut listener, mut rx) = ready_listener(); // ignore_spot=true
        listener.apply_event_batch(1, EventBatch::BookDiffs(make_multi_diff_batch(&["@1"], 1)), EventSource::OrderDiffs);
        while let Ok(msg) = rx.try_recv() {
            assert!(
                !matches!(msg.as_ref(), InternalMessage::L4OrderDiffs { .. }),
                "an entirely-filtered batch must not produce an empty broadcast"
            );
        }
    }

    #[test]
    fn test_statuses_broadcast_grouped_by_coin() {
        let (mut listener, mut rx) = ready_listener();
        listener.apply_event_batch(7, EventBatch::Orders(make_status_batch("BTC", 1, 7)), EventSource::OrderStatuses);

        let mut found = false;
        while let Ok(msg) = rx.try_recv() {
            if let InternalMessage::L4OrderStatuses { height, statuses_by_coin, .. } = msg.as_ref() {
                assert_eq!(*height, 7);
                assert_eq!(statuses_by_coin.get("BTC").map(|s| s.statuses.len()), Some(1));
                found = true;
            }
        }
        assert!(found, "a grouped L4OrderStatuses message must be broadcast");
    }

    // ==================== Parse / apply split ====================

    #[test]
    fn test_parse_event_line_valid_malformed_empty() {
        let line = r#"{"local_time":"2024-01-15T10:30:00.000000000","block_time":"2024-01-15T10:30:00.000000000","block_number":7,"events":[]}"#;
        let parsed = parse_event_line(line, EventSource::OrderDiffs);
        assert!(matches!(parsed, Some((7, EventBatch::BookDiffs(_)))));
        let parsed = parse_event_line(line, EventSource::OrderStatuses);
        assert!(matches!(parsed, Some((7, EventBatch::Orders(_)))));
        assert!(parse_event_line("", EventSource::OrderDiffs).is_none());
        assert!(parse_event_line("not json", EventSource::Fills).is_none());
        assert!(parse_event_line("{\"truncated\":", EventSource::OrderDiffs).is_none());
    }

    // ==================== Dirty set + universe in Snapshot broadcasts ====================

    #[test]
    fn test_flush_broadcasts_dirty_set_and_universe_on_coin_set_change() {
        let (mut listener, mut rx) = ready_listener();
        let _guard = listener.active_l2_params().acquire(L2SnapshotParams::new(None, None));

        // First flush after BTC appears: dirty contains BTC, universe included.
        feed_order(&mut listener, "BTC", 1, 1);
        listener.last_l2_broadcast = None;
        listener.flush_l2_if_due();
        let (dirty, universe) = next_snapshot_msg(&mut rx);
        assert!(dirty.contains("BTC"));
        let universe = universe.expect("universe must be included when the coin set changes");
        assert!(universe.contains("BTC"));

        // The same coin changes again: dirty yes, but the coin set is unchanged
        // so no universe is attached.
        let update = make_diff_batch("BTC", 1, 2, serde_json::json!({"update": {"origSz": "1.0", "newSz": "2.0"}}));
        listener.apply_event_batch(2, EventBatch::BookDiffs(update), EventSource::OrderDiffs);
        listener.last_l2_broadcast = None;
        listener.flush_l2_if_due();
        let (dirty, universe) = next_snapshot_msg(&mut rx);
        assert!(dirty.contains("BTC"));
        assert!(universe.is_none(), "universe is only rebuilt when the coin set changes");
    }

    #[test]
    fn test_active_l2_params_guard_decrements_on_drop() {
        let reg = ActiveL2Params::new();
        let p = L2SnapshotParams::new(Some(5), Some(2));
        {
            let _g1 = reg.acquire(p);
            let _g2 = reg.acquire(p); // refcount 2
            assert!(reg.snapshot().contains(&p));
        } // both guards dropped here
        assert!(reg.snapshot().is_empty(), "shape removed once refcount hits zero");
    }

    #[test]
    fn test_active_l2_params_partial_drop_keeps_shape() {
        let reg = ActiveL2Params::new();
        let p = L2SnapshotParams::new(None, None);
        let g1 = reg.acquire(p);
        {
            let _g2 = reg.acquire(p);
        } // one guard dropped, refcount back to 1
        assert!(reg.snapshot().contains(&p), "shape still referenced by g1");
        drop(g1);
        assert!(reg.snapshot().is_empty());
    }
}
