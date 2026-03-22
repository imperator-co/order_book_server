use crate::{
    listeners::order_book::state::OrderBookState,
    metrics::{
        BBO_BROADCAST_LATENCY, EVENT_PROCESSING_LATENCY, EVENTS_PROCESSED_TOTAL, FILE_EVENTS_TOTAL,
        FILE_LINES_PARSED_TOTAL, L2_BROADCAST_LATENCY, ORDERBOOK_COINS_COUNT, ORDERBOOK_HEIGHT, ORDERBOOK_ORDERS_TOTAL,
        ORDERBOOK_TIME_MS, PARSE_ERRORS_TOTAL, PENDING_DIFFS_CACHE, PENDING_ORDERS_CACHE,
    },
    order_book::{
        Coin, Px, Snapshot, Sz,
        multi_book::{Snapshots, load_snapshots_from_cli_json},
    },
    prelude::*,
    types::{
        L4Order,
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
    time::{Instant, sleep},
};
use utils::{EventBatch, SnapshotConfig, get_visor_path, process_rmp_file};

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
        // CRITICAL: Start caching BEFORE generating snapshot
        // This ensures we don't miss any events during snapshot generation
        let _state = {
            let mut listener = listener.lock().await;
            listener.begin_caching();
            listener.clone_state()
        };

        // Now generate snapshot - any events during this time are cached
        let visor_path = get_visor_path(&snapshot_config);
        let res = match process_rmp_file(&snapshot_config).await {
            Ok(output_fln) => {
                let snapshot =
                    load_snapshots_from_cli_json::<InnerL4Order, (Address, L4Order)>(&output_fln, &visor_path).await;
                info!("Snapshot fetched");
                // sleep to let some updates build up.
                sleep(Duration::from_secs(1)).await;
                let _cache = {
                    let mut listener = listener.lock().await;
                    listener.take_cache()
                };
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        info!("Snapshot loaded at height {}", height);
                        // Always reinitialize from snapshot to get fresh, accurate orderbook
                        // This corrects any drift from missed streaming updates
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
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    // Throttle L2 broadcasts to prevent flooding clients
    last_l2_broadcast: Option<Instant>,
}

impl OrderBookListener {
    pub(crate) const fn new(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, ignore_spot: bool) -> Self {
        Self {
            ignore_spot,
            order_book_state: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            last_l2_broadcast: None,
        }
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // take the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("Initializing from snapshot at height {}", height);
        // On initial startup, just trust the snapshot and start fresh
        // Don't try to apply cached updates - they may have gaps
        let new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        self.order_book_state = Some(new_order_book);
        // Clear any stale cache
        self.fetched_snapshot_cache = None;
        info!("Order book ready at height {}", height);
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }
}

impl OrderBookListener {
    /// HFT version of process_data - doesn't skip first line errors since we're processing complete JSON lines
    pub(crate) fn process_data_hft(&mut self, line: String, event_source: EventSource) -> Result<()> {
        // Count events for debugging
        static HFT_EVENT_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let count = HFT_EVENT_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count % 1000 == 0 {
            info!("process_data_hft event #{}, source: {}, line_len: {}", count, event_source, line.len());
        }

        if line.is_empty() {
            return Ok(());
        }

        // Parse the batch
        let res = match event_source {
            EventSource::Fills => sonic_rs::from_str::<Batch<NodeDataFill>>(&line).map(|batch| {
                let height = batch.block_number();
                (height, EventBatch::Fills(batch))
            }),
            EventSource::OrderStatuses => sonic_rs::from_str(&line)
                .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
            EventSource::OrderDiffs => sonic_rs::from_str(&line)
                .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
        };

        let (height, event_batch) = match res {
            Ok(data) => data,
            Err(err) => {
                // Log ALL parse errors for debugging
                let err_source_label = match event_source {
                    EventSource::Fills => "fills",
                    EventSource::OrderStatuses => "orders",
                    EventSource::OrderDiffs => "diffs",
                };
                PARSE_ERRORS_TOTAL.with_label_values(&[err_source_label]).inc();
                static PARSE_ERR_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let err_count = PARSE_ERR_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if err_count % 1000 == 0 {
                    error!("parse error #{}: {}, source: {}, line_len: {}", err_count, err, event_source, line.len());
                }
                return Ok(()); // Skip this line but don't fail
            }
        };

        // Log successful parses periodically
        static PARSE_OK_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let ok_count = PARSE_OK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Record file watcher metrics
        let source_label = match event_source {
            EventSource::Fills => "fills",
            EventSource::OrderStatuses => "orders",
            EventSource::OrderDiffs => "diffs",
        };
        FILE_EVENTS_TOTAL.with_label_values(&[source_label]).inc();
        FILE_LINES_PARSED_TOTAL.with_label_values(&[source_label]).inc_by(line.len() as u64);
        let process_start = Instant::now();

        if ok_count % 10_000 == 0 {
            info!("parse OK #{}: height={}, source={}", ok_count, height, event_source);
        }

        if height % 100 == 0 {
            info!("{event_source} block: {height}");
        }

        // HFT mode: Process events DIRECTLY without block-level synchronization
        // This is arbor's key insight - process independently with order-level caching
        let changed_coins: HashSet<Coin> = if let Some(state) = self.order_book_state.as_mut() {
            let result = match event_batch {
                EventBatch::Orders(batch) => {
                    // Broadcast L4 order statuses for L4Book subscribers
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        let batch_clone = batch.clone();
                        tokio::spawn(async move {
                            let msg = Arc::new(InternalMessage::L4OrderStatuses { batch: batch_clone });
                            drop(tx.send(msg));
                        });
                    }
                    // Count order status events
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["orders"]).inc();
                    // Apply OrderStatuses directly using HFT method
                    state.apply_order_statuses_hft(batch)
                }
                EventBatch::BookDiffs(batch) => {
                    // Broadcast L4 order diffs for L4Book subscribers
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        let batch_clone = batch.clone();
                        tokio::spawn(async move {
                            let msg = Arc::new(InternalMessage::L4OrderDiffs { batch: batch_clone });
                            drop(tx.send(msg));
                        });
                    }
                    // Count book diff events
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["diffs"]).inc();
                    // Apply OrderDiffs directly using HFT method
                    state.apply_order_diffs_hft(batch)
                }
                EventBatch::Fills(batch) => {
                    // Count fill events
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["fills"]).inc();

                    // Broadcast fills immediately
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            drop(tx.send(snapshot));
                        });
                    }
                    Ok(HashSet::new())
                }
            };

            match result {
                Ok(coins) => coins,
                Err(err) => {
                    self.order_book_state = None;
                    return Err(err);
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

                // Cleanup stale pending entries to prevent unbounded memory growth
                state.cleanup_stale_pending();

                info!(
                    "State progress #{}: height={}, pending_statuses={}, pending_diffs={}",
                    sc,
                    state.height(),
                    state.pending_order_statuses_count(),
                    state.pending_new_diffs_count()
                );
            }
        }

        // Fast BBO broadcast - ONLY for coins that changed!
        // No throttle needed since we only compute BBO for changed coins (usually 1-2)
        if !changed_coins.is_empty() {
            if let Some(state) = &self.order_book_state {
                let bbo_start = Instant::now();
                let (time, bbos) = state.get_bbos_for_coins(&changed_coins);
                if let Some(tx) = &self.internal_message_tx {
                    // Count fast BBO broadcasts
                    static BBO_BROADCAST_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                    let bc = BBO_BROADCAST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if bc % 1000 == 0 {
                        info!("Fast BBO broadcast #{} at time {} for {} coins", bc, time, changed_coins.len());
                    }

                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let msg = Arc::new(InternalMessage::BboUpdate { bbos, time });
                        drop(tx.send(msg));
                    });
                }
                BBO_BROADCAST_LATENCY.observe(bbo_start.elapsed().as_secs_f64());
            }
        }

        // Throttled L2 snapshot broadcast for L2Book subscribers
        // l2_snapshots_uncached() is expensive, so limit to 100 broadcasts/sec max (10ms interval)
        let should_broadcast_l2 =
            self.last_l2_broadcast.map(|t| t.elapsed() >= Duration::from_millis(10)).unwrap_or(true);

        if should_broadcast_l2 {
            if let Some(state) = &self.order_book_state {
                let l2_start = Instant::now();
                let (time, l2_snapshots) = state.l2_snapshots_uncached();
                if let Some(tx) = &self.internal_message_tx {
                    self.last_l2_broadcast = Some(Instant::now());

                    // Count L2 broadcasts
                    static L2_BROADCAST_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                    let bc = L2_BROADCAST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if bc % 100 == 0 {
                        info!("L2 broadcast #{} at time {}", bc, time);
                    }

                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let msg = Arc::new(InternalMessage::Snapshot { l2_snapshots, time });
                        drop(tx.send(msg));
                    });
                }
                L2_BROADCAST_LATENCY.observe(l2_start.elapsed().as_secs_f64());
            }
        }
        Ok(())
    }
}

pub(crate) struct L2Snapshots(HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>);

impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>> {
        &self.0
    }
}

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    Snapshot {
        l2_snapshots: L2Snapshots,
        time: u64,
    },
    Fills {
        batch: Batch<NodeDataFill>,
    },
    /// Fast BBO-only broadcast path - bypasses expensive L2 snapshot computation
    BboUpdate {
        bbos: HashMap<Coin, (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>)>,
        time: u64,
    },
    /// HFT L4 streaming - order diffs without waiting for status pairing
    L4OrderDiffs {
        batch: Batch<NodeDataOrderDiff>,
    },
    /// HFT L4 streaming - order statuses without waiting for diff pairing
    L4OrderStatuses {
        batch: Batch<NodeDataOrderStatus>,
    },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
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
    let dir = config.data_dir.clone().unwrap_or_else(|| dirs::home_dir().expect("Could not find home directory"));

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

    // Start parallel file watchers (crossbeam channel)
    let (crossbeam_rx, _handles, _last_os, _last_fills, _last_diffs) = parallel::start_parallel_file_watchers(dir);

    // Bridge crossbeam to tokio mpsc
    let (tokio_tx, mut tokio_rx) = unbounded_channel::<parallel::FileEvent>();

    // Spawn a blocking task to bridge crossbeam -> tokio
    tokio::task::spawn_blocking(move || {
        info!("Bridge task started");
        let mut event_count = 0u64;
        loop {
            match crossbeam_rx.recv() {
                Ok(event) => {
                    event_count += 1;
                    if event_count % 100_000 == 0 {
                        info!("Bridge: received {} events", event_count);
                    }
                    if tokio_tx.send(event).is_err() {
                        error!("Bridge: tokio channel closed");
                        break;
                    }
                }
                Err(_) => {
                    error!("Bridge: crossbeam channel closed");
                    break;
                }
            }
        }
    });

    // Snapshot fetch channel
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = tokio::time::interval_at(start, Duration::from_secs(10));
    let mut snapshot_fetch_pending = false;

    info!("Main event loop starting");

    loop {
        tokio::select! {
            biased;

            // Process events from file watchers (via bridge)
            Some(event) = tokio_rx.recv() => {
                match event {
                    parallel::FileEvent::OrderDiff(line) => {
                        // Process OrderDiff immediately - this is the BBO-critical path
                        if let Err(err) = listener.lock().await.process_data_hft(line, EventSource::OrderDiffs) {
                            error!("OrderDiff error: {err}");
                        }
                    }
                    parallel::FileEvent::OrderStatus(line) => {
                        // OrderStatuses are less latency-critical
                        if let Err(err) = listener.lock().await.process_data_hft(line, EventSource::OrderStatuses) {
                            error!("OrderStatus error: {err}");
                        }
                    }
                    parallel::FileEvent::Fill(line) => {
                        // Fills are for trade data, not BBO
                        if let Err(err) = listener.lock().await.process_data_hft(line, EventSource::Fills) {
                            error!("Fill error: {err}");
                        }
                    }
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
                        return Err(format!("Abci state reading error: {err}").into());
                    }
                    Some(Ok(())) => {}
                }
            }

            // Periodic snapshot fetch (initial only)
            _ = ticker.tick() => {
                let is_ready = listener.lock().await.is_ready();
                info!("Ticker: is_ready={}, snapshot_fetch_pending={}", is_ready, snapshot_fetch_pending);
                if !is_ready && !snapshot_fetch_pending {
                    snapshot_fetch_pending = true;
                    let listener = listener.clone();
                    let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                    fetch_snapshot(snapshot_config.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
                }
            }
        }
    }
}
