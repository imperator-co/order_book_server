use crate::{
    listeners::order_book::{
        ActiveL2Params, ActiveSubGuard, ActiveSubs, CoinBbo, InternalMessage, L2FrameCache, L2FrameKey, L2ParamGuard,
        L2SnapshotParams, OrderBookListener, hl_listen_hft,
    },
    metrics::{
        BBO_CHANGES_TOTAL, BROADCAST_RECEIVERS, BROADCASTS_TOTAL, CHANNEL_DROPS_TOTAL, CHANNEL_LAG,
        LAST_EVENT_APPLIED_MS, MESSAGES_SENT_TOTAL, ORDERBOOK_HEIGHT, ORDERBOOK_READY, WS_CONNECTIONS_ACTIVE,
        WS_CONNECTIONS_TOTAL, WS_SEND_ERRORS_TOTAL,
    },
    order_book::{Coin, PxBand, Snapshot},
    prelude::*,
    types::{
        Bbo, L2Book, L4Book, L4BookUpdates, L4Order,
        inner::InnerLevel,
        subscription::{ClientMessage, DEFAULT_LEVELS, OrderUpdate, ServerResponse, Subscription, SubscriptionManager},
    },
};
use axum::{Router, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::select;
use tokio::{
    net::TcpListener,
    sync::{
        Mutex,
        broadcast::{Sender, channel},
        mpsc,
    },
};
use yawc::{FrameView, OpCode, WebSocket};

use crate::ServerConfig;

/// Per-(coin, params) cached L2 broadcast. `hash` is used for change-based dedup;
/// `payload` is resent verbatim (with refreshed `time`) when the heartbeat fires,
/// and is only stored when the L2 heartbeat is enabled (default off) - the
/// change-driven sends use the broadcast's shared frames instead.
struct L2Entry {
    hash: u64,
    last_sent: Instant,
    payload: Option<L2Book>,
}

/// Raw fixed-point (px, sz) pairs for the best bid and ask. Comparing these
/// for dedup avoids the four String allocations the old tuple cost per BBO
/// per connection per change-check.
type BboKey = (Option<(u64, u64)>, Option<(u64, u64)>);

/// Per-coin cached BBO broadcast. `tuple` is used for change-based dedup;
/// `payload` is resent verbatim (with refreshed `time`) when the heartbeat fires,
/// and is only stored when the BBO heartbeat is enabled (default off) - the
/// change-driven sends use the broadcast's shared frames instead.
struct BboEntry {
    tuple: BboKey,
    last_sent: Instant,
    payload: Option<Bbo>,
}

/// Per-subscription dedup/heartbeat cache key. `n_levels` MUST be part of the
/// key: two subscriptions on the same (coin, nSigFigs, mantissa) but different
/// nLevels produce different payloads, and sharing one entry made their hashes
/// ping-pong (dedup defeated, both resent every broadcast) while unsubscribing
/// one silently dropped the other's cache. Validation rejects an explicit
/// `nLevels == DEFAULT_LEVELS`, so `unwrap_or(DEFAULT_LEVELS)` cannot collide
/// with an explicit value.
fn l2_cache_key(coin: &str, n_sig_figs: Option<u32>, mantissa: Option<u64>, n_levels: Option<usize>) -> String {
    format!(
        "{}:{}:{}:{}",
        coin,
        n_sig_figs.unwrap_or(0),
        mantissa.unwrap_or(0),
        n_levels.unwrap_or(DEFAULT_LEVELS)
    )
}

/// Build a tokio interval that fires often enough to drive both heartbeats with
/// at most half the configured period of drift. Returns None when both heartbeats are disabled.
fn build_heartbeat_ticker(l2book_heartbeat_ms: u64, bbo_heartbeat_ms: u64) -> Option<tokio::time::Interval> {
    let enabled = [l2book_heartbeat_ms, bbo_heartbeat_ms].into_iter().filter(|&ms| ms > 0).min()?;
    let tick_ms = (enabled / 2).max(50).min(500);
    let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    Some(interval)
}

/// Await the next heartbeat tick, or pend forever when no heartbeat is configured.
async fn heartbeat_tick(ticker: &mut Option<tokio::time::Interval>) {
    match ticker {
        Some(t) => {
            t.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

pub async fn run_websocket_server(config: ServerConfig) -> Result<()> {
    // One channel multiplexes every event (L2/BBO/L4/fills) to all connections.
    // In `--stream-with-block-info` mode the listener emits one message per node
    // event (order statuses/diffs: thousands/s), so depth 32 drained in
    // milliseconds and any consumer jitter tripped `RecvError::Lagged`, evicting
    // messages across all channels. Even with batch draining and shared frames,
    // a single trades subscriber lost ~71% of trades on a busy mainnet window at
    // depth 32 vs ~1.7% at 16384, in the same conditions. 16384 gives seconds of
    // headroom; each slot is one Arc pointer and a persistently-slow consumer
    // still sheds via Lagged, so memory stays bounded.
    // TODO: this is a mitigation, not a fix — loss is still cross-channel
    // (residual ~1.7% above) and every connection is woken for the full L4
    // firehose. Splitting into per-message-type channels, subscribed per
    // connection as needed, would decouple loss domains and wakeup cost.
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(16384);

    // Market filter flags from config
    let market_filter = (config.include_perps, config.include_spot, config.include_hip3);
    let ignore_spot = !config.include_spot; // For OrderBookListener (legacy)
    let compression_level = config.compression_level;

    // Shared registry of L2 variant shapes any live connection wants. Cloned into
    // the listener (read at flush time) and handed to each connection (which
    // acquires/releases refcounted guards on subscribe/unsubscribe + disconnect).
    let active_l2_params = ActiveL2Params::new();

    // Resolve data directory
    // Central task: listen to messages and forward them for distribution
    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        let mut listener =
            OrderBookListener::new(Some(internal_message_tx), ignore_spot, active_l2_params.clone(), market_filter);
        listener.set_tolerate_drift(config.no_resync);
        // BBO-only deployments are sized for a lightweight envelope and have no
        // consumers for /untriggeredOrders - skip maintaining the side table.
        listener.set_track_untriggered(!config.bbo_only);
        listener
    };
    let listener = Arc::new(Mutex::new(listener));
    let listener_task = {
        let listener = listener.clone();
        let config = config.clone();
        tokio::spawn(async move {
            info!("Starting HFT-optimized listener");
            let result = hl_listen_hft(listener, config).await;
            if let Err(err) = result {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        })
    };

    let websocket_opts = websocket_options(compression_level);

    let start_time = Instant::now();

    // Shared L4 snapshot body cache (GET /l4Book + WS l4Book subscribe).
    let snapshot_build_permits = Arc::new(tokio::sync::Semaphore::new(L4_SNAPSHOT_BUILD_PERMITS));
    let l4_cache = Arc::new(L4SnapshotCache::new(snapshot_build_permits.clone()));
    // Separate cache for GET /untriggeredOrders bodies (distinct key space),
    // sharing the build permits so the total number of under-lock snapshot
    // builds queued ahead of ingest stays bounded by L4_SNAPSHOT_BUILD_PERMITS.
    let untriggered_cache = Arc::new(L4SnapshotCache::new(snapshot_build_permits));

    let app: Router = Router::new()
        .route(
            "/ws",
            get({
                let internal_message_tx = internal_message_tx.clone();
                let bbo_only = config.bbo_only;
                let l2book_heartbeat_ms = config.l2book_heartbeat_ms;
                let bbo_heartbeat_ms = config.bbo_heartbeat_ms;
                let listener = listener.clone();
                let l4_cache = l4_cache.clone();
                move |ws_upgrade| async move {
                    ws_handler(
                        ws_upgrade,
                        internal_message_tx.clone(),
                        listener.clone(),
                        l4_cache.clone(),
                        bbo_only,
                        l2book_heartbeat_ms,
                        bbo_heartbeat_ms,
                        websocket_opts,
                    )
                }
            }),
        )
        .route(
            "/l4Book",
            get({
                let listener = listener.clone();
                let l4_cache = l4_cache.clone();
                move |query, headers| l4_snapshot_handler(query, headers, listener.clone(), l4_cache.clone())
            }),
        )
        .route(
            "/untriggeredOrders",
            get({
                let listener = listener.clone();
                let untriggered_cache = untriggered_cache.clone();
                move |query, headers| {
                    untriggered_orders_handler(query, headers, listener.clone(), untriggered_cache.clone())
                }
            }),
        )
        .route(
            "/health",
            get(move || async move {
                // Lock-free on purpose: this endpoint used to take the listener
                // lock, so a long ingest hold (resync install, l4Book snapshot
                // storm) made the node look dead to health checks exactly when
                // it was busiest - and load balancers then stampeded clients
                // onto the other node. Reads only atomic gauges now.
                //
                // `stale` means the book is installed but no event batch has
                // been applied recently: subscriptions are acked and the socket
                // is live, yet clients receive no data. The threshold sits well
                // above block cadence and normal commit pauses, and well below
                // the 120s watcher stall alarm.
                const HEALTH_STALE_AFTER_MS: i64 = 15_000;
                let is_ready = ORDERBOOK_READY.get() == 1;
                let last_event_ms = LAST_EVENT_APPLIED_MS.get();
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                // -1 = no batch applied yet (also reported while initializing).
                let age_ms = if last_event_ms > 0 { (now_ms - last_event_ms).max(0) } else { -1 };
                let status = if !is_ready {
                    "initializing"
                } else if age_ms < 0 || age_ms > HEALTH_STALE_AFTER_MS {
                    "stale"
                } else {
                    "ready"
                };
                let uptime_secs = start_time.elapsed().as_secs();
                let height = ORDERBOOK_HEIGHT.get();
                let connections = WS_CONNECTIONS_ACTIVE.get();
                let body = format!(
                    r#"{{"status":"{status}","uptime_seconds":{uptime_secs},"height":{height},"connections":{connections},"last_event_age_ms":{age_ms}}}"#,
                );
                axum::response::Response::builder().header("content-type", "application/json").body(body).unwrap()
            }),
        );

    let tcp_listener = TcpListener::bind(&config.address).await?;
    info!("WebSocket server running at ws://{}", config.address);

    tokio::select! {
        result = axum::serve(NoDelayListener(tcp_listener), app) => {
            if let Err(err) = result {
                error!("Server fatal error: {err}");
                std::process::exit(2);
            }
        }
        // hl_listen_hft loops forever and exits the process itself on a fatal
        // Err; reaching this arm means the task panicked or was aborted. The
        // old fire-and-forget spawn left the server up with a dead feed.
        join = listener_task => {
            error!("Listener task exited unexpectedly: {join:?}");
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Negotiate permessage-deflate only when a nonzero level is configured.
/// yawc's `with_compression_level` always enables the extension (level 0
/// means "stored blocks", not "off"), so an unconditional call runs deflate
/// per frame PER CONNECTION even at level 0 - fan-out CPU scaling with
/// subscriber count for zero bandwidth win. `Options::default()` leaves
/// compression None, declining the extension entirely.
fn websocket_options(compression_level: u32) -> yawc::Options {
    if compression_level > 0 {
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level))
    } else {
        yawc::Options::default()
    }
}

/// `TcpListener` wrapper that sets `TCP_NODELAY` on every accepted socket.
/// Without it, Nagle's algorithm can delay small frames (BBO updates are a few
/// hundred bytes) by up to an RTT while an unacked segment is outstanding.
struct NoDelayListener(TcpListener);

impl axum::serve::Listener for NoDelayListener {
    type Io = tokio::net::TcpStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        // Delegate to TcpListener's impl (it retries transient accept errors).
        let (stream, addr) = axum::serve::Listener::accept(&mut self.0).await;
        if let Err(err) = stream.set_nodelay(true) {
            log::warn!("failed to set TCP_NODELAY on {addr}: {err}");
        }
        (stream, addr)
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

#[allow(clippy::too_many_arguments)]
fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    l4_cache: Arc<L4SnapshotCache>,
    bbo_only: bool,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
    websocket_opts: yawc::Options,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    // Reject malformed WS handshakes cleanly. The previous `.unwrap()` would panic
    // inside the axum handler task and dump a backtrace per request.
    let (resp, fut) = match incoming.upgrade(websocket_opts) {
        Ok(pair) => pair,
        Err(err) => {
            log::warn!("rejecting malformed websocket upgrade: {err}");
            return (axum::http::StatusCode::BAD_REQUEST, "invalid websocket upgrade").into_response();
        }
    };
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, internal_message_tx, listener, l4_cache, bbo_only, l2book_heartbeat_ms, bbo_heartbeat_ms)
            .await;
    });

    resp.into_response()
}

#[allow(clippy::too_many_arguments)]
async fn handle_socket(
    socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    l4_cache: Arc<L4SnapshotCache>,
    bbo_only: bool,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
) {
    // Track connection metrics
    WS_CONNECTIONS_ACTIVE.inc();
    WS_CONNECTIONS_TOTAL.inc();

    // Use a guard to decrement active connections when this function exits
    struct ConnectionGuard;
    impl Drop for ConnectionGuard {
        fn drop(&mut self) {
            WS_CONNECTIONS_ACTIVE.dec();
            BROADCAST_RECEIVERS.dec();
        }
    }
    let _connection_guard = ConnectionGuard;

    let mut internal_message_rx = internal_message_tx.subscribe();
    BROADCAST_RECEIVERS.set(internal_message_tx.receiver_count() as i64);
    // One lock hold for all setup state (previously four separate acquisitions:
    // during a reconnect storm every new connection queued four times behind
    // the FIFO-fair listener lock, exactly when it was most contended).
    let (is_ready, mut universe, active_l2_params, active_subs) = {
        let listener_state = listener.lock().await;
        (listener_state.is_ready(), listener_state.universe(), listener_state.active_l2_params(), listener_state.active_subs())
    };
    let mut manager = SubscriptionManager::default();
    // `universe` above: market-filtered universe for subscription validation.
    // Refreshed from Snapshot broadcasts (Arc-shared, built once in the
    // listener) whenever the coin set changes.
    // Per-(coin,params) cache for L2 dedup + heartbeat resend (key = "<coin>:<n_sig_figs>:<mantissa>")
    let mut last_l2: HashMap<String, L2Entry> = HashMap::new();
    // Per-coin cache for BBO dedup + heartbeat resend
    let mut last_bbo: HashMap<String, BboEntry> = HashMap::new();
    // Parsed orderUpdates user addresses, so the hot broadcast path doesn't
    // re-parse the hex string per message. Bounded by the subscription cap.
    let mut user_addrs: HashMap<String, alloy::primitives::Address> = HashMap::new();
    // Shared L2 variant registry + this connection's refcount guards (one per variant
    // shape it subscribes to). Dropping the map on disconnect releases every guard,
    // so cleanup is robust to abnormal disconnects.
    let mut l2_param_guards: HashMap<L2SnapshotParams, L2ParamGuard> = HashMap::new();
    // Per-family subscription counts (l4/trades/bbo): the listener skips the
    // per-event grouping+broadcast work for families with zero subscribers.
    // One guard set per live subscription; dropping the map on disconnect
    // releases everything, mirroring l2_param_guards.
    let mut sub_guards: HashMap<Subscription, Vec<ActiveSubGuard>> = HashMap::new();

    // Split the socket into an owned write half (driven by the writer task) and
    // a read half polled by this session task. Every send enqueues onto a
    // bounded queue instead of awaiting the TCP write inline, so a slow client
    // can never keep this task from polling the socket - which is exactly what
    // used to stop BOTH application pongs (handled here) and protocol pongs
    // (flushed by yawc only while the socket is polled): one under-window
    // client made a broadcast iteration take up to 256 x 5s without a single
    // poll of the read side.
    let (sink, mut ws_read) = socket.split();
    let (data_tx, data_rx) = mpsc::channel(OUTBOUND_QUEUE_DEPTH);
    let (pong_tx, pong_rx) = mpsc::channel(PONG_QUEUE_DEPTH);
    let outbound = Outbound { data_tx, pong_tx };
    let mut writer = tokio::spawn(write_task(sink, data_rx, pong_rx));

    if is_ready {
        run_session(
            &mut ws_read,
            &outbound,
            &mut internal_message_rx,
            &mut manager,
            &mut universe,
            &mut last_l2,
            &mut last_bbo,
            &mut user_addrs,
            &active_l2_params,
            &mut l2_param_guards,
            &active_subs,
            &mut sub_guards,
            &listener,
            &l4_cache,
            bbo_only,
            l2book_heartbeat_ms,
            bbo_heartbeat_ms,
        )
        .await;
    } else {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        let _unused = outbound.send_message(msg).await;
    }

    // Writer shutdown: dropping the queue senders lets write_task drain what
    // it already accepted, then exit and run the close handshake. Bound the
    // drain so a trickle-reading client can't pin the task (and the connection
    // gauge) open indefinitely.
    drop(outbound);
    if tokio::time::timeout(WS_SEND_TIMEOUT, &mut writer).await.is_err() {
        writer.abort();
    }
}

/// A connection's session loop: broadcast fan-out, heartbeats, and inbound
/// client messages. Returns when the client disconnects, an outbound queue
/// send fails (writer dead or queue wedged), or the broadcast receiver closes.
#[allow(clippy::too_many_arguments)]
async fn run_session(
    ws_read: &mut futures_util::stream::SplitStream<WebSocket>,
    outbound: &Outbound,
    internal_message_rx: &mut tokio::sync::broadcast::Receiver<Arc<InternalMessage>>,
    manager: &mut SubscriptionManager,
    universe: &mut Arc<HashSet<String>>,
    last_l2: &mut HashMap<String, L2Entry>,
    last_bbo: &mut HashMap<String, BboEntry>,
    user_addrs: &mut HashMap<String, alloy::primitives::Address>,
    active_l2_params: &ActiveL2Params,
    l2_param_guards: &mut HashMap<L2SnapshotParams, L2ParamGuard>,
    active_subs: &ActiveSubs,
    sub_guards: &mut HashMap<Subscription, Vec<ActiveSubGuard>>,
    listener: &Arc<Mutex<OrderBookListener>>,
    l4_cache: &Arc<L4SnapshotCache>,
    bbo_only: bool,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
) {
    // Optional heartbeat ticker. We tick at min(enabled_heartbeats)/2 (clamped to [50, 500] ms)
    // so each subscription's last-sent timestamp can drift at most half a heartbeat from the configured value.
    let mut heartbeat_ticker = build_heartbeat_ticker(l2book_heartbeat_ms, bbo_heartbeat_ms);
    let l2_hb = if l2book_heartbeat_ms > 0 { Some(Duration::from_millis(l2book_heartbeat_ms)) } else { None };
    let bbo_hb = if bbo_heartbeat_ms > 0 { Some(Duration::from_millis(bbo_heartbeat_ms)) } else { None };

    // `alive` flips to false the moment any outbound send returns false (writer
    // dead or queue wedged past the timeout). The outer loop checks it at every
    // iteration boundary so a doomed client is dropped instead of looping.
    let mut alive = true;
    // Set after a broadcast-channel lag: a dropped Snapshot message may have
    // carried dirty coins this connection never saw, so the next Snapshot must
    // re-evaluate every subscription instead of trusting the dirty-set skip.
    let mut force_full_l2 = false;
    while alive {
        select! {
            recv_result = internal_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        match msg.as_ref() {
                            InternalMessage::Snapshot{ l2_snapshots, time, dirty, universe: new_universe, l2_frames } => {
                                if let Some(u) = new_universe {
                                    *universe = Arc::clone(u);
                                }
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    // Skip BBO subs here - they get fast updates via BboUpdate
                                    if !matches!(sub, Subscription::Bbo { .. }) {
                                        alive &= send_ws_data_from_snapshot(outbound, sub, l2_snapshots.as_ref(), *time, last_l2, dirty, force_full_l2, l2_frames, l2_hb.is_some()).await;
                                    }
                                }
                                force_full_l2 = false;
                            },
                            InternalMessage::BboUpdate{ bbos, time } => {
                                // Fast path for BBO subscribers only
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    if let Subscription::Bbo { coin } = sub {
                                        alive &= send_ws_data_from_bbo(outbound, coin, bbos, *time, last_bbo, bbo_hb.is_some()).await;
                                    }
                                }
                            },
                            InternalMessage::Fills{ trades_by_coin } => {
                                // Per-coin payloads were grouped once in the listener; the
                                // wire frame is serialized once by the first subscribed
                                // connection and shared (refcounted bytes) by every other.
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    if let Subscription::Trades { coin } = sub {
                                        if let Some(ct) = trades_by_coin.get(coin.as_str()) {
                                            BROADCASTS_TOTAL.with_label_values(&["trades"]).inc();
                                            let frame = ct.frame.get_or_serialize(|| ServerResponse::Trades(Arc::clone(&ct.trades)));
                                            alive &= outbound.send_frame(frame).await;
                                        }
                                    }
                                }
                            },
                            InternalMessage::L4OrderDiffs{ time, height, diffs_by_coin } => {
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    match sub {
                                        Subscription::BookDiffs { coin } => {
                                            if let Some(cd) = diffs_by_coin.get(coin.as_str()) {
                                                BROADCASTS_TOTAL.with_label_values(&["bookDiffs"]).inc();
                                                let frame = cd.book_diffs_frame.get_or_serialize(|| ServerResponse::BookDiffs(Arc::clone(&cd.diffs)));
                                                alive &= outbound.send_frame(frame).await;
                                            }
                                        }
                                        Subscription::L4Book { coin } => {
                                            if let Some(cd) = diffs_by_coin.get(coin.as_str()) {
                                                BROADCASTS_TOTAL.with_label_values(&["l4"]).inc();
                                                let frame = cd.l4_frame.get_or_serialize(|| {
                                                    ServerResponse::L4Book(L4Book::Updates(L4BookUpdates {
                                                        time: *time,
                                                        height: *height,
                                                        order_statuses: Arc::new(Vec::new()),
                                                        book_diffs: Arc::clone(&cd.diffs),
                                                    }))
                                                });
                                                alive &= outbound.send_frame(frame).await;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            },
                            InternalMessage::L4OrderStatuses{ time, height, statuses_by_coin } => {
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    match sub {
                                        Subscription::L4Book { coin } => {
                                            if let Some(cs) = statuses_by_coin.get(coin.as_str()) {
                                                BROADCASTS_TOTAL.with_label_values(&["l4"]).inc();
                                                let frame = cs.l4_frame.get_or_serialize(|| {
                                                    ServerResponse::L4Book(L4Book::Updates(L4BookUpdates {
                                                        time: *time,
                                                        height: *height,
                                                        order_statuses: Arc::clone(&cs.statuses),
                                                        book_diffs: Arc::new(Vec::new()),
                                                    }))
                                                });
                                                alive &= outbound.send_frame(frame).await;
                                            }
                                        }
                                        Subscription::OrderUpdates { user } => {
                                            alive &= send_ws_order_updates(outbound, user, *time, *height, statuses_by_coin, user_addrs).await;
                                        }
                                        _ => {}
                                    }
                                }
                            },
                        }

                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        CHANNEL_LAG.set(n as i64);
                        CHANNEL_DROPS_TOTAL.inc();
                        // A dropped Snapshot may have carried dirty coins we never
                        // saw - process the next one in full (hash dedup still
                        // suppresses sends whose payload didn't actually change).
                        force_full_l2 = true;
                        log::debug!("Receiver lagged: {n} messages");
                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        return;
                    }
                }
            }

            _ = heartbeat_tick(&mut heartbeat_ticker) => {
                let now = Instant::now();
                let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
                for sub in manager.subscriptions() {
                    if !alive { break; }
                    match sub {
                        Subscription::L2Book { coin, n_sig_figs, mantissa, n_levels } => {
                            let Some(hb) = l2_hb else { continue };
                            let key = l2_cache_key(coin, *n_sig_figs, *mantissa, *n_levels);
                            if let Some(entry) = last_l2.get_mut(&key) {
                                // payload is always Some when the heartbeat is enabled
                                // (the change-driven send stores it for exactly this).
                                if now.duration_since(entry.last_sent) >= hb
                                    && let Some(payload) = entry.payload.as_mut()
                                {
                                    payload.set_time(now_ms);
                                    entry.last_sent = now;
                                    BROADCASTS_TOTAL.with_label_values(&["l2_heartbeat"]).inc();
                                    let payload = payload.clone();
                                    alive &= outbound.send_message(ServerResponse::L2Book(payload)).await;
                                }
                            }
                        }
                        Subscription::Bbo { coin } => {
                            let Some(hb) = bbo_hb else { continue };
                            if let Some(entry) = last_bbo.get_mut(coin) {
                                if now.duration_since(entry.last_sent) >= hb
                                    && let Some(payload) = entry.payload.as_mut()
                                {
                                    payload.time = now_ms;
                                    entry.last_sent = now;
                                    BROADCASTS_TOTAL.with_label_values(&["bbo_heartbeat"]).inc();
                                    let payload = payload.clone();
                                    alive &= outbound.send_message(ServerResponse::Bbo(payload)).await;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            msg = ws_read.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    return;
                                }
                            };

                            log::debug!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                match value {
                                    ClientMessage::Ping => {
                                        // Non-blocking priority enqueue: answered even
                                        // while the data queue is at capacity.
                                        alive &= outbound.send_pong();
                                    }
                                    _ => {
                                        alive &= receive_client_message(outbound, manager, value, universe.as_ref(), listener.clone(), l4_cache, bbo_only, last_l2, last_bbo, active_l2_params, l2_param_guards, active_subs, sub_guards).await;
                                    }
                                }
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                alive &= outbound.send_message(msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return;
                }
            }

            // The writer dropped its queue receivers (send error/timeout).
            // Without this branch an idle session - no broadcasts, no inbound
            // traffic - would never notice the dead writer. Cancel-safe.
            _ = outbound.data_tx.closed() => {
                break;
            }
        }
    }
    info!("Dropping connection: socket write failed or timed out");
}

#[allow(clippy::too_many_arguments)]
async fn receive_client_message(
    outbound: &Outbound,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
    l4_cache: &Arc<L4SnapshotCache>,
    bbo_only: bool,
    last_l2: &mut HashMap<String, L2Entry>,
    last_bbo: &mut HashMap<String, BboEntry>,
    active_l2_params: &ActiveL2Params,
    l2_param_guards: &mut HashMap<L2SnapshotParams, L2ParamGuard>,
    active_subs: &ActiveSubs,
    sub_guards: &mut HashMap<Subscription, Vec<ActiveSubGuard>>,
) -> bool {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
        ClientMessage::Ping => unreachable!("Ping is handled before receive_client_message"),
    };
    // BBO-only mode rejects non-BBO subs up-front, before validation, so the
    // operator sees a single clear "denied" message in the log instead of "valid
    // subscription" then a rejection.
    if bbo_only && !matches!(&subscription, Subscription::Bbo { .. }) {
        return outbound.send_message(ServerResponse::Error(
            "BBO-only mode: L2/L4/Trades subscriptions disabled. Only BBO subscriptions allowed.".to_string(),
        )).await;
    }
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        return outbound.send_message(ServerResponse::Error(format!("Invalid subscription: {sub}"))).await;
    }

    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => match manager.subscribe(subscription.clone()) {
            Ok(inserted) => {
                // Register the variant shape so the listener computes it. One guard
                // per shape per connection (n_levels is a send-time truncation, not
                // part of the cached shape); the entry API dedups shared shapes.
                if inserted
                    && let Subscription::L2Book { n_sig_figs, mantissa, .. } = &subscription
                {
                    let params = L2SnapshotParams::new(*n_sig_figs, *mantissa);
                    l2_param_guards.entry(params).or_insert_with(|| active_l2_params.acquire(params));
                }
                // Count the subscription's broadcast families as live. MUST
                // happen before handle_immediate_snapshot below: the listener
                // only groups/broadcasts for counted families, so counting
                // first guarantees no update falls between the snapshot and
                // the stream.
                if inserted {
                    let guards = active_subs.acquire_for(&subscription);
                    if !guards.is_empty() {
                        sub_guards.insert(subscription.clone(), guards);
                    }
                }
                ("", inserted)
            }
            Err(err) => {
                return outbound.send_message(ServerResponse::Error(format!("Rejected subscription: {err}"))).await;
            }
        },
        ClientMessage::Unsubscribe { .. } => {
            let removed = manager.unsubscribe(subscription.clone());
            // Drop the per-connection dedup/heartbeat cache entry for the just-unsubscribed
            // stream. Without this, a client that sub/unsub-cycles distinct L2 variants on
            // the same coin (or BBO across coins) leaks one entry per cycle until disconnect.
            if removed {
                sub_guards.remove(&subscription);
                match &subscription {
                    Subscription::L2Book { coin, n_sig_figs, mantissa, n_levels } => {
                        last_l2.remove(&l2_cache_key(coin, *n_sig_figs, *mantissa, *n_levels));
                        // Release this connection's guard for the shape only if no
                        // remaining L2 subscription on this connection still uses it
                        // (e.g. same shape on another coin / different n_levels).
                        let params = L2SnapshotParams::new(*n_sig_figs, *mantissa);
                        let still_used = manager.subscriptions().iter().any(|s| {
                            matches!(s, Subscription::L2Book { n_sig_figs: nsf, mantissa: m, .. }
                                if L2SnapshotParams::new(*nsf, *m) == params)
                        });
                        if !still_used {
                            l2_param_guards.remove(&params);
                        }
                    }
                    Subscription::Bbo { coin } => {
                        last_bbo.remove(coin);
                    }
                    _ => {}
                }
            }
            ("un", removed)
        }
        ClientMessage::Ping => unreachable!(),
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener, l4_cache).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    sub_guards.remove(subscription);
                    return outbound.send_message(
                        ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"))).await;
                }
            }
        } else {
            None
        };
        if !outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        if let Some(snapshot_frame) = snapshot_msg {
            return outbound.send_frame(snapshot_frame).await;
        }
        true
    } else {
        outbound.send_message(ServerResponse::Error(format!("Already {word}subscribed: {sub}"))).await
    }
}

/// Fast BBO broadcast - directly from BBO HashMap without L2 snapshot computation.
/// Returns false if the socket send failed/timed out (caller must drop the connection).
async fn send_ws_data_from_bbo(
    outbound: &Outbound,
    coin: &str,
    bbos: &HashMap<Coin, CoinBbo>,
    time: u64,
    last_bbo: &mut HashMap<String, BboEntry>,
    store_payload: bool,
) -> bool {
    // Borrow<str> lookup - no Coin/String allocation per subscription per update.
    if let Some(cb) = bbos.get(coin) {
        let (best_bid, best_ask) = (&cb.raw.0, &cb.raw.1);
        // Dedup on the raw fixed-point values BEFORE rendering anything: the
        // strings are only built when the BBO actually changed.
        let current: BboKey = (
            best_bid.as_ref().map(|(px, sz, _)| (px.value(), sz.value())),
            best_ask.as_ref().map(|(px, sz, _)| (px.value(), sz.value())),
        );

        if last_bbo.get(coin).map(|e| e.tuple) != Some(current) {
            // Canonical wire format (Px/Sz::to_str) - matches what the L2 path
            // emits. Rendered inside the shared-frame builder, so it runs once
            // per coin per broadcast (plus once per heartbeat-enabled
            // connection for the resend payload) instead of per connection.
            let render = || {
                let bid =
                    best_bid.as_ref().map(|(px, sz, n)| crate::types::Level::new(px.to_str(), sz.to_str(), *n as usize));
                let ask =
                    best_ask.as_ref().map(|(px, sz, n)| crate::types::Level::new(px.to_str(), sz.to_str(), *n as usize));
                Bbo { coin: coin.to_string(), time, bid, ask }
            };

            BBO_CHANGES_TOTAL.with_label_values(&[coin]).inc();
            BROADCASTS_TOTAL.with_label_values(&["bbo"]).inc();
            let frame = cb.frame.get_or_serialize(|| ServerResponse::Bbo(render()));
            let payload = store_payload.then(render);
            last_bbo.insert(coin.to_string(), BboEntry { tuple: current, last_sent: Instant::now(), payload });
            return outbound.send_frame(frame).await;
        }
    }
    true
}

/// Per-send timeout. A slow or hostile client whose TCP receive window stays full
/// would otherwise block the writer's `sink.send(...).await` indefinitely. Also
/// bounds how long the session waits for a slot on a full outbound queue.
const WS_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Frames a connection may have queued before the session considers it too
/// slow. Broadcast frames are refcounted `bytes::Bytes` clones (built once and
/// shared across connections), so a full queue holds refcounts, not copies.
/// Combined with `WS_SEND_TIMEOUT` this is the whole slow-client budget: queue
/// depth plus one timeout - the old inline sends compounded the timeout across
/// a subscription loop (256 subscriptions x 5s = minutes during which the
/// session never polled the socket, so pings went unanswered).
const OUTBOUND_QUEUE_DEPTH: usize = 128;
/// Dedicated pong lane depth; see [`Outbound::send_pong`].
const PONG_QUEUE_DEPTH: usize = 8;

/// Pre-serialized `{"channel":"pong"}` frame, so answering a ping allocates
/// nothing and never serializes (guarded by a test against
/// `ServerResponse::Pong`'s serde output).
const PONG_FRAME: &[u8] = br#"{"channel":"pong"}"#;

/// Bounded outbound queues feeding a connection's writer task. All sends from
/// the session task enqueue here instead of awaiting the TCP write inline, so
/// a slow client can never keep the session from polling its socket.
struct Outbound {
    data_tx: mpsc::Sender<bytes::Bytes>,
    pong_tx: mpsc::Sender<bytes::Bytes>,
}

impl Outbound {
    /// Serialize and enqueue a `ServerResponse`. Returns `false` when the
    /// connection is doomed (writer gone or queue wedged); callers in the
    /// session loop must bail out on `false`, mirroring the old direct sends.
    async fn send_message(&self, msg: ServerResponse) -> bool {
        let payload = match serde_json::to_string(&msg) {
            Ok(p) => p,
            Err(err) => {
                error!("Server response serialization error: {err}");
                // Serialization failure is our bug, not the client's; keep the connection.
                return true;
            }
        };
        self.send_payload(bytes::Bytes::from(payload)).await
    }

    /// Enqueue a pre-serialized wire frame (built once in/for the listener
    /// broadcast and shared by every subscribed connection). An empty frame
    /// means its serialization failed when it was first built (already logged
    /// there) - skip it and keep the connection, mirroring `send_message`.
    async fn send_frame(&self, frame: bytes::Bytes) -> bool {
        if frame.is_empty() {
            return true;
        }
        self.send_payload(frame).await
    }

    async fn send_payload(&self, payload: bytes::Bytes) -> bool {
        match tokio::time::timeout(WS_SEND_TIMEOUT, self.data_tx.send(payload)).await {
            Ok(Ok(())) => true,
            // Writer exited; it already logged and counted the send failure.
            Ok(Err(_)) => false,
            Err(_) => {
                error!("Outbound queue full for >{WS_SEND_TIMEOUT:?}; dropping slow client");
                WS_SEND_ERRORS_TOTAL.inc();
                false
            }
        }
    }

    /// Enqueue a pong reply. Synchronous and independent of any data backlog:
    /// pongs ride a small dedicated lane the writer drains first, so a ping is
    /// answered even while the data queue is full. A full pong lane means the
    /// client cannot drain even a handful of tiny control replies - wedged -
    /// so the connection is dropped.
    fn send_pong(&self) -> bool {
        match self.pong_tx.try_send(bytes::Bytes::from_static(PONG_FRAME)) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                error!("Pong queue full; dropping wedged client");
                WS_SEND_ERRORS_TOTAL.inc();
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }
}

/// Writer half of a connection: drains the bounded outbound queues onto the
/// socket sink, pongs first (`biased`). Generic over the sink for testability.
/// Exits on a send error/timeout or once both queue senders are dropped, then
/// attempts a best-effort close handshake (`poll_ready`/`close` also flush
/// yawc's internally-queued control replies, e.g. protocol-level pongs).
async fn write_task<S>(mut sink: S, mut data_rx: mpsc::Receiver<bytes::Bytes>, mut pong_rx: mpsc::Receiver<bytes::Bytes>)
where
    S: futures_util::Sink<FrameView> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut data_open = true;
    let mut pong_open = true;
    while data_open || pong_open {
        let frame = select! {
            biased;

            pong = pong_rx.recv(), if pong_open => match pong {
                Some(frame) => frame,
                None => { pong_open = false; continue; }
            },
            data = data_rx.recv(), if data_open => match data {
                Some(frame) => frame,
                None => { data_open = false; continue; }
            },
        };
        match tokio::time::timeout(WS_SEND_TIMEOUT, sink.send(FrameView::text(frame))).await {
            Ok(Ok(())) => MESSAGES_SENT_TOTAL.inc(),
            Ok(Err(err)) => {
                error!("Failed to send: {err}");
                WS_SEND_ERRORS_TOTAL.inc();
                break;
            }
            Err(_) => {
                error!("Send timeout (>{WS_SEND_TIMEOUT:?}); dropping slow client");
                WS_SEND_ERRORS_TOTAL.inc();
                break;
            }
        }
    }
    // Best-effort close handshake. If the close itself times out we just drop.
    let _unused = tokio::time::timeout(Duration::from_secs(1), sink.close()).await;
}

#[allow(clippy::too_many_arguments)]
async fn send_ws_data_from_snapshot(
    outbound: &Outbound,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>,
    time: u64,
    last_l2: &mut HashMap<String, L2Entry>,
    dirty: &HashSet<Coin>,
    force_full: bool,
    l2_frames: &L2FrameCache,
    store_payload: bool,
) -> bool {
    // BBO subscriptions are filtered out by the caller (they are served by the
    // BboUpdate fast path), so only L2Book needs handling here.
    if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
        // Skip coins that were not rebuilt in this flush: the payload we already
        // sent is still current, so the truncate/export/hash work below would be
        // pure waste. Runs for every subscription on every broadcast, which is
        // why it compares with `&str` (no allocation). `force_full` overrides
        // after a broadcast lag; a missing cache entry means we never sent
        // anything for this subscription (it is brand new) - always process.
        let key = l2_cache_key(coin, *n_sig_figs, *mantissa, *n_levels);
        if !force_full && !dirty.contains(coin.as_str()) && last_l2.contains_key(&key) {
            return true;
        }

        let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
        // Resolve the data source BEFORE consulting the shared frame cache, so
        // the raced-variant early-return below doesn't poison the cache.
        let variant = match snapshot.get(coin.as_str()) {
            Some(per_coin) => {
                let Some(variant) = per_coin.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)) else {
                    // Coin present but this variant shape hasn't been built yet
                    // (subscriber raced the flush); the next flush covers it.
                    error!("Variant for coin {coin} not found");
                    return true;
                };
                Some(variant)
            }
            // The coin's book emptied and the multi-book evicted it. Send an
            // empty snapshot so subscribers learn the book is gone instead of
            // keeping the last non-empty payload on screen forever.
            None => None,
        };

        // Truncate/export (one String per level!), hash, and serialize ONCE per
        // (coin, shape, nLevels) per broadcast via the shared frame cache - the
        // old path repeated all of it per subscribed connection.
        let built = l2_frames.get_or_build(L2FrameKey::new(coin, *n_sig_figs, *mantissa, n_levels), || {
            let exported: [Vec<crate::types::Level>; 2] =
                variant.map_or_else(|| [Vec::new(), Vec::new()], |v| v.truncate(n_levels).export_inner_snapshot());

            // Hash the exported levels for dedup comparison. Level derives Hash;
            // FxHasher because this hashes our own payload (no DoS surface).
            use std::hash::{Hash, Hasher};
            let mut hasher = rustc_hash::FxHasher::default();
            exported.hash(&mut hasher);
            let hash = hasher.finish();

            let l2_book =
                L2Book::from_l2_snapshot(coin.clone(), exported, time, *n_sig_figs, *mantissa, Some(n_levels));
            let frame = match serde_json::to_string(&ServerResponse::L2Book(l2_book.clone())) {
                Ok(json) => bytes::Bytes::from(json),
                Err(err) => {
                    error!("Server response serialization error: {err}");
                    bytes::Bytes::new() // skipped by Outbound::send_frame
                }
            };
            (hash, frame, l2_book)
        });
        let (current_hash, frame, payload) = (built.0, &built.1, &built.2);

        if last_l2.get(&key).map(|e| e.hash) != Some(current_hash) {
            BROADCASTS_TOTAL.with_label_values(&["l2"]).inc();
            let payload = store_payload.then(|| payload.clone());
            last_l2.insert(key, L2Entry { hash: current_hash, last_sent: Instant::now(), payload });
            return outbound.send_frame(frame.clone()).await;
        }
        // else: skip, L2 unchanged
    }
    true
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
        l4_cache: &Arc<L4SnapshotCache>,
    ) -> Result<Option<bytes::Bytes>> {
        if let Self::L4Book { coin } = self {
            if let Some(body) = l4_snapshot_body(l4_cache, &listener, coin, PxBand::default()).await? {
                return Ok(Some(l4_ws_frame(&body)));
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
}

/// Wrap a serialized `L4Book` body into the l4Book WS frame. Byte-identical to
/// `serde_json::to_string(&ServerResponse::L4Book(..))` (guarded by a test),
/// without re-serializing the MB-scale body.
fn l4_ws_frame(body: &bytes::Bytes) -> bytes::Bytes {
    let mut frame = Vec::with_capacity(body.len() + 32);
    frame.extend_from_slice(br#"{"channel":"l4Book","data":"#);
    frame.extend_from_slice(body);
    frame.push(b'}');
    bytes::Bytes::from(frame)
}

/// How long a built L4 snapshot body may be re-served. Long enough that a
/// burst of pollers (or a reconnect storm of l4Book subscribes) shares ONE
/// under-lock build, short enough that "the book as of NOW" stays honest -
/// well under the block cadence clients can observe.
const L4_SNAPSHOT_CACHE_TTL: Duration = Duration::from_millis(100);
/// Concurrent under-lock snapshot builds. More than a couple stacked builds
/// just queue multi-ms lock holds ahead of ingest (the lock is FIFO-fair);
/// waiters usually wake into a cache hit instead.
const L4_SNAPSHOT_BUILD_PERMITS: usize = 2;
/// Cap on distinct cached (coin, band) keys. minPx/maxPx are client-supplied,
/// so the key space is unbounded - without a cap an adversary could mint keys
/// faster than the TTL expires them.
const L4_SNAPSHOT_CACHE_MAX_ENTRIES: usize = 64;

struct L4CacheEntry {
    built_at: Instant,
    /// Serialized `L4Book` JSON: the HTTP body, also the WS frame's `data`.
    body: bytes::Bytes,
    /// Lazily-built gzip of `body`, shared by every request within the TTL.
    gzipped: Option<bytes::Bytes>,
}

/// Short-TTL cache + build limiter for L4 snapshot bodies. Repeat pollers of
/// GET /l4Book (explicitly a polling API) and l4Book subscribe storms used to
/// each pay a full banded book clone UNDER the ingest listener lock, plus
/// their own MB-scale serialization; now at most one build per (coin, band)
/// per TTL, with at most `L4_SNAPSHOT_BUILD_PERMITS` builds in flight.
///
/// A plain `std::sync::Mutex` guards the map deliberately: every access is a
/// short lookup/insert and never spans an `.await`.
struct L4SnapshotCache {
    entries: std::sync::Mutex<HashMap<(String, PxBand), L4CacheEntry>>,
    // Shared across every cache instance (l4Book + untriggeredOrders): the
    // permits bound concurrent builds on the ONE listener mutex they all
    // contend for, so per-instance permits would multiply the worst-case
    // build queue ahead of ingest.
    build_permits: Arc<tokio::sync::Semaphore>,
}

impl L4SnapshotCache {
    fn new(build_permits: Arc<tokio::sync::Semaphore>) -> Self {
        Self { entries: std::sync::Mutex::new(HashMap::new()), build_permits }
    }

    /// Fresh cached body (and gzip, if one was built) for `key`.
    fn get(&self, key: &(String, PxBand)) -> Option<(bytes::Bytes, Option<bytes::Bytes>)> {
        let entries = self.entries.lock().ok()?;
        let entry = entries.get(key)?;
        let hit = (entry.built_at.elapsed() < L4_SNAPSHOT_CACHE_TTL).then(|| (entry.body.clone(), entry.gzipped.clone()));
        drop(entries);
        hit
    }

    /// Insert a freshly-built body. Expired entries are swept here (inserts
    /// are TTL-rate-limited per key, so the sweep is cheap); if the map is
    /// still at capacity afterwards the body is simply served uncached.
    fn insert(&self, key: (String, PxBand), body: bytes::Bytes) {
        if let Ok(mut entries) = self.entries.lock() {
            if entries.len() >= L4_SNAPSHOT_CACHE_MAX_ENTRIES {
                entries.retain(|_, e| e.built_at.elapsed() < L4_SNAPSHOT_CACHE_TTL);
            }
            if entries.len() < L4_SNAPSHOT_CACHE_MAX_ENTRIES {
                entries.insert(key, L4CacheEntry { built_at: Instant::now(), body, gzipped: None });
            }
        }
    }

    /// Attach a gzip variant to an existing fresh entry (best-effort: the
    /// entry may have expired or been evicted while the gzip was running).
    fn set_gzipped(&self, key: &(String, PxBand), gz: &bytes::Bytes) {
        if let Ok(mut entries) = self.entries.lock()
            && let Some(entry) = entries.get_mut(key)
        {
            entry.gzipped = Some(gz.clone());
        }
    }
}

/// Serialized L4 snapshot body for one coin+band, built at most once per TTL.
/// The listener lock is held only for the banded clone inside
/// `compute_snapshot_for_coin`; the `L4Order` conversion and the MB-scale
/// serialization run on a blocking thread so they neither hold the lock nor
/// wedge async runtime workers. `Ok(None)` when the coin has no book.
async fn l4_snapshot_body(
    cache: &Arc<L4SnapshotCache>,
    listener: &Arc<Mutex<OrderBookListener>>,
    coin: &str,
    band: PxBand,
) -> Result<Option<bytes::Bytes>> {
    let key = (coin.to_string(), band);
    if let Some((body, _)) = cache.get(&key) {
        return Ok(Some(body));
    }
    // Single-flight (approximate): concurrent requesters queue here; whoever
    // follows the builder through re-checks the cache and hits it.
    let _permit = cache.build_permits.acquire().await?;
    if let Some((body, _)) = cache.get(&key) {
        return Ok(Some(body));
    }

    let snapshot = listener.lock().await.compute_snapshot_for_coin(&Coin::new(coin), band);
    let Some((time, height, coin_snapshot)) = snapshot else {
        return Ok(None);
    };
    let coin_owned = coin.to_string();
    let body = tokio::task::spawn_blocking(move || -> Result<bytes::Bytes> {
        // The snapshot is already owned (cloned under the lock) - consume it
        // instead of the old `.as_ref().clone()`, which deep-cloned every
        // order (several heap Strings each) a second time for nothing.
        let levels = coin_snapshot.into_inner().map(|orders| orders.into_iter().map(L4Order::from).collect());
        let book = L4Book::Snapshot { coin: coin_owned, time, height, levels };
        Ok(bytes::Bytes::from(serde_json::to_string(&book)?))
    })
    .await??;
    cache.insert(key, body.clone());
    Ok(Some(body))
}

/// Query parameters for the one-shot GET /l4Book endpoint.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct L4SnapshotQuery {
    coin: String,
    min_px: Option<String>,
    max_px: Option<String>,
}

/// One-shot banded L4 snapshot over plain HTTP: every request returns the book
/// slice as of NOW, so repeat requests get current state with no subscription
/// lifecycle and no update stream to filter. The body is the same JSON as the
/// WS l4Book message's `data` field (`{"Snapshot":{...}}`), so clients can
/// share their parsing with the WS path.
async fn l4_snapshot_handler(
    axum::extract::Query(query): axum::extract::Query<L4SnapshotQuery>,
    headers: axum::http::HeaderMap,
    listener: Arc<Mutex<OrderBookListener>>,
    l4_cache: Arc<L4SnapshotCache>,
) -> axum::response::Response {
    fn json_response(status: axum::http::StatusCode, body: String) -> axum::response::Response {
        axum::response::Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(body.into())
            .unwrap_or_else(|_| axum::response::Response::new(String::new().into()))
    }

    let band = match PxBand::parse(query.min_px.as_deref(), query.max_px.as_deref()) {
        Ok(band) => band,
        Err(err) => {
            return json_response(
                axum::http::StatusCode::BAD_REQUEST,
                format!(r#"{{"error":"invalid price band: {err}"}}"#),
            );
        }
    };
    match l4_snapshot_body(&l4_cache, &listener, &query.coin, band).await {
        Ok(Some(body)) => {
            // Order JSON compresses ~10x; without this, transfer time
            // dwarfs the build for remote clients pulling MB-scale
            // snapshots (a $2000 BTC band is ~2.4MB raw, ~250KB gzipped).
            let accepts_gzip = headers
                .get(axum::http::header::ACCEPT_ENCODING)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.contains("gzip"));
            if accepts_gzip && let Some(gz) = gzipped_l4_body(&l4_cache, (query.coin.clone(), band), body.clone()).await {
                return axum::response::Response::builder()
                    .status(axum::http::StatusCode::OK)
                    .header("content-type", "application/json")
                    .header("content-encoding", "gzip")
                    .body(gz.into())
                    .unwrap_or_else(|_| axum::response::Response::new(String::new().into()));
            }
            axum::response::Response::builder()
                .status(axum::http::StatusCode::OK)
                .header("content-type", "application/json")
                .body(body.into())
                .unwrap_or_else(|_| axum::response::Response::new(String::new().into()))
        }
        Ok(None) => json_response(
            axum::http::StatusCode::NOT_FOUND,
            format!(r#"{{"error":"no order book for coin {}"}}"#, query.coin),
        ),
        Err(err) => {
            error!("l4Book snapshot build error: {err}");
            json_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"error":"snapshot build failed"}"#.to_string(),
            )
        }
    }
}

/// Gzip of a cached L4 body, built once per TTL window and shared: the first
/// gzip-accepting request compresses on a blocking thread (MB-scale deflate
/// would stall an async worker) and stores the result on the cache entry;
/// followers reuse it. None on failure (caller serves the uncompressed body).
async fn gzipped_l4_body(
    cache: &Arc<L4SnapshotCache>,
    key: (String, PxBand),
    body: bytes::Bytes,
) -> Option<bytes::Bytes> {
    if let Some((_, Some(gz))) = cache.get(&key) {
        return Some(gz);
    }
    let gz = tokio::task::spawn_blocking(move || gzip_body(&body)).await.ok()??;
    let gz = bytes::Bytes::from(gz);
    cache.set_gzipped(&key, &gz);
    Some(gz)
}

/// Gzip at the fastest level: on MB-scale order JSON the ~10x ratio is what
/// matters, and level 1 keeps the CPU cost per request in single-digit ms.
/// None on write failure (caller falls back to the uncompressed body).
fn gzip_body(body: &[u8]) -> Option<Vec<u8>> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::with_capacity(body.len() / 8), flate2::Compression::fast());
    encoder.write_all(body).ok()?;
    encoder.finish().ok()
}

/// Query parameters for the one-shot GET /untriggeredOrders endpoint.
#[derive(serde::Deserialize)]
struct UntriggeredQuery {
    coin: Option<String>,
}

/// Wire shape of GET /untriggeredOrders: untriggered trigger orders (stops /
/// TP-SL waiting for their trigger price) grouped per coin, each order as an
/// `[ownerAddress, order]` tuple - the same tuple layout as l4Book orders, so
/// downstream snapshot producers can splice them into `untriggered_orders`
/// arrays without reshaping.
#[derive(serde::Serialize)]
struct UntriggeredOrders {
    time: u64,
    height: u64,
    data: Vec<(String, Vec<(alloy::primitives::Address, L4Order)>)>,
}

/// Serialized untriggered-orders body (all coins, or one coin), built at most
/// once per TTL. Same discipline as `l4_snapshot_body`: the listener lock is
/// held only for the order clone; grouping, conversion, and serialization run
/// on a blocking thread. `Ok(None)` until the first snapshot install.
async fn untriggered_body(
    cache: &Arc<L4SnapshotCache>,
    listener: &Arc<Mutex<OrderBookListener>>,
    coin: Option<&str>,
) -> Result<Option<bytes::Bytes>> {
    // Reuses the l4 cache's (String, PxBand) key with a default band; ""
    // (never a valid coin) keys the all-coins body.
    let key = (coin.unwrap_or_default().to_string(), PxBand::default());
    if let Some((body, _)) = cache.get(&key) {
        return Ok(Some(body));
    }
    let _permit = cache.build_permits.acquire().await?;
    if let Some((body, _)) = cache.get(&key) {
        return Ok(Some(body));
    }

    let filter_coin = coin.map(Coin::new);
    let snapshot = listener.lock().await.compute_untriggered_snapshot(filter_coin.as_ref());
    let Some((time, height, mut orders)) = snapshot else {
        return Ok(None);
    };
    let body = tokio::task::spawn_blocking(move || -> Result<bytes::Bytes> {
        // Deterministic output (sorted coins, oid-ordered orders) so repeat
        // polls and side-by-side source diffs compare cleanly. The deep
        // per-order clone (Arc -> owned L4Order) happens here, off-lock.
        orders.sort_unstable_by(|a, b| a.coin.cmp(&b.coin).then(a.oid.cmp(&b.oid)));
        let mut data: Vec<(String, Vec<(alloy::primitives::Address, L4Order)>)> = Vec::new();
        for inner in orders {
            let user = inner.user;
            let coin_name = inner.coin.value();
            let order = L4Order::from((*inner).clone());
            match data.last_mut() {
                Some((current, entries)) if *current == coin_name => entries.push((user, order)),
                _ => data.push((coin_name, vec![(user, order)])),
            }
        }
        let body = UntriggeredOrders { time, height, data };
        Ok(bytes::Bytes::from(serde_json::to_string(&body)?))
    })
    .await??;
    cache.insert(key, body.clone());
    Ok(Some(body))
}

/// One-shot untriggered trigger orders over plain HTTP. Every request returns
/// the pending stop / TP-SL set as of NOW - the part of the order state that
/// never rests on the book and is therefore invisible to l4Book. `coin` is
/// optional; omitting it returns every coin. A coin with no pending triggers
/// yields an empty `data` (a legitimate state, not an error); 503 until the
/// first snapshot install has completed.
async fn untriggered_orders_handler(
    axum::extract::Query(query): axum::extract::Query<UntriggeredQuery>,
    headers: axum::http::HeaderMap,
    listener: Arc<Mutex<OrderBookListener>>,
    cache: Arc<L4SnapshotCache>,
) -> axum::response::Response {
    fn json_response(status: axum::http::StatusCode, body: String) -> axum::response::Response {
        axum::response::Response::builder()
            .status(status)
            .header("content-type", "application/json")
            .body(body.into())
            .unwrap_or_else(|_| axum::response::Response::new(String::new().into()))
    }

    // An empty coin param would collide with the all-coins cache key ("" is
    // the sentinel) and let one client poison the shared all-coins body with
    // an empty per-coin one - reject it outright.
    if query.coin.as_deref() == Some("") {
        return json_response(
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"error":"coin must not be empty; omit the parameter for all coins"}"#.to_string(),
        );
    }

    match untriggered_body(&cache, &listener, query.coin.as_deref()).await {
        Ok(Some(body)) => {
            let accepts_gzip = headers
                .get(axum::http::header::ACCEPT_ENCODING)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.contains("gzip"));
            let gzip_key = (query.coin.clone().unwrap_or_default(), PxBand::default());
            if accepts_gzip && let Some(gz) = gzipped_l4_body(&cache, gzip_key, body.clone()).await {
                return axum::response::Response::builder()
                    .status(axum::http::StatusCode::OK)
                    .header("content-type", "application/json")
                    .header("content-encoding", "gzip")
                    .body(gz.into())
                    .unwrap_or_else(|_| axum::response::Response::new(String::new().into()));
            }
            axum::response::Response::builder()
                .status(axum::http::StatusCode::OK)
                .header("content-type", "application/json")
                .body(body.into())
                .unwrap_or_else(|_| axum::response::Response::new(String::new().into()))
        }
        Ok(None) => json_response(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            r#"{"error":"initializing - snapshot not yet installed"}"#.to_string(),
        ),
        Err(err) => {
            error!("untriggeredOrders build error: {err}");
            json_response(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"error":"untriggered orders build failed"}"#.to_string(),
            )
        }
    }
}

/// Send order updates to an OrderUpdates subscriber, filtered by user address.
/// Filters by reference over the shared per-coin grouping and clones only the
/// matching statuses - the old path deep-cloned the whole batch per user
/// subscription per message. Within a coin the original order is preserved;
/// across coins (same block, same time/height) the grouping iterates in map
/// order.
async fn send_ws_order_updates(
    outbound: &Outbound,
    user: &str,
    time: u64,
    height: u64,
    statuses_by_coin: &HashMap<String, crate::listeners::order_book::CoinStatuses>,
    user_addrs: &mut HashMap<String, alloy::primitives::Address>,
) -> bool {
    // Parse each subscription's address once, not once per broadcast.
    let user_addr = match user_addrs.get(user) {
        Some(addr) => *addr,
        None => match user.parse::<alloy::primitives::Address>() {
            Ok(addr) => {
                user_addrs.insert(user.to_string(), addr);
                addr
            }
            // invalid address; validation prevents this at subscribe time
            Err(_) => return true,
        },
    };

    let user_updates: Vec<OrderUpdate> = statuses_by_coin
        .values()
        .flat_map(|cs| cs.statuses.iter())
        .filter(|status| status.user == user_addr)
        .map(|status| OrderUpdate::new(status.user, time, height, status.clone()))
        .collect();

    if !user_updates.is_empty() {
        return outbound.send_message(ServerResponse::OrderUpdates(user_updates)).await;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    /// Sink that records every sent frame's payload, for driving `write_task`.
    #[derive(Clone, Default)]
    struct RecordingSink(Arc<std::sync::Mutex<Vec<bytes::Bytes>>>);

    impl futures_util::Sink<FrameView> for RecordingSink {
        type Error = std::convert::Infallible;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, item: FrameView) -> std::result::Result<(), Self::Error> {
            self.0.lock().unwrap().push(item.payload.clone());
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Sink that is never ready: models a client whose TCP window stays full.
    struct StuckSink;

    impl futures_util::Sink<FrameView> for StuckSink {
        type Error = std::convert::Infallible;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Pending
        }
        fn start_send(self: Pin<&mut Self>, _item: FrameView) -> std::result::Result<(), Self::Error> {
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn test_untriggered_orders_wire_shape() {
        // Consumers splice `data` entries into snapshot-blob `untriggered_orders`
        // arrays: each order must serialize as an [ownerAddress, order] tuple
        // with the HL field names (triggerPx / isTrigger / isPositionTpsl ...).
        // Built through the real InnerL4Order -> L4Order conversion (the path
        // untriggered_body uses), with an address containing alphabetic hex so
        // the lowercase (non-EIP-55) serialization is actually pinned.
        let user: alloy::primitives::Address =
            "0xAbCdEf0123456789aBcDeF0123456789abcdef01".parse().expect("valid address");
        let inner = crate::types::inner::InnerL4Order {
            user,
            coin: Coin::new("BTC"),
            side: crate::order_book::Side::Bid,
            limit_px: crate::order_book::Px::parse_from_str("100.5").unwrap(),
            sz: crate::order_book::Sz::parse_from_str("1.5").unwrap(),
            oid: 42,
            timestamp: 1000,
            trigger_condition: "Price above 110".to_string(),
            is_trigger: true,
            trigger_px: "110.0".to_string(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: "Stop Market".to_string(),
            tif: None,
            cloid: None,
        };
        let order = L4Order::from(inner);
        let body = UntriggeredOrders { time: 5, height: 7, data: vec![("BTC".to_string(), vec![(user, order)])] };
        let json = serde_json::to_string(&body).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["time"], 5);
        assert_eq!(v["height"], 7);
        assert_eq!(v["data"][0][0], "BTC");
        // Tuple layout: [address, order] - address serialized as lowercase hex.
        assert_eq!(v["data"][0][1][0][0], "0xabcdef0123456789abcdef0123456789abcdef01");
        let order_json = &v["data"][0][1][0][1];
        assert_eq!(order_json["triggerPx"], "110.0");
        assert_eq!(order_json["isTrigger"], true);
        assert_eq!(order_json["isPositionTpsl"], false);
        assert_eq!(order_json["side"], "B");
        assert_eq!(order_json["sz"], "1.5");
        assert_eq!(order_json["oid"], 42);
        assert_eq!(order_json["limitPx"], "100.5");
        // Conversion artifacts the consumer sees: children always [], origSz
        // mirrors sz (InnerL4Order does not retain the original size).
        assert_eq!(order_json["children"], serde_json::json!([]));
        assert_eq!(order_json["origSz"], "1.5");
        assert_eq!(order_json["user"], "0xabcdef0123456789abcdef0123456789abcdef01");
    }

    #[test]
    fn test_pong_frame_matches_server_response_serialization() {
        // send_pong enqueues the pre-serialized constant; it must stay
        // byte-identical to the serde wire format of ServerResponse::Pong.
        assert_eq!(PONG_FRAME, serde_json::to_string(&ServerResponse::Pong).unwrap().as_bytes());
    }

    #[tokio::test]
    async fn test_write_task_sends_pongs_before_queued_data() {
        // Data enqueued FIRST, pong after - the pong must still go out first:
        // that ordering is what keeps pings answered under a data backlog.
        let (data_tx, data_rx) = mpsc::channel(8);
        let (pong_tx, pong_rx) = mpsc::channel(8);
        for i in 0..3 {
            data_tx.send(bytes::Bytes::from(format!("data{i}"))).await.unwrap();
        }
        pong_tx.send(bytes::Bytes::from_static(PONG_FRAME)).await.unwrap();
        drop((data_tx, pong_tx));

        let sink = RecordingSink::default();
        write_task(sink.clone(), data_rx, pong_rx).await;

        let frames = sink.0.lock().unwrap();
        assert_eq!(frames.len(), 4, "all queued frames must drain before exit");
        assert_eq!(frames[0].as_ref(), PONG_FRAME, "the pong must jump the data backlog");
    }

    #[tokio::test]
    async fn test_outbound_fails_fast_when_writer_is_gone() {
        // A dead writer (dropped receivers) must fail sends immediately - the
        // session bails out instead of queueing into the void.
        let (data_tx, data_rx) = mpsc::channel(8);
        let (pong_tx, pong_rx) = mpsc::channel(8);
        drop((data_rx, pong_rx));
        let outbound = Outbound { data_tx, pong_tx };
        assert!(!outbound.send_payload(bytes::Bytes::from_static(b"x")).await);
        assert!(!outbound.send_pong());
        // And the idle-session watchdog branch fires.
        outbound.data_tx.closed().await;
    }

    #[tokio::test]
    async fn test_send_pong_full_lane_means_wedged_client() {
        let (data_tx, _data_rx) = mpsc::channel(8);
        let (pong_tx, _pong_rx) = mpsc::channel(2);
        let outbound = Outbound { data_tx, pong_tx };
        assert!(outbound.send_pong());
        assert!(outbound.send_pong());
        // Lane full and nothing draining: the client is wedged.
        assert!(!outbound.send_pong());
    }

    #[tokio::test(start_paused = true)]
    async fn test_stuck_writer_times_out_and_dooms_the_session() {
        // Writer wedged on a never-ready socket: it must give up after
        // WS_SEND_TIMEOUT, and session sends must then start failing. This
        // bounds the slow-client budget at queue depth + one timeout - the old
        // inline sends compounded the timeout per subscription.
        let (data_tx, data_rx) = mpsc::channel(1);
        let (pong_tx, pong_rx) = mpsc::channel(1);
        let outbound = Outbound { data_tx, pong_tx };
        let writer = tokio::spawn(write_task(StuckSink, data_rx, pong_rx));

        assert!(outbound.send_payload(bytes::Bytes::from_static(b"x")).await);
        writer.await.unwrap();
        assert!(!outbound.send_payload(bytes::Bytes::from_static(b"y")).await);
    }

    #[test]
    fn test_l2_cache_key_distinguishes_n_levels() {
        // Two subscriptions differing only in nLevels MUST have distinct keys:
        // a shared entry made their dedup hashes ping-pong (both resent every
        // broadcast) and unsubscribing one dropped the other's cache.
        let a = l2_cache_key("BTC", Some(5), None, None);
        let b = l2_cache_key("BTC", Some(5), None, Some(50));
        assert_ne!(a, b);
        // Validation rejects an explicit nLevels == DEFAULT_LEVELS, so the
        // None default cannot collide with a permitted explicit value.
        assert_eq!(l2_cache_key("BTC", Some(5), None, None), l2_cache_key("BTC", Some(5), None, Some(DEFAULT_LEVELS)));
        assert_ne!(l2_cache_key("BTC", Some(5), None, None), l2_cache_key("ETH", Some(5), None, None));
        assert_ne!(l2_cache_key("BTC", Some(5), Some(2), None), l2_cache_key("BTC", Some(5), Some(5), None));
    }

    #[test]
    fn test_l4_ws_frame_matches_server_response_serialization() {
        // The WS l4Book snapshot frame is now assembled by wrapping the cached
        // body (no re-serialization); it must stay byte-identical to the old
        // serde_json::to_string(&ServerResponse::L4Book(..)) wire format.
        let book = L4Book::Snapshot { coin: "BTC".to_string(), time: 1, height: 2, levels: [Vec::new(), Vec::new()] };
        let body = bytes::Bytes::from(serde_json::to_string(&book).unwrap());
        let expected = serde_json::to_string(&ServerResponse::L4Book(book)).unwrap();
        assert_eq!(l4_ws_frame(&body).as_ref(), expected.as_bytes());
    }

    #[test]
    fn test_l4_snapshot_cache_ttl_and_cap() {
        let cache = L4SnapshotCache::new(Arc::new(tokio::sync::Semaphore::new(L4_SNAPSHOT_BUILD_PERMITS)));
        let key = ("BTC".to_string(), PxBand::default());
        assert!(cache.get(&key).is_none());
        cache.insert(key.clone(), bytes::Bytes::from_static(b"{}"));
        let (body, gz) = cache.get(&key).expect("fresh entry must hit");
        assert_eq!(body.as_ref(), b"{}");
        assert!(gz.is_none());
        // gzip variant is attached to the live entry and shared afterwards.
        cache.set_gzipped(&key, &bytes::Bytes::from_static(b"gz"));
        assert!(cache.get(&key).and_then(|(_, gz)| gz).is_some());
        // The key-count cap holds even when every entry is fresh: over-cap
        // inserts are dropped (served uncached) instead of growing the map.
        for i in 0..(2 * L4_SNAPSHOT_CACHE_MAX_ENTRIES) {
            cache.insert((format!("C{i}"), PxBand::default()), bytes::Bytes::from_static(b"{}"));
        }
        let len = cache.entries.lock().unwrap().len();
        assert!(len <= L4_SNAPSHOT_CACHE_MAX_ENTRIES, "cache must stay capped, got {len}");
    }

    #[test]
    fn test_http_l4_snapshot_body_matches_ws_data_field() {
        // The GET /l4Book body is documented as identical to the WS l4Book
        // message's `data` field, so clients can share parsing across both.
        let make = || L4Book::Snapshot { coin: "BTC".to_string(), time: 1, height: 2, levels: [Vec::new(), Vec::new()] };
        let http_body = serde_json::to_string(&make()).unwrap();
        let ws_frame = serde_json::to_string(&ServerResponse::L4Book(make())).unwrap();
        assert_eq!(ws_frame, format!(r#"{{"channel":"l4Book","data":{http_body}}}"#));
        assert!(http_body.starts_with(r#"{"Snapshot":"#));
    }

    #[test]
    fn test_l4_snapshot_query_camel_case() {
        // GET /l4Book?coin=BTC&minPx=..&maxPx=.. - the query params are
        // camelCase like every other wire-facing name.
        let q: L4SnapshotQuery =
            serde_json::from_str(r#"{"coin":"BTC","minPx":"64000","maxPx":"66000"}"#).unwrap();
        assert_eq!(q.coin, "BTC");
        assert_eq!(q.min_px.as_deref(), Some("64000"));
        assert_eq!(q.max_px.as_deref(), Some("66000"));
        let bare: L4SnapshotQuery = serde_json::from_str(r#"{"coin":"BTC"}"#).unwrap();
        assert!(bare.min_px.is_none() && bare.max_px.is_none());
    }
}
