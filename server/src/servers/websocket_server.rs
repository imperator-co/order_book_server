use crate::{
    listeners::order_book::{
        ActiveL2Params, ActiveSubGuard, ActiveSubs, CoinBbo, InternalMessage, L2FrameCache, L2FrameKey, L2ParamGuard,
        L2SnapshotParams, OrderBookListener, hl_listen_hft,
    },
    metrics::{
        BBO_CHANGES_TOTAL, BROADCAST_RECEIVERS, BROADCASTS_TOTAL, CHANNEL_DROPS_TOTAL, CHANNEL_LAG,
        MESSAGES_SENT_TOTAL, ORDERBOOK_HEIGHT, WS_CONNECTIONS_ACTIVE, WS_CONNECTIONS_TOTAL, WS_SEND_ERRORS_TOTAL,
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
    let listener_for_health = listener.clone();

    // Shared L4 snapshot body cache (GET /l4Book + WS l4Book subscribe).
    let l4_cache = Arc::new(L4SnapshotCache::new());

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
            "/health",
            get(move || {
                let listener = listener_for_health.clone();
                async move {
                    let is_ready = listener.lock().await.is_ready();
                    let uptime_secs = start_time.elapsed().as_secs();
                    let height = ORDERBOOK_HEIGHT.get();
                    let connections = WS_CONNECTIONS_ACTIVE.get();
                    let body = format!(
                        r#"{{"status":"{}","uptime_seconds":{},"height":{},"connections":{}}}"#,
                        if is_ready { "ready" } else { "initializing" },
                        uptime_secs,
                        height,
                        connections,
                    );
                    axum::response::Response::builder().header("content-type", "application/json").body(body).unwrap()
                }
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
    mut socket: WebSocket,
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
    let is_ready = listener.lock().await.is_ready();
    let mut manager = SubscriptionManager::default();
    // Market-filtered universe for subscription validation. Refreshed from
    // Snapshot broadcasts (Arc-shared, built once in the listener) whenever the
    // coin set changes - the old code rebuilt the full String set per connection
    // on every broadcast.
    let mut universe = listener.lock().await.universe();
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
    let active_l2_params = listener.lock().await.active_l2_params();
    let mut l2_param_guards: HashMap<L2SnapshotParams, L2ParamGuard> = HashMap::new();
    // Per-family subscription counts (l4/trades/bbo): the listener skips the
    // per-event grouping+broadcast work for families with zero subscribers.
    // One guard set per live subscription; dropping the map on disconnect
    // releases everything, mirroring l2_param_guards.
    let active_subs = listener.lock().await.active_subs();
    let mut sub_guards: HashMap<Subscription, Vec<ActiveSubGuard>> = HashMap::new();
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        let _ = send_socket_message(&mut socket, msg).await;
        return;
    }

    // Optional heartbeat ticker. We tick at min(enabled_heartbeats)/2 (clamped to [50, 500] ms)
    // so each subscription's last-sent timestamp can drift at most half a heartbeat from the configured value.
    let mut heartbeat_ticker = build_heartbeat_ticker(l2book_heartbeat_ms, bbo_heartbeat_ms);
    let l2_hb = if l2book_heartbeat_ms > 0 { Some(Duration::from_millis(l2book_heartbeat_ms)) } else { None };
    let bbo_hb = if bbo_heartbeat_ms > 0 { Some(Duration::from_millis(bbo_heartbeat_ms)) } else { None };

    // `alive` flips to false the moment any `send_socket_message` returns false
    // (network error or send timeout). The outer loop checks it at every iteration
    // boundary so a wedged client is dropped instead of looping forever.
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
                                    universe = Arc::clone(u);
                                }
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    // Skip BBO subs here - they get fast updates via BboUpdate
                                    if !matches!(sub, Subscription::Bbo { .. }) {
                                        alive &= send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time, &mut last_l2, dirty, force_full_l2, l2_frames, l2_hb.is_some()).await;
                                    }
                                }
                                force_full_l2 = false;
                            },
                            InternalMessage::BboUpdate{ bbos, time } => {
                                // Fast path for BBO subscribers only
                                for sub in manager.subscriptions() {
                                    if !alive { break; }
                                    if let Subscription::Bbo { coin } = sub {
                                        alive &= send_ws_data_from_bbo(&mut socket, coin, bbos, *time, &mut last_bbo, bbo_hb.is_some()).await;
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
                                            alive &= send_socket_frame(&mut socket, frame).await;
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
                                                alive &= send_socket_frame(&mut socket, frame).await;
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
                                                alive &= send_socket_frame(&mut socket, frame).await;
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
                                                alive &= send_socket_frame(&mut socket, frame).await;
                                            }
                                        }
                                        Subscription::OrderUpdates { user } => {
                                            alive &= send_ws_order_updates(&mut socket, user, *time, *height, statuses_by_coin, &mut user_addrs).await;
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
                                    alive &= send_socket_message(&mut socket, ServerResponse::L2Book(payload)).await;
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
                                    alive &= send_socket_message(&mut socket, ServerResponse::Bbo(payload)).await;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            msg = socket.next() => {
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
                                        alive &= send_socket_message(&mut socket, ServerResponse::Pong).await;
                                    }
                                    _ => {
                                        alive &= receive_client_message(&mut socket, &mut manager, value, &universe, listener.clone(), &l4_cache, bbo_only, &mut last_l2, &mut last_bbo, &active_l2_params, &mut l2_param_guards, &active_subs, &mut sub_guards).await;
                                    }
                                }
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                alive &= send_socket_message(&mut socket, msg).await;
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
        }
    }
    info!("Dropping connection: socket write failed or timed out");
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
async fn receive_client_message(
    socket: &mut WebSocket,
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
        return send_socket_message(socket, ServerResponse::Error(
            "BBO-only mode: L2/L4/Trades subscriptions disabled. Only BBO subscriptions allowed.".to_string(),
        )).await;
    }
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        return send_socket_message(socket, ServerResponse::Error(format!("Invalid subscription: {sub}"))).await;
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
                return send_socket_message(socket, ServerResponse::Error(format!("Rejected subscription: {err}"))).await;
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
                    return send_socket_message(socket,
                        ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"))).await;
                }
            }
        } else {
            None
        };
        if !send_socket_message(socket, ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        if let Some(snapshot_frame) = snapshot_msg {
            return send_socket_frame(socket, snapshot_frame).await;
        }
        true
    } else {
        send_socket_message(socket, ServerResponse::Error(format!("Already {word}subscribed: {sub}"))).await
    }
}

/// Fast BBO broadcast - directly from BBO HashMap without L2 snapshot computation.
/// Returns false if the socket send failed/timed out (caller must drop the connection).
async fn send_ws_data_from_bbo(
    socket: &mut WebSocket,
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
            return send_socket_frame(socket, frame).await;
        }
    }
    true
}

/// Per-send timeout. A slow or hostile client whose TCP receive window stays full
/// would otherwise block `socket.send(...).await` indefinitely, freezing this
/// connection's whole `select!` loop and accumulating broadcast lag.
const WS_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Send a `ServerResponse` to the client. Returns `false` when the underlying
/// socket failed to write (network error or `WS_SEND_TIMEOUT` elapsed). Callers
/// in the `select!` loop must bail out on `false` so we drop the doomed
/// connection instead of looping forever on a wedged write.
async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) -> bool {
    let payload = match serde_json::to_string(&msg) {
        Ok(p) => p,
        Err(err) => {
            error!("Server response serialization error: {err}");
            // Serialization failure is our bug, not the client's; keep the connection.
            return true;
        }
    };
    send_socket_payload(socket, bytes::Bytes::from(payload)).await
}

/// Send a pre-serialized wire frame (built once in/for the listener broadcast
/// and shared by every subscribed connection). An empty frame means its
/// serialization failed when it was first built (already logged there) - skip
/// it and keep the connection, mirroring `send_socket_message`.
async fn send_socket_frame(socket: &mut WebSocket, frame: bytes::Bytes) -> bool {
    if frame.is_empty() {
        return true;
    }
    send_socket_payload(socket, frame).await
}

async fn send_socket_payload(socket: &mut WebSocket, payload: bytes::Bytes) -> bool {
    match tokio::time::timeout(WS_SEND_TIMEOUT, socket.send(FrameView::text(payload))).await {
        Ok(Ok(())) => {
            MESSAGES_SENT_TOTAL.inc();
            true
        }
        Ok(Err(err)) => {
            error!("Failed to send: {err}");
            WS_SEND_ERRORS_TOTAL.inc();
            false
        }
        Err(_) => {
            error!("Send timeout (>{:?}); dropping slow client", WS_SEND_TIMEOUT);
            WS_SEND_ERRORS_TOTAL.inc();
            // Best-effort close handshake. If the close itself times out we just drop.
            let _unused = tokio::time::timeout(Duration::from_secs(1), socket.close()).await;
            false
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
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
                    bytes::Bytes::new() // skipped by send_socket_frame
                }
            };
            (hash, frame, l2_book)
        });
        let (current_hash, frame, payload) = (built.0, &built.1, &built.2);

        if last_l2.get(&key).map(|e| e.hash) != Some(current_hash) {
            BROADCASTS_TOTAL.with_label_values(&["l2"]).inc();
            let payload = store_payload.then(|| payload.clone());
            last_l2.insert(key, L2Entry { hash: current_hash, last_sent: Instant::now(), payload });
            return send_socket_frame(socket, frame.clone()).await;
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
    build_permits: tokio::sync::Semaphore,
}

impl L4SnapshotCache {
    fn new() -> Self {
        Self {
            entries: std::sync::Mutex::new(HashMap::new()),
            build_permits: tokio::sync::Semaphore::new(L4_SNAPSHOT_BUILD_PERMITS),
        }
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

/// Send order updates to an OrderUpdates subscriber, filtered by user address.
/// Filters by reference over the shared per-coin grouping and clones only the
/// matching statuses - the old path deep-cloned the whole batch per user
/// subscription per message. Within a coin the original order is preserved;
/// across coins (same block, same time/height) the grouping iterates in map
/// order.
async fn send_ws_order_updates(
    socket: &mut WebSocket,
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
        return send_socket_message(socket, ServerResponse::OrderUpdates(user_updates)).await;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let cache = L4SnapshotCache::new();
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
