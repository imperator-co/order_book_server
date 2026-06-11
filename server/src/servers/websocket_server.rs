use crate::{
    listeners::order_book::{
        ActiveL2Params, CoinBbo, InternalMessage, L2FrameCache, L2FrameKey, L2ParamGuard, L2SnapshotParams,
        OrderBookListener, hl_listen_hft,
    },
    metrics::{
        BBO_CHANGES_TOTAL, BROADCAST_RECEIVERS, BROADCASTS_TOTAL, CHANNEL_DROPS_TOTAL, CHANNEL_LAG,
        MESSAGES_SENT_TOTAL, ORDERBOOK_HEIGHT, WS_CONNECTIONS_ACTIVE, WS_CONNECTIONS_TOTAL, WS_SEND_ERRORS_TOTAL,
    },
    order_book::{Coin, Snapshot},
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
        OrderBookListener::new(Some(internal_message_tx), ignore_spot, active_l2_params.clone(), market_filter)
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

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));

    let start_time = Instant::now();
    let listener_for_health = listener.clone();

    let app: Router = Router::new()
        .route(
            "/ws",
            get({
                let internal_message_tx = internal_message_tx.clone();
                let bbo_only = config.bbo_only;
                let l2book_heartbeat_ms = config.l2book_heartbeat_ms;
                let bbo_heartbeat_ms = config.bbo_heartbeat_ms;
                let listener = listener.clone();
                move |ws_upgrade| async move {
                    ws_handler(
                        ws_upgrade,
                        internal_message_tx.clone(),
                        listener.clone(),
                        bbo_only,
                        l2book_heartbeat_ms,
                        bbo_heartbeat_ms,
                        websocket_opts,
                    )
                }
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

        handle_socket(ws, internal_message_tx, listener, bbo_only, l2book_heartbeat_ms, bbo_heartbeat_ms).await;
    });

    resp.into_response()
}

#[allow(clippy::too_many_arguments)]
async fn handle_socket(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
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
                                        alive &= receive_client_message(&mut socket, &mut manager, value, &universe, listener.clone(), bbo_only, &mut last_l2, &mut last_bbo, &active_l2_params, &mut l2_param_guards).await;
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
    bbo_only: bool,
    last_l2: &mut HashMap<String, L2Entry>,
    last_bbo: &mut HashMap<String, BboEntry>,
    active_l2_params: &ActiveL2Params,
    l2_param_guards: &mut HashMap<L2SnapshotParams, L2ParamGuard>,
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
            let msg = subscription.handle_immediate_snapshot(listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
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
        if let Some(snapshot_msg) = snapshot_msg {
            return send_socket_message(socket, snapshot_msg).await;
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
    ) -> Result<Option<ServerResponse>> {
        if let Self::L4Book { coin } = self {
            // Snapshot ONLY the requested coin. The old path cloned the entire
            // multi-book (every coin, every order) under the listener lock,
            // stalling event processing for hundreds of milliseconds per
            // l4Book subscribe.
            let snapshot = listener.lock().await.compute_snapshot_for_coin(&Coin::new(coin));
            if let Some((time, height, coin_snapshot)) = snapshot {
                let levels =
                    coin_snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot {
                    coin: coin.clone(),
                    time,
                    height,
                    levels,
                })));
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
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
}
