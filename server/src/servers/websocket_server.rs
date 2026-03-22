use crate::{
    listeners::order_book::{
        InternalMessage, L2SnapshotParams, L2Snapshots, OrderBookListener, TimedSnapshots, hl_listen_hft,
    },
    metrics::{
        BBO_CHANGES_TOTAL, BROADCAST_RECEIVERS, BROADCASTS_TOTAL, CHANNEL_DROPS_TOTAL, CHANNEL_LAG,
        MESSAGES_SENT_TOTAL, ORDERBOOK_HEIGHT, WS_CONNECTIONS_ACTIVE, WS_CONNECTIONS_TOTAL, WS_SEND_ERRORS_TOTAL,
    },
    order_book::{Coin, Px, Snapshot, Sz},
    prelude::*,
    types::{
        Bbo, L2Book, L4Book, L4BookUpdates, L4Order, Trade,
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ClientMessage, DEFAULT_LEVELS, OrderUpdate, ServerResponse, Subscription, SubscriptionManager},
    },
};
use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
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

pub async fn run_websocket_server(config: ServerConfig) -> Result<()> {
    // Broadcast channel buffer: larger = less "channel lagged" errors on slower machines
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(256);

    // Market filter flags from config
    let market_filter = (config.include_perps, config.include_spot, config.include_hip3);
    let ignore_spot = !config.include_spot; // For OrderBookListener (legacy)
    let compression_level = config.compression_level;

    // Resolve data directory
    // Central task: listen to messages and forward them for distribution
    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        OrderBookListener::new(Some(internal_message_tx), ignore_spot)
    };
    let listener = Arc::new(Mutex::new(listener));
    {
        let listener = listener.clone();
        let config = config.clone();
        tokio::spawn(async move {
            info!("Starting HFT-optimized listener");
            let result = hl_listen_hft(listener, config).await;
            if let Err(err) = result {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));

    let start_time = std::time::Instant::now();
    let listener_for_health = listener.clone();

    let app: Router = Router::new()
        .route(
            "/ws",
            get({
                let internal_message_tx = internal_message_tx.clone();
                let bbo_only = config.bbo_only;
                let listener = listener.clone();
                move |ws_upgrade| async move {
                    ws_handler(
                        ws_upgrade,
                        internal_message_tx.clone(),
                        listener.clone(),
                        market_filter,
                        bbo_only,
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
                        r#"{{"status":"{}","uptime_seconds":{},"height":{},"connections":{}}}"
                    "#,
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

    if let Err(err) = axum::serve(tcp_listener, app).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    market_filter: (bool, bool, bool), // (include_perps, include_spot, include_hip3)
    bbo_only: bool,
    websocket_opts: yawc::Options,
) -> impl IntoResponse {
    let (resp, fut) = incoming.upgrade(websocket_opts).unwrap();
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, internal_message_tx, listener, market_filter, bbo_only).await
    });

    resp
}

async fn handle_socket(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    market_filter: (bool, bool, bool), // (include_perps, include_spot, include_hip3)
    bbo_only: bool,
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
    let mut universe = listener.lock().await.universe().into_iter().map(|c| c.value()).collect();
    // Track last BBO per coin to avoid sending duplicates (bid_px, bid_sz, ask_px, ask_sz)
    let mut last_bbo: HashMap<String, (String, String, String, String)> = HashMap::new();
    // Track last L2 hash per (coin, params) to avoid sending duplicates
    let mut last_l2_hash: HashMap<String, u64> = HashMap::new();
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        send_socket_message(&mut socket, msg).await;
        return;
    }
    loop {
        select! {
            recv_result = internal_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        match msg.as_ref() {
                            InternalMessage::Snapshot{ l2_snapshots, time } => {
                                universe = new_universe(l2_snapshots, market_filter.0, market_filter.1, market_filter.2);
                                for sub in manager.subscriptions() {
                                    // Skip BBO subs here - they get fast updates via BboUpdate
                                    if !matches!(sub, Subscription::Bbo { .. }) {
                                        send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time, &mut last_bbo, &mut last_l2_hash).await;
                                    }
                                }
                            },
                            InternalMessage::BboUpdate{ bbos, time } => {
                                // Fast path for BBO subscribers only
                                for sub in manager.subscriptions() {
                                    if let Subscription::Bbo { coin } = sub {
                                        send_ws_data_from_bbo(&mut socket, coin, bbos, *time, &mut last_bbo).await;
                                    }
                                }
                            },
                            InternalMessage::Fills{ batch } => {
                                let mut trades = coin_to_trades(batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_trades(&mut socket, sub, &mut trades).await;
                                }
                            },
                            InternalMessage::L4OrderDiffs{ batch } => {
                                // HFT mode: process diffs independently
                                let mut book_updates = coin_to_book_diffs_only(batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_book_updates(&mut socket, sub, &mut book_updates).await;
                                }
                            },
                            InternalMessage::L4OrderStatuses{ batch } => {
                                // HFT mode: process statuses independently
                                let mut book_updates = coin_to_book_statuses_only(batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_book_updates(&mut socket, sub, &mut book_updates).await;
                                }
                                // Also handle OrderUpdates subscriptions
                                for sub in manager.subscriptions() {
                                    send_ws_order_updates(&mut socket, sub, batch).await;
                                }
                            },
                        }

                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        CHANNEL_LAG.set(n as i64);
                        CHANNEL_DROPS_TOTAL.inc();
                        log::debug!("Receiver lagged: {n} messages");
                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        return;
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

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                match value {
                                    ClientMessage::Ping => {
                                        send_socket_message(&mut socket, ServerResponse::Pong).await;
                                    }
                                    _ => {
                                        receive_client_message(&mut socket, &mut manager, value, &universe, listener.clone(), bbo_only).await;
                                    }
                                }
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                send_socket_message(&mut socket, msg).await;
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
}

async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
    bbo_only: bool,
) {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
        ClientMessage::Ping => unreachable!("Ping is handled before receive_client_message"),
    };
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        let msg = ServerResponse::Error(format!("Invalid subscription: {sub}"));
        send_socket_message(socket, msg).await;
        return;
    }

    // In BBO-only mode, reject non-BBO subscriptions to save RAM
    if bbo_only {
        let is_bbo = matches!(&subscription, Subscription::Bbo { .. });
        if !is_bbo {
            let msg = ServerResponse::Error(
                "BBO-only mode: L2/L4/Trades subscriptions disabled. Only BBO subscriptions allowed.".to_string(),
            );
            send_socket_message(socket, msg).await;
            return;
        }
    }
    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => ("", manager.subscribe(subscription)),
        ClientMessage::Unsubscribe { .. } => ("un", manager.unsubscribe(subscription)),
        ClientMessage::Ping => unreachable!(),
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                    send_socket_message(socket, msg).await;
                    return;
                }
            }
        } else {
            None
        };
        let msg = ServerResponse::SubscriptionResponse(client_message);
        send_socket_message(socket, msg).await;
        if let Some(snapshot_msg) = snapshot_msg {
            send_socket_message(socket, snapshot_msg).await;
        }
    } else {
        let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub}"));
        send_socket_message(socket, msg).await;
    }
}

/// Fast BBO broadcast - directly from BBO HashMap without L2 snapshot computation
async fn send_ws_data_from_bbo(
    socket: &mut WebSocket,
    coin: &str,
    bbos: &HashMap<Coin, (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>)>,
    time: u64,
    last_bbo: &mut HashMap<String, (String, String, String, String)>,
) {
    let coin_key = Coin::new(coin);
    if let Some((best_bid, best_ask)) = bbos.get(&coin_key) {
        // Convert to Level format - Px and Sz implement Debug for formatting
        let bid = best_bid
            .as_ref()
            .map(|(px, sz, n)| crate::types::Level::new(format!("{:?}", px), format!("{:?}", sz), *n as usize));
        let ask = best_ask
            .as_ref()
            .map(|(px, sz, n)| crate::types::Level::new(format!("{:?}", px), format!("{:?}", sz), *n as usize));

        // Deduplication check
        let bid_px = bid.as_ref().map(|b| b.px().to_string()).unwrap_or_default();
        let bid_sz = bid.as_ref().map(|b| b.sz().to_string()).unwrap_or_default();
        let ask_px = ask.as_ref().map(|a| a.px().to_string()).unwrap_or_default();
        let ask_sz = ask.as_ref().map(|a| a.sz().to_string()).unwrap_or_default();
        let current = (bid_px, bid_sz, ask_px, ask_sz);

        if last_bbo.get(coin) != Some(&current) {
            last_bbo.insert(coin.to_string(), current);
            BBO_CHANGES_TOTAL.with_label_values(&[coin]).inc();
            BROADCASTS_TOTAL.with_label_values(&["bbo"]).inc();
            let bbo = Bbo { coin: coin.to_string(), time, bid, ask };
            let msg = ServerResponse::Bbo(bbo);
            send_socket_message(socket, msg).await;
        }
    }
}

async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) {
    let msg = serde_json::to_string(&msg);
    match msg {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
                WS_SEND_ERRORS_TOTAL.inc();
            } else {
                MESSAGES_SENT_TOTAL.inc();
            }
        }
        Err(err) => {
            error!("Server response serialization error: {err}");
        }
    }
}

// derive it from l2_snapshots because thats convenient
// Filters coins based on market type flags
fn new_universe(
    l2_snapshots: &L2Snapshots,
    include_perps: bool,
    include_spot: bool,
    include_hip3: bool,
) -> HashSet<String> {
    l2_snapshots
        .as_ref()
        .iter()
        .filter_map(|(c, _)| {
            let include =
                (c.is_perp() && include_perps) || (c.is_spot() && include_spot) || (c.is_hip3() && include_hip3);
            if include { Some(c.clone().value()) } else { None }
        })
        .collect()
}

async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>,
    time: u64,
    last_bbo: &mut HashMap<String, (String, String, String, String)>,
    last_l2_hash: &mut HashMap<String, u64>,
) {
    match subscription {
        Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } => {
            let snapshot = snapshot.get(&Coin::new(coin));
            if let Some(snapshot) =
                snapshot.and_then(|snapshot| snapshot.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
            {
                let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
                let snapshot = snapshot.truncate(n_levels);
                let snapshot = snapshot.export_inner_snapshot();

                // Hash the snapshot for dedup comparison
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                // Hash bids and asks using their Debug representation (simple but effective)
                format!("{:?}", snapshot).hash(&mut hasher);
                let current_hash = hasher.finish();

                // Create unique key for this subscription (coin + params)
                let key = format!("{}:{}:{}", coin, n_sig_figs.unwrap_or(0), mantissa.unwrap_or(0));

                if last_l2_hash.get(&key) != Some(&current_hash) {
                    last_l2_hash.insert(key, current_hash);
                    BROADCASTS_TOTAL.with_label_values(&["l2"]).inc();
                    let l2_book =
                        L2Book::from_l2_snapshot(coin.clone(), snapshot, time, *n_sig_figs, *mantissa, Some(n_levels));
                    let msg = ServerResponse::L2Book(l2_book);
                    send_socket_message(socket, msg).await;
                }
                // else: skip, L2 unchanged
            } else {
                error!("Coin {coin} not found");
            }
        }
        Subscription::Bbo { coin } => {
            // Get default snapshot (no aggregation)
            let snapshot = snapshot.get(&Coin::new(coin));
            if let Some(snapshot) = snapshot.and_then(|s| s.get(&L2SnapshotParams::new(None, None))) {
                let levels = snapshot.truncate(1).export_inner_snapshot();
                let bid = levels[0].first().cloned();
                let ask = levels[1].first().cloned();

                // Only send if BBO changed (dedupe identical messages)
                let bid_px = bid.as_ref().map(|b| b.px().to_string()).unwrap_or_default();
                let bid_sz = bid.as_ref().map(|b| b.sz().to_string()).unwrap_or_default();
                let ask_px = ask.as_ref().map(|a| a.px().to_string()).unwrap_or_default();
                let ask_sz = ask.as_ref().map(|a| a.sz().to_string()).unwrap_or_default();
                let current = (bid_px, bid_sz, ask_px, ask_sz);

                if last_bbo.get(coin) != Some(&current) {
                    last_bbo.insert(coin.clone(), current);
                    let bbo = Bbo { coin: coin.clone(), time, bid, ask };
                    let msg = ServerResponse::Bbo(bbo);
                    send_socket_message(socket, msg).await;
                }
                // else: skip, BBO unchanged
            }
        }
        _ => {}
    }
}

fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
    let fills = batch.clone().events();
    let mut trades = HashMap::new();

    // Convert each fill directly to a trade (no pairing)
    for fill in fills {
        let trade = Trade::from_single_fill(fill);
        let coin = trade.coin.clone();
        trades.entry(coin).or_insert_with(Vec::new).push(trade);
    }

    trades
}

/// HFT helper: convert order diffs batch to book updates (without statuses)
fn coin_to_book_diffs_only(diff_batch: &Batch<NodeDataOrderDiff>) -> HashMap<String, L4BookUpdates> {
    let diffs = diff_batch.clone().events();
    let time = diff_batch.block_time();
    let height = diff_batch.block_number();
    let mut updates = HashMap::new();
    for diff in diffs {
        let coin = diff.coin().value();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff);
    }
    updates
}

/// HFT helper: convert order statuses batch to book updates (without diffs)
fn coin_to_book_statuses_only(status_batch: &Batch<NodeDataOrderStatus>) -> HashMap<String, L4BookUpdates> {
    let statuses = status_batch.clone().events();
    let time = status_batch.block_time();
    let height = status_batch.block_number();
    let mut updates = HashMap::new();
    for status in statuses {
        let coin = status.order.coin.clone();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status);
    }
    updates
}

async fn send_ws_data_from_book_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_updates: &mut HashMap<String, L4BookUpdates>,
) {
    if let Subscription::L4Book { coin } = subscription {
        if let Some(updates) = book_updates.remove(coin) {
            BROADCASTS_TOTAL.with_label_values(&["l4"]).inc();
            let msg = ServerResponse::L4Book(L4Book::Updates(updates));
            send_socket_message(socket, msg).await;
        }
    }
}

async fn send_ws_data_from_trades(
    socket: &mut WebSocket,
    subscription: &Subscription,
    trades: &mut HashMap<String, Vec<Trade>>,
) {
    if let Subscription::Trades { coin } = subscription {
        if let Some(trades) = trades.remove(coin) {
            BROADCASTS_TOTAL.with_label_values(&["trades"]).inc();
            let msg = ServerResponse::Trades(trades);
            send_socket_message(socket, msg).await;
        }
    }
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
    ) -> Result<Option<ServerResponse>> {
        if let Self::L4Book { coin } = self {
            let snapshot = listener.lock().await.compute_snapshot();
            if let Some(TimedSnapshots { time, height, snapshot }) = snapshot {
                let requested_coin = Coin::new(coin);
                let filtered =
                    snapshot.value().into_iter().filter(|(c, _)| *c == requested_coin).collect::<Vec<_>>().pop();
                if let Some((found_coin, coin_snapshot)) = filtered {
                    let levels =
                        coin_snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                    return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot {
                        coin: found_coin.value(),
                        time,
                        height,
                        levels,
                    })));
                }
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
}

/// Send order updates to OrderUpdates subscribers filtered by user address
async fn send_ws_order_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    batch: &Batch<NodeDataOrderStatus>,
) {
    if let Subscription::OrderUpdates { user } = subscription {
        // Parse the user address from the subscription
        let user_addr = match user.parse::<alloy::primitives::Address>() {
            Ok(addr) => addr,
            Err(_) => return, // Invalid address, skip
        };

        let time = batch.block_time();
        let height = batch.block_number();
        let statuses = batch.clone().events();

        // Filter statuses for this specific user
        let user_updates: Vec<OrderUpdate> = statuses
            .into_iter()
            .filter(|status| status.user == user_addr)
            .map(|status| OrderUpdate::new(status.user, time, height, status))
            .collect();

        if !user_updates.is_empty() {
            let msg = ServerResponse::OrderUpdates(user_updates);
            send_socket_message(socket, msg).await;
        }
    }
}
