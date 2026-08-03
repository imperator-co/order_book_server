//! Prometheus metrics for orderbook server monitoring
//!
//! Provides comprehensive metrics for:
//! - WebSocket connections and subscriptions
//! - Event processing latency and throughput
//! - Orderbook health and state
//! - Errors and anomalies

use lazy_static::lazy_static;
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // ==================== CONNECTION METRICS ====================

    /// Current number of active WebSocket connections
    pub static ref WS_CONNECTIONS_ACTIVE: IntGauge = IntGauge::new(
        "ws_connections_active",
        "Number of active WebSocket connections"
    ).expect("metric can be created");

    /// Total WebSocket connections since startup
    pub static ref WS_CONNECTIONS_TOTAL: IntCounter = IntCounter::new(
        "ws_connections_total",
        "Total WebSocket connections since startup"
    ).expect("metric can be created");

    /// Active subscriptions by type (bbo, l2Book, l4Book, trades)
    pub static ref WS_SUBSCRIPTIONS_ACTIVE: IntGaugeVec = IntGaugeVec::new(
        Opts::new("ws_subscriptions_active", "Active subscriptions by type"),
        &["type"]
    ).expect("metric can be created");

    // ==================== LATENCY METRICS ====================

    /// BBO broadcast latency in seconds
    pub static ref BBO_BROADCAST_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new("bbo_broadcast_latency_seconds", "BBO broadcast latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).expect("metric can be created");

    /// L2 broadcast latency in seconds
    pub static ref L2_BROADCAST_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new("l2_broadcast_latency_seconds", "L2 broadcast latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).expect("metric can be created");

    /// Number of coins rebuilt per L2 broadcast (conflation batch size). Tracks how
    /// many coins changed within each 50ms throttle window; spikes toward the
    /// universe size indicate full-universe windows (lock-duration pressure).
    pub static ref L2_CONFLATION_BATCH_SIZE: Histogram = Histogram::with_opts(
        HistogramOpts::new("l2_conflation_batch_size", "Coins rebuilt per L2 broadcast")
            .buckets(vec![1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 150.0])
    ).expect("metric can be created");

    /// Event processing latency by type
    pub static ref EVENT_PROCESSING_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("event_processing_latency_seconds", "Event processing latency")
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
        &["event_type"]
    ).expect("metric can be created");

    // ==================== THROUGHPUT METRICS ====================

    /// Events processed by type (orders, diffs, fills)
    pub static ref EVENTS_PROCESSED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("events_processed_total", "Total events processed"),
        &["type"]
    ).expect("metric can be created");

    /// Broadcasts sent by channel type
    pub static ref BROADCASTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("broadcasts_total", "Total broadcasts sent"),
        &["channel"]
    ).expect("metric can be created");

    /// Fill legs dropped because no matching counterpart followed (trades pairing).
    /// ~0 in steady state; growth means the node stopped emitting the two legs
    /// of a match adjacently and trade pairing should be revisited.
    pub static ref TRADES_UNPAIRED_FILLS_TOTAL: IntCounter = IntCounter::new(
        "trades_unpaired_fills_total", "Fill legs dropped without a matching counterpart"
    ).expect("metric can be created");

    /// WebSocket messages sent
    pub static ref MESSAGES_SENT_TOTAL: IntCounter = IntCounter::new(
        "messages_sent_total",
        "Total WebSocket messages sent"
    ).expect("metric can be created");

    // ==================== HEALTH METRICS ====================

    /// Current orderbook block height
    pub static ref ORDERBOOK_HEIGHT: IntGauge = IntGauge::new(
        "orderbook_height",
        "Current orderbook block height"
    ).expect("metric can be created");

    /// Orderbook timestamp in milliseconds
    pub static ref ORDERBOOK_TIME_MS: IntGauge = IntGauge::new(
        "orderbook_time_ms",
        "Orderbook timestamp in milliseconds"
    ).expect("metric can be created");

    /// Pending order statuses in HFT cache
    pub static ref PENDING_ORDERS_CACHE: IntGauge = IntGauge::new(
        "pending_orders_cache_size",
        "Pending order statuses in HFT cache"
    ).expect("metric can be created");

    /// Pending order diffs in HFT cache
    pub static ref PENDING_DIFFS_CACHE: IntGauge = IntGauge::new(
        "pending_diffs_cache_size",
        "Pending order diffs in HFT cache"
    ).expect("metric can be created");

    /// Broadcast channel lag (receivers behind)
    pub static ref CHANNEL_LAG: IntGauge = IntGauge::new(
        "broadcast_channel_lag",
        "Broadcast channel lag"
    ).expect("metric can be created");

    // ==================== ERROR METRICS ====================

    /// Parse errors by type
    pub static ref PARSE_ERRORS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("parse_errors_total", "Total parse errors"),
        &["type"]
    ).expect("metric can be created");

    /// WebSocket send errors
    pub static ref WS_SEND_ERRORS_TOTAL: IntCounter = IntCounter::new(
        "ws_send_errors_total",
        "Total WebSocket send errors"
    ).expect("metric can be created");

    /// Times the order book was marked out-of-sync (by reason). Every increment
    /// triggers a background snapshot re-fetch that rebuilds the book, so a
    /// non-zero rate here means events were lost but the book self-healed.
    pub static ref ORDERBOOK_DESYNCS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_desyncs_total", "Times the order book was marked out-of-sync"),
        &["reason"]
    ).expect("metric can be created");

    /// Messages dropped due to channel lag
    pub static ref CHANNEL_DROPS_TOTAL: IntCounter = IntCounter::new(
        "channel_drops_total",
        "Messages dropped due to channel lag"
    ).expect("metric can be created");

    /// New diffs whose insertBefore anchor was not resting at the level; the
    /// order was rested at the back of the level instead and a re-sync was
    /// scheduled, since queue priority may be wrong until then.
    pub static ref INSERT_BEFORE_FALLBACK_TOTAL: IntCounter = IntCounter::new(
        "insert_before_fallback_total",
        "New diffs whose insertBefore anchor was missing; order rested at back of level"
    ).expect("metric can be created");

    // ==================== FILE WATCHER METRICS ====================

    /// File events received per source (orders, diffs, fills)
    pub static ref FILE_EVENTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("file_events_total", "File events received by source"),
        &["source"]
    ).expect("metric can be created");

    /// Lines parsed from files by source
    pub static ref FILE_LINES_PARSED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("file_lines_parsed_total", "Lines parsed from files by source"),
        &["source"]
    ).expect("metric can be created");

    // ==================== ORDERBOOK STATS ====================

    /// Total orders currently in the orderbook
    pub static ref ORDERBOOK_ORDERS_TOTAL: IntGauge = IntGauge::new(
        "orderbook_orders_total",
        "Total orders currently in orderbook"
    ).expect("metric can be created");

    /// Number of coins tracked in orderbook
    pub static ref ORDERBOOK_COINS_COUNT: IntGauge = IntGauge::new(
        "orderbook_coins_count",
        "Number of coins tracked in orderbook"
    ).expect("metric can be created");

    /// BBO changes per coin (top 5 tracked individually)
    pub static ref BBO_CHANGES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("bbo_changes_total", "BBO changes by coin"),
        &["coin"]
    ).expect("metric can be created");

    // ==================== RESYNC & LOCK METRICS ====================

    /// Wall-clock duration of each re-sync phase. `fetch_dump` is the hl-node
    /// CLI dump, `parse` the snapshot JSON deserialize, `build` the replacement
    /// book construction, `chase` the off-lock replay loop, `commit` the final
    /// under-lock swap, and `total` the whole fetch-to-install cycle.
    pub static ref RESYNC_PHASE_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new("resync_phase_duration_seconds", "Duration of snapshot re-sync phases")
            .buckets(vec![0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]),
        &["phase"]
    ).expect("metric can be created");

    /// Time the ingest loop spends waiting to acquire the listener lock. The
    /// lock is shared with connection setup and l4Book snapshot builds, so
    /// sustained waits here mean ingest (and everything queued behind it) is
    /// being convoyed by other lock holders.
    pub static ref LISTENER_LOCK_WAIT: Histogram = Histogram::with_opts(
        HistogramOpts::new("listener_lock_wait_seconds", "Ingest-loop wait to acquire the listener lock")
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
    ).expect("metric can be created");

    /// 1 once the first snapshot install completed and the book is serving.
    /// Read by /health so the endpoint never has to take the listener lock.
    pub static ref ORDERBOOK_READY: IntGauge = IntGauge::new(
        "orderbook_ready",
        "1 when the order book has installed a snapshot and is serving"
    ).expect("metric can be created");

    /// Unix epoch ms of the last event batch applied to the live book. /health
    /// derives a staleness signal from this so consumers can tell "connected
    /// but not receiving data" apart from a healthy quiet market.
    pub static ref LAST_EVENT_APPLIED_MS: IntGauge = IntGauge::new(
        "last_event_applied_ms",
        "Unix epoch ms of the last event batch applied to the live book"
    ).expect("metric can be created");

    /// 1 while a snapshot fetch + install cycle is in flight, so stalls can be
    /// correlated with re-syncs from the metrics alone.
    pub static ref ORDERBOOK_RESYNC_IN_FLIGHT: IntGauge = IntGauge::new(
        "orderbook_resync_in_flight",
        "1 while a snapshot fetch/install cycle is running"
    ).expect("metric can be created");

    // ==================== UPTIME & SYSTEM ====================

    /// Server uptime in seconds
    pub static ref UPTIME_SECONDS: IntCounter = IntCounter::new(
        "uptime_seconds",
        "Server uptime in seconds"
    ).expect("metric can be created");

    /// Server start timestamp (unix seconds)
    pub static ref SERVER_START_TIME: IntGauge = IntGauge::new(
        "server_start_time_seconds",
        "Server start timestamp (unix seconds)"
    ).expect("metric can be created");

    /// Broadcast channel receiver count
    pub static ref BROADCAST_RECEIVERS: IntGauge = IntGauge::new(
        "broadcast_receivers",
        "Number of broadcast channel receivers"
    ).expect("metric can be created");

}

/// Register all metrics with the registry
pub fn register_metrics() {
    // Connection metrics
    REGISTRY.register(Box::new(WS_CONNECTIONS_ACTIVE.clone())).ok();
    REGISTRY.register(Box::new(WS_CONNECTIONS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WS_SUBSCRIPTIONS_ACTIVE.clone())).ok();

    // Latency metrics
    REGISTRY.register(Box::new(BBO_BROADCAST_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(L2_BROADCAST_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(L2_CONFLATION_BATCH_SIZE.clone())).ok();
    REGISTRY.register(Box::new(EVENT_PROCESSING_LATENCY.clone())).ok();

    // Throughput metrics
    REGISTRY.register(Box::new(EVENTS_PROCESSED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(BROADCASTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(TRADES_UNPAIRED_FILLS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(MESSAGES_SENT_TOTAL.clone())).ok();

    // Health metrics
    REGISTRY.register(Box::new(ORDERBOOK_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_TIME_MS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_ORDERS_CACHE.clone())).ok();
    REGISTRY.register(Box::new(PENDING_DIFFS_CACHE.clone())).ok();
    REGISTRY.register(Box::new(CHANNEL_LAG.clone())).ok();

    // Error metrics
    REGISTRY.register(Box::new(PARSE_ERRORS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WS_SEND_ERRORS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(CHANNEL_DROPS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_DESYNCS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(INSERT_BEFORE_FALLBACK_TOTAL.clone())).ok();

    // File watcher metrics
    REGISTRY.register(Box::new(FILE_EVENTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(FILE_LINES_PARSED_TOTAL.clone())).ok();

    // Orderbook stats
    REGISTRY.register(Box::new(ORDERBOOK_ORDERS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_COINS_COUNT.clone())).ok();
    REGISTRY.register(Box::new(BBO_CHANGES_TOTAL.clone())).ok();

    // Resync & lock metrics
    REGISTRY.register(Box::new(RESYNC_PHASE_DURATION.clone())).ok();
    REGISTRY.register(Box::new(LISTENER_LOCK_WAIT.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_READY.clone())).ok();
    REGISTRY.register(Box::new(LAST_EVENT_APPLIED_MS.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_RESYNC_IN_FLIGHT.clone())).ok();

    // Uptime & system
    REGISTRY.register(Box::new(UPTIME_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(SERVER_START_TIME.clone())).ok();
    REGISTRY.register(Box::new(BROADCAST_RECEIVERS.clone())).ok();

    // Set server start time
    let start_time =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    SERVER_START_TIME.set(start_time);
}

/// Get metrics as Prometheus text format
pub fn gather_metrics() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
