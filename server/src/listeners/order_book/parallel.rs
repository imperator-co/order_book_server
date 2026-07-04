// HFT-optimized parallel file watcher
// Each event source runs on its own thread for maximum throughput

use crate::types::node_data::EventSource;
use log::{error, info};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    thread,
    time::Duration,
};

/// Message sent from file watcher threads to the main processor
#[derive(Debug)]
pub(crate) enum FileEvent {
    OrderStatus(String),
    OrderDiff(String),
    Fill(String),
    /// Startup-backfill lines: data that was already on disk when the watcher
    /// started, above the node's persisted height. These are cached for
    /// snapshot replay but NEVER applied to a live book - they are older than
    /// the live stream, and applying e.g. a stale size update out of order
    /// would corrupt the book.
    BackfillOrderStatus(String),
    BackfillOrderDiff(String),
    /// The watcher had to discard buffered data (oversized partial line). The
    /// book may have missed events and must be re-synced from a snapshot.
    Desync(EventSource),
}

/// Cheap block-height extraction without a full JSON parse: streaming lines
/// embed `"block_number":N` in their fixed header (after the two timestamps,
/// well before the events array). Returns None when the field can't be found -
/// callers treat that conservatively (keep the line / stop walking).
fn extract_block_number(line: &str) -> Option<u64> {
    let idx = line.find("\"block_number\":")?;
    let rest = line[idx + "\"block_number\":".len()..].trim_start();
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    rest[..end].parse().ok()
}

/// Block height of the first line in `path`, read from the head of the file.
/// 8 KB is far beyond where `block_number` sits in the line header.
fn first_block_number(path: &std::path::Path) -> Option<u64> {
    let mut file = File::open(path).ok()?;
    let mut buf = [0_u8; 8192];
    let n = file.read(&mut buf).ok()?;
    extract_block_number(&String::from_utf8_lossy(&buf[..n]))
}

/// Hard cap on a single un-terminated JSON line. The streaming files write
/// newline-delimited JSON; this bound is a safety net against a corrupt/partial
/// flush from the node that would otherwise let `partial_line` grow without
/// limit and OOM the host.
const MAX_PARTIAL_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Wall-clock milliseconds since the unix epoch, for the watcher health
/// timestamps. (The previous `Instant::now().elapsed()` measured elapsed time
/// since *now* - always ~0 - making the health values meaningless.)
pub(super) fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// File reader state for a single source
struct FileReader {
    current_path: Option<PathBuf>,
    // Open handle to current_path, reused across reads. Re-opening per modify
    // event (plus per poll-timeout fallback) cost an open+stat syscall pair
    // thousands of times per second per watcher thread.
    file: Option<File>,
    file_position: u64,
    partial_line: String,
    base_dir: PathBuf, // Base streaming directory to scan for new files
    // Set when buffered data had to be discarded (oversized partial line);
    // drained by take_desynced so the watcher can notify the listener.
    desynced: bool,
}

impl FileReader {
    fn new(base_dir: PathBuf) -> Self {
        Self {
            current_path: None,
            file: None,
            file_position: 0,
            partial_line: String::new(),
            base_dir,
            desynced: false,
        }
    }

    /// True once if data was discarded since the last call.
    fn take_desynced(&mut self) -> bool {
        std::mem::take(&mut self.desynced)
    }

    /// Every streaming file, newest first: day directories (YYYYMMDD -
    /// lexicographic order is chronological) descending, files within a day by
    /// mtime descending. Only the two most recent days are considered - the
    /// backfill never needs to reach further back than the snapshot source's
    /// staleness, which is minutes.
    fn all_files_newest_first(&self) -> Vec<PathBuf> {
        let hourly_dir = self.base_dir.join("hourly");
        let mut day_dirs: Vec<PathBuf> = std::fs::read_dir(&hourly_dir)
            .map(|entries| entries.flatten().map(|e| e.path()).filter(|p| p.is_dir()).collect())
            .unwrap_or_default();
        day_dirs.sort();
        let mut files = Vec::new();
        for day_dir in day_dirs.iter().rev().take(2) {
            let mut day_files: Vec<(std::time::SystemTime, PathBuf)> = std::fs::read_dir(day_dir)
                .map(|entries| {
                    entries
                        .flatten()
                        .map(|e| e.path())
                        .filter(|p| p.is_file())
                        .filter_map(|p| {
                            let mtime = p.metadata().and_then(|m| m.modified()).ok()?;
                            Some((mtime, p))
                        })
                        .collect()
                })
                .unwrap_or_default();
            day_files.sort();
            files.extend(day_files.into_iter().rev().map(|(_, path)| path));
        }
        files
    }

    /// One-shot startup backfill. Streams (oldest-first) every complete line
    /// already on disk whose block height exceeds `min_height`, then positions
    /// live tracking exactly at the backfill cut so no line is skipped or read
    /// twice. Returns false if `emit` reported a closed channel.
    ///
    /// This closes the startup drift window: the old behavior seeked straight
    /// to EOF, so lines written between the node's last persisted state (what
    /// the initial snapshot is computed from) and watcher start were never read
    /// at all. The snapshot height is always >= the node's persisted height at
    /// boot, so filtering by `min_height` (that persisted height) over-covers;
    /// the replay in init_from_snapshot then filters exactly by snapshot height.
    fn backfill_and_track(&mut self, min_height: u64, emit: &mut dyn FnMut(String) -> bool) -> bool {
        /// More candidate files than this means the snapshot source lags hours
        /// behind the stream - bail out and let the desync re-sync loop surface
        /// the node-level problem instead of reading unbounded history.
        const MAX_BACKFILL_FILES: usize = 4;

        let files = self.all_files_newest_first();
        let Some(newest) = files.first().cloned() else {
            return true; // no files yet; live tracking starts on the first inotify event
        };

        // Walk newest -> oldest collecting candidate files. A file whose FIRST
        // line is already above the floor is entirely relevant AND the next
        // older file may end above the floor too - keep walking. A file that
        // starts at or below the floor is the last possible contributor.
        let mut included = Vec::new();
        for path in &files {
            if included.len() >= MAX_BACKFILL_FILES {
                error!(
                    "Backfill would span more than {MAX_BACKFILL_FILES} files (snapshot source extremely stale); \
                     flagging desync so the book re-syncs once ready"
                );
                self.desynced = true;
                break;
            }
            included.push(path.clone());
            match first_block_number(path) {
                Some(height) if height > min_height => {} // whole file above the floor
                _ => break, // starts at/below the floor (or unreadable head): last candidate
            }
        }
        included.reverse(); // stream oldest first to preserve event order

        // Record the live-tracking cut BEFORE reading the newest file, so lines
        // appended while the backfill runs are picked up by live tracking
        // rather than read twice.
        let recorded_len = std::fs::metadata(&newest).map(|m| m.len()).unwrap_or(0);
        let mut cut_pos = recorded_len;

        for path in &included {
            let is_newest = *path == newest;
            match File::open(path) {
                Ok(file) => {
                    let mut buf_reader = std::io::BufReader::new(file);
                    let mut pos: u64 = 0;
                    loop {
                        let mut line = String::new();
                        match std::io::BufRead::read_line(&mut buf_reader, &mut line) {
                            Ok(0) => break, // EOF
                            Ok(n) => {
                                let line_end = pos + n as u64;
                                // A line crossing the recorded cut belongs to live tracking.
                                if is_newest && line_end > recorded_len {
                                    break;
                                }
                                // A partial tail (no newline yet) is also left for live tracking.
                                if !line.ends_with('\n') {
                                    break;
                                }
                                pos = line_end;
                                let trimmed = line.trim_end();
                                if !trimmed.is_empty()
                                    && trimmed.starts_with('{')
                                    && trimmed.ends_with('}')
                                    && extract_block_number(trimmed).is_none_or(|h| h > min_height)
                                    && !emit(trimmed.to_string())
                                {
                                    return false;
                                }
                            }
                            Err(err) => {
                                error!("Backfill read error on {}: {err}", path.display());
                                break;
                            }
                        }
                    }
                    if is_newest {
                        cut_pos = pos;
                    }
                }
                Err(err) => {
                    error!("Backfill failed to open {}: {err}", path.display());
                }
            }
        }

        // Live tracking continues exactly where the backfill stopped.
        self.current_path = Some(newest);
        self.file = None;
        self.file_position = cut_pos;
        self.partial_line.clear();
        true
    }

    /// Find the latest file in the streaming directory tree
    /// Scans hourly/YYYYMMDD/HH structure and returns the most recently modified file
    fn find_latest_file(&self) -> Option<PathBuf> {
        let hourly_dir = self.base_dir.join("hourly");
        if !hourly_dir.exists() {
            return None;
        }

        // Find the latest day directory
        let mut latest_day: Option<PathBuf> = None;
        if let Ok(entries) = std::fs::read_dir(&hourly_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if latest_day.is_none() || path > latest_day.clone().unwrap() {
                        latest_day = Some(path);
                    }
                }
            }
        }

        let day_dir = latest_day?;

        // Find the latest hour file in this day
        let mut latest_file: Option<PathBuf> = None;
        let mut latest_mtime: Option<std::time::SystemTime> = None;

        if let Ok(entries) = std::fs::read_dir(&day_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(metadata) = path.metadata() {
                        if let Ok(mtime) = metadata.modified() {
                            if latest_mtime.is_none() || mtime > latest_mtime.unwrap() {
                                latest_mtime = Some(mtime);
                                latest_file = Some(path);
                            }
                        }
                    }
                }
            }
        }

        latest_file
    }

    /// Check if there's a newer file than what we're currently tracking
    fn check_for_newer_file(&mut self) -> Option<PathBuf> {
        if let Some(latest) = self.find_latest_file() {
            if let Some(ref current) = self.current_path {
                if latest != *current {
                    match (latest.metadata(), current.metadata()) {
                        // Check if the new file has data (modification time is newer)
                        (Ok(latest_meta), Ok(current_meta)) => {
                            if let (Ok(latest_mtime), Ok(current_mtime)) =
                                (latest_meta.modified(), current_meta.modified())
                            {
                                if latest_mtime > current_mtime {
                                    return Some(latest);
                                }
                            }
                        }
                        // The tracked file no longer exists (deleted, or it was a
                        // transient path that vanished — observed when a node
                        // upgrade briefly created a stray entry in the streaming
                        // dir): the mtime comparison can never succeed again, so
                        // without this arm the reader is wedged on the ghost path
                        // forever — every event logs an open error and no live
                        // data flows. Fail over to the newest existing file.
                        (Ok(_), Err(_)) => return Some(latest),
                        _ => {}
                    }
                }
            } else {
                // No current file, use the latest
                return Some(latest);
            }
        }
        None
    }

    /// Process file modification - read new data and return lines
    fn on_modify(&mut self) -> Vec<String> {
        static MODIFY_COUNT: AtomicU64 = AtomicU64::new(0);
        let count = MODIFY_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut lines = Vec::new();
        if let Some(ref path) = self.current_path {
            // Open once and reuse the handle; a fresh fstat on the cached handle
            // still observes appended data (the node only ever appends).
            if self.file.is_none() {
                match File::open(path) {
                    Ok(file) => self.file = Some(file),
                    Err(err) => {
                        error!("Failed to open {}: {err}", path.display());
                        return lines;
                    }
                }
            }
            let mut read_failed = false;
            if let Some(file) = self.file.as_mut() {
                // Get fresh file size from the handle
                if let Ok(metadata) = file.metadata() {
                    let file_size = metadata.len();

                    // Only read if there's new data
                    if file_size > self.file_position {
                        // Log every read attempt
                        if count % 10_000 == 0 {
                            log::debug!(
                                "on_modify #{}: reading {} bytes (pos {} -> {})",
                                count,
                                file_size - self.file_position,
                                self.file_position,
                                file_size
                            );
                        }

                        if file.seek(SeekFrom::Start(self.file_position)).is_ok() {
                            let mut buf = String::new();
                            match file.read_to_string(&mut buf) {
                                Ok(bytes_read) => {
                                    if bytes_read > 0 {
                                        // Update position
                                        self.file_position += bytes_read as u64;

                                        // Prepend any partial line from last read
                                        let full_buf = std::mem::take(&mut self.partial_line) + &buf;

                                        // Debug logging
                                        let line_count = full_buf.lines().count();
                                        let ends_newline = buf.ends_with('\n');
                                        if count % 10_000 == 0 {
                                            log::debug!(
                                                "on_modify #{}: read {} bytes, {} lines, ends_newline={}",
                                                count, bytes_read, line_count, ends_newline
                                            );
                                        }

                                        // Only the unterminated tail may go to `partial_line`.
                                        // A newline-TERMINATED line that fails the JSON shape
                                        // check is complete-but-corrupt: buffering it (the old
                                        // behavior) prepended the garbage to the next read and
                                        // corrupted the following valid line too. Discard it and
                                        // flag the data loss so the book re-syncs.
                                        for segment in full_buf.split_inclusive('\n') {
                                            if segment.ends_with('\n') {
                                                let line = segment.trim_end();
                                                if line.is_empty() {
                                                    continue;
                                                }
                                                if line.starts_with('{') && line.ends_with('}') {
                                                    lines.push(line.to_string());
                                                } else {
                                                    error!(
                                                        "discarding malformed terminated line ({} bytes); flagging desync",
                                                        line.len()
                                                    );
                                                    self.desynced = true;
                                                }
                                            } else {
                                                // Unterminated tail - buffer until the newline arrives.
                                                self.partial_line = segment.to_string();
                                            }
                                        }

                                        // Bound the partial-line buffer. If the upstream goes wedged
                                        // mid-JSON (corrupt flush, mmap weirdness, multi-MB single line),
                                        // we'd otherwise grow `partial_line` until we OOM. Drop, flag the
                                        // data loss so the book re-syncs, and resync on the next newline.
                                        if self.partial_line.len() > MAX_PARTIAL_LINE_BYTES {
                                            error!(
                                                "partial_line exceeded {} bytes ({} bytes buffered); discarding and resyncing",
                                                MAX_PARTIAL_LINE_BYTES,
                                                self.partial_line.len()
                                            );
                                            self.partial_line.clear();
                                            self.desynced = true;
                                        }

                                        // Log result
                                        if count % 10_000 == 0 {
                                            log::debug!("on_modify #{}: returning {} lines", count, lines.len());
                                        }
                                    }
                                }
                                Err(err) => {
                                    error!("Read error: {}", err);
                                    read_failed = true;
                                }
                            }
                        }
                    }
                }
            }
            // Drop a handle that failed to read so the next call re-opens fresh.
            if read_failed {
                self.file = None;
            }
        }
        lines
    }

    /// Switch to a new file (on create event)
    fn on_create(&mut self, path: &PathBuf) -> Vec<String> {
        // Drain the old file until it goes quiet: a single read raced the
        // node's final appends (anything written between the read and the
        // switch was silently lost). Each pass observes the size at read time,
        // so the loop ends only after a read that saw no new data.
        let mut old_lines = self.on_modify();
        loop {
            let more = self.on_modify();
            if more.is_empty() {
                break;
            }
            old_lines.extend(more);
        }

        // Start tracking new file from beginning
        self.current_path = Some(path.clone());
        self.file = None;
        self.file_position = 0;
        self.partial_line.clear();

        old_lines
    }

    /// Track an existing file (first event we see for it)
    fn start_tracking(&mut self, path: &PathBuf) {
        // Get current file size to start from end
        if let Ok(metadata) = std::fs::metadata(path) {
            self.file_position = metadata.len();
        } else {
            self.file_position = 0;
        }
        self.current_path = Some(path.clone());
        self.file = None;
        self.partial_line.clear();
    }
}

/// Spawn a file watcher thread for a single event source
/// Uses polling with inotify hints for streaming files
pub(super) fn spawn_file_watcher(
    source: EventSource,
    dir: PathBuf,
    tx: tokio::sync::mpsc::Sender<FileEvent>,
    last_event: Arc<AtomicU64>,
    backfill_min_height: u64,
) -> thread::JoinHandle<()> {
    let source_name = match source {
        EventSource::OrderStatuses => "OrderStatuses",
        EventSource::Fills => "Fills",
        EventSource::OrderDiffs => "OrderDiffs",
    };

    thread::spawn(move || {
        info!("{} watcher thread started for {:?}", source_name, dir);

        let mut reader = FileReader::new(dir.clone());

        // Create watcher with callback
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let mut watcher = match recommended_watcher(move |res: Result<Event, _>| {
            drop(event_tx.send(res));
        }) {
            Ok(w) => w,
            Err(err) => {
                error!("{} watcher failed to create: {}", source_name, err);
                return;
            }
        };

        if let Err(err) = watcher.watch(&dir, RecursiveMode::Recursive) {
            error!("{} watcher failed to start: {}", source_name, err);
            return;
        }

        // One-shot startup backfill: stream lines already on disk above the
        // height floor (everything at/below it is covered by the initial
        // snapshot), wrapped in Backfill* variants so the listener caches them
        // for snapshot replay without applying them to a live book. Fills are
        // excluded: they never mutate the book and are not replayed. Runs after
        // watch() so appends racing the backfill are still notified; ordering
        // per source is preserved because backfill lines enter the channel
        // before any live line.
        if backfill_min_height > 0 && !matches!(source, EventSource::Fills) {
            let mut sent = 0_usize;
            let channel_open = reader.backfill_and_track(backfill_min_height, &mut |line| {
                let evt = match source {
                    EventSource::OrderStatuses => FileEvent::BackfillOrderStatus(line),
                    EventSource::OrderDiffs => FileEvent::BackfillOrderDiff(line),
                    EventSource::Fills => unreachable!("fills are excluded from backfill"),
                };
                sent += 1;
                tx.blocking_send(evt).is_ok()
            });
            if !channel_open {
                error!("{source_name} channel closed during backfill, exiting");
                return;
            }
            info!("{source_name} backfill complete: {sent} lines above height {backfill_min_height}");
            if reader.take_desynced() && tx.blocking_send(FileEvent::Desync(source)).is_err() {
                error!("{source_name} channel closed, exiting");
                return;
            }
        }

        // HFT CRITICAL: Use fast polling (1ms) for lowest latency
        // inotify provides immediate notifications when available, but polling ensures we never wait
        let poll_interval = Duration::from_millis(1);

        // Main event loop - primarily event-driven with fallback polling
        let mut poll_count = 0u64;
        loop {
            poll_count += 1;

            // Wait for inotify events (with fallback timeout)
            match event_rx.recv_timeout(poll_interval) {
                Ok(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let path = &event.paths[0];
                        if path.is_dir() {
                            continue;
                        }

                        if event.kind.is_create() {
                            info!("{} new file: {:?}", source_name, path.file_name());
                            let old_lines = reader.on_create(path);
                            for line in old_lines {
                                let evt = match source {
                                    EventSource::OrderStatuses => FileEvent::OrderStatus(line),
                                    EventSource::OrderDiffs => FileEvent::OrderDiff(line),
                                    EventSource::Fills => FileEvent::Fill(line),
                                };
                                if tx.blocking_send(evt).is_err() {
                                    error!("{} channel closed, exiting", source_name);
                                    return;
                                }
                            }
                        } else if reader.current_path.is_none() {
                            // First time seeing this file
                            info!("{} tracking: {:?}", source_name, path.file_name());
                            reader.start_tracking(path);
                        }

                        // EVENT-DRIVEN: Read data when inotify fires modify event
                        let lines = reader.on_modify();
                        for line in lines {
                            let event = match source {
                                EventSource::OrderStatuses => FileEvent::OrderStatus(line),
                                EventSource::OrderDiffs => FileEvent::OrderDiff(line),
                                EventSource::Fills => FileEvent::Fill(line),
                            };

                            if tx.blocking_send(event).is_err() {
                                error!("{} channel closed, exiting", source_name);
                                return;
                            }

                            // Update health timestamp
                            last_event.store(now_unix_ms(), AtomicOrdering::Relaxed);
                        }
                    }
                }
                Ok(Err(err)) => {
                    error!("{} watcher error: {}", source_name, err);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // Fallback polling - safety net for missed events
                    // This runs every 500ms instead of every 10ms
                    let lines = reader.on_modify();
                    for line in lines {
                        let event = match source {
                            EventSource::OrderStatuses => FileEvent::OrderStatus(line),
                            EventSource::OrderDiffs => FileEvent::OrderDiff(line),
                            EventSource::Fills => FileEvent::Fill(line),
                        };

                        if tx.blocking_send(event).is_err() {
                            error!("{} channel closed, exiting", source_name);
                            return;
                        }

                        last_event.store(now_unix_ms(), AtomicOrdering::Relaxed);
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    error!("{} event channel closed, exiting", source_name);
                    return;
                }
            }

            // If the reader had to discard buffered data, tell the listener so it
            // can re-sync the book from a fresh snapshot.
            if reader.take_desynced() && tx.blocking_send(FileEvent::Desync(source)).is_err() {
                error!("{source_name} channel closed, exiting");
                return;
            }

            // Every 100000 polls, log status
            if poll_count % 100_000 == 0 {
                if let Some(ref path) = reader.current_path {
                    if let Ok(file) = File::open(path) {
                        if let Ok(metadata) = file.metadata() {
                            log::debug!(
                                "{} poll {} - pos {} / size {}",
                                source_name,
                                poll_count,
                                reader.file_position,
                                metadata.len()
                            );
                        }
                    }
                }
            }

            // Every 10000 polls (~10 seconds), check for newer files (handles day rotation)
            if poll_count % 10_000 == 0 {
                if let Some(newer_file) = reader.check_for_newer_file() {
                    info!("{} detected newer file (day rotation?): {:?}", source_name, newer_file.file_name());
                    // Switch to the new file
                    let old_lines = reader.on_create(&newer_file);
                    for line in old_lines {
                        let evt = match source {
                            EventSource::OrderStatuses => FileEvent::OrderStatus(line),
                            EventSource::OrderDiffs => FileEvent::OrderDiff(line),
                            EventSource::Fills => FileEvent::Fill(line),
                        };
                        if tx.blocking_send(evt).is_err() {
                            error!("{} channel closed, exiting", source_name);
                            return;
                        }
                    }
                }
            }
        }
    })
}

/// Start all 3 file watcher threads, returns receiver for events
/// Uses *_streaming directories (for --stream-with-block-info mode)
/// `backfill_min_height` is the startup backfill floor (the node's persisted
/// height at boot); 0 disables the backfill.
///
/// The watcher threads send straight into a tokio mpsc via `blocking_send` -
/// the old crossbeam channel + spawn_blocking bridge added a thread and a
/// queue hop per event for nothing.
pub(crate) fn start_parallel_file_watchers(
    data_dir: PathBuf,
    backfill_min_height: u64,
) -> (tokio::sync::mpsc::Receiver<FileEvent>, Vec<thread::JoinHandle<()>>, Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>)
{
    // BOUNDED so a slow downstream actually back-pressures the file readers
    // (blocking_send parks until a slot frees up). Under processing stalls an
    // unbounded queue would accumulate multi-KB JSON strings indefinitely - a
    // primary OOM vector; the events sit on disk, no need to mirror them in
    // memory.
    let (tx, rx) = tokio::sync::mpsc::channel(10_000);
    let mut handles = Vec::new();

    // Health monitoring
    let last_order_status = Arc::new(AtomicU64::new(0));
    let last_fills = Arc::new(AtomicU64::new(0));
    let last_order_diffs = Arc::new(AtomicU64::new(0));

    // HFT mode uses streaming directories (for --stream-with-block-info)
    // Spawn watcher for OrderStatuses
    let order_statuses_dir = EventSource::OrderStatuses.event_source_dir_streaming(&data_dir);
    info!("OrderStatuses dir: {:?}", order_statuses_dir);
    handles.push(spawn_file_watcher(
        EventSource::OrderStatuses,
        order_statuses_dir,
        tx.clone(),
        last_order_status.clone(),
        backfill_min_height,
    ));

    // Spawn watcher for Fills (no backfill: fills never mutate the book)
    let fills_dir = EventSource::Fills.event_source_dir_streaming(&data_dir);
    info!("Fills dir: {:?}", fills_dir);
    handles.push(spawn_file_watcher(EventSource::Fills, fills_dir, tx.clone(), last_fills.clone(), 0));

    // Spawn watcher for OrderDiffs
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir_streaming(&data_dir);
    info!("OrderDiffs dir: {:?}", order_diffs_dir);
    handles.push(spawn_file_watcher(
        EventSource::OrderDiffs,
        order_diffs_dir,
        tx,
        last_order_diffs.clone(),
        backfill_min_height,
    ));

    (rx, handles, last_order_status, last_fills, last_order_diffs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("obs_watcher_test_{}_{name}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn append(path: &PathBuf, data: &str) {
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
        file.write_all(data.as_bytes()).unwrap();
    }

    #[test]
    fn test_on_modify_reads_appended_lines_and_buffers_partials() {
        let dir = test_dir("appended");
        let path = dir.join("0");
        append(&path, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path); // position = EOF (0 bytes so far)

        append(&path, "{\"a\":1}\n{\"b\":2}\n{\"c\":");
        let lines = reader.on_modify();
        assert_eq!(lines, vec!["{\"a\":1}".to_string(), "{\"b\":2}".to_string()]);

        // The unterminated tail was buffered, and the SAME cached handle picks up
        // the continuation on the next read (persistent-fd reuse path).
        append(&path, "3}\n");
        let lines = reader.on_modify();
        assert_eq!(lines, vec!["{\"c\":3}".to_string()]);
        assert!(!reader.take_desynced());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_start_tracking_existing_content_starts_at_eof() {
        let dir = test_dir("eof");
        let path = dir.join("0");
        append(&path, "{\"old\":1}\n");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path);
        assert!(reader.on_modify().is_empty(), "pre-existing content is skipped (covered by the snapshot)");
        append(&path, "{\"new\":2}\n");
        assert_eq!(reader.on_modify(), vec!["{\"new\":2}".to_string()]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_on_create_reads_old_tail_then_switches() {
        let dir = test_dir("rotate");
        let old = dir.join("0");
        let new = dir.join("1");
        append(&old, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&old);

        append(&old, "{\"tail\":1}\n");
        append(&new, "{\"first\":2}\n");
        let old_lines = reader.on_create(&new);
        assert_eq!(old_lines, vec!["{\"tail\":1}".to_string()], "old file's tail is drained before switching");
        // After the switch the new file is read from position 0.
        assert_eq!(reader.on_modify(), vec!["{\"first\":2}".to_string()]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_malformed_terminated_line_is_discarded_not_buffered() {
        // Regression: a newline-TERMINATED line failing the JSON shape check
        // used to be stored into `partial_line` as if it were a partial tail,
        // then got prepended to the next read - corrupting the next valid line
        // too. It must be discarded (flagging desync) and later lines kept.
        let dir = test_dir("malformed_mid");
        let path = dir.join("0");
        append(&path, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path);

        append(&path, "{\"a\":1}\ngarbage\n{\"b\":2}\n");
        let lines = reader.on_modify();
        assert_eq!(lines, vec!["{\"a\":1}".to_string(), "{\"b\":2}".to_string()]);
        assert!(reader.take_desynced(), "a discarded complete line is data loss");

        // The garbage must NOT contaminate the next read.
        append(&path, "{\"c\":3}\n");
        assert_eq!(reader.on_modify(), vec!["{\"c\":3}".to_string()]);
        assert!(!reader.take_desynced());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_partial_line_overflow_sets_desynced() {
        let dir = test_dir("overflow");
        let path = dir.join("0");
        append(&path, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path);

        // A single unterminated line larger than the cap must be discarded and
        // flagged as data loss (the listener re-syncs the book on this signal).
        let huge = "{".repeat(MAX_PARTIAL_LINE_BYTES + 2);
        append(&path, &huge);
        assert!(reader.on_modify().is_empty());
        assert!(reader.take_desynced(), "discarding buffered data must flag a desync");
        assert!(!reader.take_desynced(), "the flag is drained by take_desynced");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_check_for_newer_file_fails_over_when_tracked_file_vanishes() {
        let dir = test_dir("ghost");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        let live = day.join("0");
        append(&live, "{\"a\":1}\n");

        // Track a transient path that then disappears (e.g. a stray entry a
        // node upgrade briefly created in the streaming dir).
        let ghost = dir.join("20240101");
        append(&ghost, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&ghost);
        std::fs::remove_file(&ghost).unwrap();

        // Without the vanished-file arm this returns None forever (mtime
        // comparison needs both files to exist) and the reader never leaves
        // the ghost path.
        assert_eq!(reader.check_for_newer_file(), Some(live));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_check_for_newer_file_keeps_current_when_it_is_the_newest() {
        let dir = test_dir("keep_current");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        let live = day.join("0");
        append(&live, "{\"a\":1}\n");

        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&live);
        assert_eq!(reader.check_for_newer_file(), None, "still the newest existing file");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ==================== Startup backfill ====================

    fn bn_line(height: u64) -> String {
        format!("{{\"block_number\":{height},\"events\":[]}}")
    }

    #[test]
    fn test_extract_block_number() {
        assert_eq!(extract_block_number(r#"{"local_time":"t","block_time":"t","block_number":123,"events":[]}"#), Some(123));
        assert_eq!(extract_block_number(r#"{"block_number": 7}"#), Some(7), "whitespace after the colon is fine");
        assert_eq!(extract_block_number(r#"{"height":9}"#), None);
        assert_eq!(extract_block_number(r#"{"block_number":"x"}"#), None);
        assert_eq!(extract_block_number(""), None);
    }

    #[test]
    fn test_backfill_emits_lines_above_floor_and_cuts_for_live_tracking() {
        let dir = test_dir("backfill_floor");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        let path = day.join("0");
        append(&path, &format!("{}\n{}\n{}\n", bn_line(5), bn_line(10), bn_line(15)));

        let mut reader = FileReader::new(dir.clone());
        let mut emitted = Vec::new();
        assert!(reader.backfill_and_track(10, &mut |line| {
            emitted.push(line);
            true
        }));
        // Only the line above the floor; <=10 is covered by the snapshot.
        assert_eq!(emitted, vec![bn_line(15)]);
        assert!(!reader.take_desynced());

        // Live tracking continues exactly at the cut: a fresh append is the next
        // and only thing on_modify returns (no skip, no double-read).
        append(&path, &format!("{}\n", bn_line(20)));
        assert_eq!(reader.on_modify(), vec![bn_line(20)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_backfill_walks_back_into_older_file_until_floor() {
        let dir = test_dir("backfill_walk");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        // Older file straddles the floor; newer file is entirely above it.
        let older = day.join("0");
        append(&older, &format!("{}\n{}\n", bn_line(8), bn_line(15)));
        thread::sleep(Duration::from_millis(20)); // distinct mtimes
        let newer = day.join("1");
        append(&newer, &format!("{}\n{}\n", bn_line(20), bn_line(30)));

        let mut reader = FileReader::new(dir.clone());
        let mut emitted = Vec::new();
        assert!(reader.backfill_and_track(10, &mut |line| {
            emitted.push(line);
            true
        }));
        // Oldest-first ordering, floor-filtered across both files.
        assert_eq!(emitted, vec![bn_line(15), bn_line(20), bn_line(30)]);
        assert!(!reader.take_desynced());

        // Live tracking is on the NEWER file, positioned at its end.
        append(&newer, &format!("{}\n", bn_line(31)));
        assert_eq!(reader.on_modify(), vec![bn_line(31)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_backfill_leaves_partial_tail_for_live_tracking() {
        let dir = test_dir("backfill_partial");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        let path = day.join("0");
        // Complete line above the floor + an unterminated tail.
        append(&path, &format!("{}\n{{\"block_number\":2", bn_line(15)));

        let mut reader = FileReader::new(dir.clone());
        let mut emitted = Vec::new();
        assert!(reader.backfill_and_track(10, &mut |line| {
            emitted.push(line);
            true
        }));
        assert_eq!(emitted, vec![bn_line(15)], "the partial tail must not be emitted");

        // Completing the tail makes live tracking deliver the whole line intact.
        append(&path, "0,\"events\":[]}\n");
        assert_eq!(reader.on_modify(), vec![bn_line(20)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_backfill_keeps_lines_with_unparseable_height() {
        // Height extraction is an optimization; a line it can't read must be
        // kept (the exact filter happens at replay, after a full parse).
        let dir = test_dir("backfill_unparseable");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        let path = day.join("0");
        append(&path, "{\"no_height\":true}\n");

        let mut reader = FileReader::new(dir.clone());
        let mut emitted = Vec::new();
        assert!(reader.backfill_and_track(10, &mut |line| {
            emitted.push(line);
            true
        }));
        assert_eq!(emitted, vec!["{\"no_height\":true}".to_string()]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_backfill_caps_file_walk_and_flags_desync() {
        let dir = test_dir("backfill_cap");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        // Five files, every one entirely above the floor: the walk must stop at
        // the cap and flag a desync (snapshot source pathologically stale).
        for (i, h) in [10u64, 20, 30, 40, 50].iter().enumerate() {
            append(&day.join(i.to_string()), &format!("{}\n", bn_line(*h)));
            thread::sleep(Duration::from_millis(15));
        }

        let mut reader = FileReader::new(dir.clone());
        let mut emitted = Vec::new();
        assert!(reader.backfill_and_track(1, &mut |line| {
            emitted.push(line);
            true
        }));
        // The four newest files are streamed (oldest of them first).
        assert_eq!(emitted, vec![bn_line(20), bn_line(30), bn_line(40), bn_line(50)]);
        assert!(reader.take_desynced(), "an out-of-cap walk means lost data and must trigger a re-sync");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_backfill_with_no_files_is_a_noop() {
        let dir = test_dir("backfill_empty");
        std::fs::create_dir_all(dir.join("hourly")).unwrap();
        let mut reader = FileReader::new(dir.clone());
        let mut emitted = Vec::new();
        assert!(reader.backfill_and_track(10, &mut |line| {
            emitted.push(line);
            true
        }));
        assert!(emitted.is_empty());
        assert!(reader.current_path.is_none(), "live tracking still starts on the first inotify event");
        std::fs::remove_dir_all(&dir).ok();
    }
}
