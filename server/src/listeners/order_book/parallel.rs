// HFT-optimized parallel file watcher
// Each event source runs on its own thread for maximum throughput

use crate::types::node_data::EventSource;
use crossbeam_channel::{Sender, unbounded};
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
    time::{Duration, Instant},
};

/// Message sent from file watcher threads to the main processor
#[derive(Debug)]
pub(crate) enum FileEvent {
    OrderStatus(String),
    OrderDiff(String),
    Fill(String),
}

/// File reader state for a single source
struct FileReader {
    current_path: Option<PathBuf>,
    file_position: u64,
    partial_line: String,
    base_dir: PathBuf, // Base streaming directory to scan for new files
}

impl FileReader {
    fn new(base_dir: PathBuf) -> Self {
        Self { current_path: None, file_position: 0, partial_line: String::new(), base_dir }
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
                    // Check if the new file has data (modification time is newer)
                    if let (Ok(latest_meta), Ok(current_meta)) = (latest.metadata(), current.metadata()) {
                        if let (Ok(latest_mtime), Ok(current_mtime)) = (latest_meta.modified(), current_meta.modified())
                        {
                            if latest_mtime > current_mtime {
                                return Some(latest);
                            }
                        }
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
            // Open file, seek to last position, read new data
            if let Ok(mut file) = File::open(path) {
                // Get fresh file size from opened handle
                if let Ok(metadata) = file.metadata() {
                    let file_size = metadata.len();

                    // Only read if there's new data
                    if file_size > self.file_position {
                        // Log every read attempt
                        if count % 10_000 == 0 {
                            info!(
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
                                            info!(
                                                "on_modify #{}: read {} bytes, {} lines, ends_newline={}",
                                                count, bytes_read, line_count, ends_newline
                                            );
                                        }

                                        let mut line_iter = full_buf.lines().peekable();

                                        while let Some(line) = line_iter.next() {
                                            if line_iter.peek().is_some() {
                                                // Not the last line
                                                if !line.is_empty() {
                                                    // Validate JSON structure - must start with { and end with }
                                                    if line.starts_with('{') && line.ends_with('}') {
                                                        lines.push(line.to_string());
                                                    } else {
                                                        // Incomplete JSON - store for next read
                                                        self.partial_line = line.to_string();
                                                    }
                                                }
                                            } else {
                                                // Last line - might be partial
                                                if buf.ends_with('\n') && !line.is_empty() {
                                                    // Validate JSON structure
                                                    if line.starts_with('{') && line.ends_with('}') {
                                                        lines.push(line.to_string());
                                                    } else {
                                                        // Incomplete JSON - store for next read
                                                        self.partial_line = line.to_string();
                                                    }
                                                } else if !line.is_empty() {
                                                    // Partial line without newline
                                                    self.partial_line = line.to_string();
                                                }
                                            }
                                        }

                                        // Log result
                                        if count % 10_000 == 0 {
                                            info!("on_modify #{}: returning {} lines", count, lines.len());
                                        }
                                    }
                                }
                                Err(err) => {
                                    error!("Read error: {}", err);
                                }
                            }
                        }
                    }
                }
            }
        }
        lines
    }

    /// Switch to a new file (on create event)
    fn on_create(&mut self, path: &PathBuf) -> Vec<String> {
        // First, read remaining data from old file
        let old_lines = self.on_modify();

        // Start tracking new file from beginning
        self.current_path = Some(path.clone());
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
        self.partial_line.clear();
    }
}

/// Spawn a file watcher thread for a single event source
/// Uses polling with inotify hints for streaming files
pub(super) fn spawn_file_watcher(
    source: EventSource,
    dir: PathBuf,
    tx: Sender<FileEvent>,
    last_event: Arc<AtomicU64>,
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
                                if tx.send(evt).is_err() {
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

                            if tx.send(event).is_err() {
                                error!("{} channel closed, exiting", source_name);
                                return;
                            }

                            // Update health timestamp
                            last_event.store(Instant::now().elapsed().as_millis() as u64, AtomicOrdering::Relaxed);
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

                        if tx.send(event).is_err() {
                            error!("{} channel closed, exiting", source_name);
                            return;
                        }

                        last_event.store(Instant::now().elapsed().as_millis() as u64, AtomicOrdering::Relaxed);
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    error!("{} event channel closed, exiting", source_name);
                    return;
                }
            }

            // Every 100000 polls, log status
            if poll_count % 100_000 == 0 {
                if let Some(ref path) = reader.current_path {
                    if let Ok(file) = File::open(path) {
                        if let Ok(metadata) = file.metadata() {
                            info!(
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
                        if tx.send(evt).is_err() {
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
pub(crate) fn start_parallel_file_watchers(
    data_dir: PathBuf,
) -> (crossbeam_channel::Receiver<FileEvent>, Vec<thread::JoinHandle<()>>, Arc<AtomicU64>, Arc<AtomicU64>, Arc<AtomicU64>)
{
    let (tx, rx) = unbounded();
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
    ));

    // Spawn watcher for Fills
    let fills_dir = EventSource::Fills.event_source_dir_streaming(&data_dir);
    info!("Fills dir: {:?}", fills_dir);
    handles.push(spawn_file_watcher(EventSource::Fills, fills_dir, tx.clone(), last_fills.clone()));

    // Spawn watcher for OrderDiffs
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir_streaming(&data_dir);
    info!("OrderDiffs dir: {:?}", order_diffs_dir);
    handles.push(spawn_file_watcher(EventSource::OrderDiffs, order_diffs_dir, tx, last_order_diffs.clone()));

    (rx, handles, last_order_status, last_fills, last_order_diffs)
}
