//! Reactive directory watcher with broadcast event bus.
//!
//! Watches Bronze/Silver/Gold directories for new files and subdirectories.
//! Proactively creates directories. Re-establishes watches for new subdirs.
//! Falls back to polling on NFS/EBS where inotify doesn't work.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

/// File system event types for the broadcast bus.
#[derive(Debug, Clone)]
pub enum FsEvent {
    /// A new file was created or written.
    NewFile(PathBuf),
    /// A new directory appeared.
    NewDirectory(PathBuf),
}

/// Configuration for the directory watcher.
pub struct WatcherConfig {
    /// Use polling instead of inotify. Set SHINRYU_POLL_MODE=1 for NFS/EBS.
    pub poll_mode: bool,
    /// Poll interval when in poll mode. Default: 5 seconds.
    pub poll_interval: Duration,
    /// Broadcast channel capacity.
    pub channel_capacity: usize,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            poll_mode: std::env::var("SHINRYU_POLL_MODE").is_ok(),
            poll_interval: Duration::from_secs(5),
            channel_capacity: 256,
        }
    }
}

/// Directory watcher that emits FsEvent on a broadcast channel.
pub struct DirectoryWatcher {
    tx: broadcast::Sender<FsEvent>,
    _watcher: Box<dyn Watcher + Send>,
    watched_dirs: Arc<Mutex<HashSet<PathBuf>>>,
}

impl DirectoryWatcher {
    /// Create a new watcher for the analytics directory tree.
    /// Proactively creates bronze/, silver/, gold/ subdirectories.
    pub fn new(analytics_path: &Path, config: WatcherConfig) -> anyhow::Result<Self> {
        // Proactively create tier directories
        for tier in ["bronze", "silver", "gold"] {
            let dir = analytics_path.join(tier);
            std::fs::create_dir_all(&dir)?;
            info!(?dir, "Ensured tier directory exists");
        }

        let (tx, _) = broadcast::channel(config.channel_capacity);
        let watched_dirs = Arc::new(Mutex::new(HashSet::new()));

        let tx_clone = tx.clone();
        let watched_clone = watched_dirs.clone();

        let mut watcher: Box<dyn Watcher + Send> = if config.poll_mode {
            info!(interval = ?config.poll_interval, "Using poll watcher");
            let w = notify::PollWatcher::new(
                move |res: Result<Event, notify::Error>| {
                    handle_event(res, &tx_clone, &watched_clone);
                },
                notify::Config::default().with_poll_interval(config.poll_interval),
            )?;
            Box::new(w)
        } else {
            match RecommendedWatcher::new(
                {
                    let tx = tx_clone.clone();
                    let wd = watched_clone.clone();
                    move |res: Result<Event, notify::Error>| {
                        handle_event(res, &tx, &wd);
                    }
                },
                notify::Config::default(),
            ) {
                Ok(w) => {
                    info!("Using native file watcher");
                    Box::new(w)
                }
                Err(e) => {
                    warn!(error = %e, "Native watcher failed, falling back to poll watcher");
                    let w = notify::PollWatcher::new(
                        move |res: Result<Event, notify::Error>| {
                            handle_event(res, &tx_clone, &watched_clone);
                        },
                        notify::Config::default().with_poll_interval(config.poll_interval),
                    )?;
                    Box::new(w)
                }
            }
        };

        // Watch the analytics root recursively
        watcher.watch(analytics_path, RecursiveMode::Recursive)?;
        watched_dirs.lock().unwrap().insert(analytics_path.to_path_buf());

        info!(?analytics_path, "Directory watcher started");

        Ok(Self {
            tx,
            _watcher: watcher,
            watched_dirs,
        })
    }

    /// Subscribe to the event bus.
    pub fn subscribe(&self) -> broadcast::Receiver<FsEvent> {
        self.tx.subscribe()
    }
}

fn handle_event(
    res: Result<Event, notify::Error>,
    tx: &broadcast::Sender<FsEvent>,
    watched_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) {
    let event = match res {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "Watcher error");
            return;
        }
    };

    for path in &event.paths {
        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                if path.is_dir() {
                    let mut dirs = watched_dirs.lock().unwrap();
                    if dirs.insert(path.clone()) {
                        debug!(?path, "New directory discovered");
                        let _ = tx.send(FsEvent::NewDirectory(path.clone()));
                    }
                } else if path.is_file() {
                    let _ = tx.send(FsEvent::NewFile(path.clone()));
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn watches_new_files() {
        let tmp = TempDir::new().unwrap();
        let bronze = tmp.path().join("bronze");
        std::fs::create_dir_all(&bronze).unwrap();

        let watcher = DirectoryWatcher::new(tmp.path(), WatcherConfig {
            poll_mode: true,
            poll_interval: Duration::from_millis(100),
            channel_capacity: 16,
        }).unwrap();

        let mut rx = watcher.subscribe();

        // Write a file
        std::fs::write(bronze.join("test.json"), "{}").unwrap();

        // Should receive event within poll interval
        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
        assert!(event.is_ok(), "Should receive file event");
    }

    #[test]
    fn creates_tier_directories() {
        let tmp = TempDir::new().unwrap();
        let _watcher = DirectoryWatcher::new(tmp.path(), WatcherConfig {
            poll_mode: true,
            ..Default::default()
        }).unwrap();

        assert!(tmp.path().join("bronze").exists());
        assert!(tmp.path().join("silver").exists());
        assert!(tmp.path().join("gold").exists());
    }

    #[test]
    fn subscribe_returns_receiver() {
        let tmp = TempDir::new().unwrap();
        let watcher = DirectoryWatcher::new(tmp.path(), WatcherConfig {
            poll_mode: true,
            poll_interval: Duration::from_secs(60),
            channel_capacity: 8,
        }).unwrap();
        let _rx1 = watcher.subscribe();
        let _rx2 = watcher.subscribe();
    }

    #[test]
    fn watcher_config_default_values() {
        let config = WatcherConfig::default();
        assert_eq!(config.poll_interval, Duration::from_secs(5));
        assert_eq!(config.channel_capacity, 256);
    }

    #[tokio::test]
    async fn watches_new_directory() {
        let tmp = TempDir::new().unwrap();
        let watcher = DirectoryWatcher::new(tmp.path(), WatcherConfig {
            poll_mode: true,
            poll_interval: Duration::from_millis(100),
            channel_capacity: 16,
        }).unwrap();

        let mut rx = watcher.subscribe();

        std::fs::create_dir_all(tmp.path().join("bronze").join("new_subdir")).unwrap();

        let event = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
        assert!(event.is_ok(), "should receive event for new directory");
    }

    #[test]
    fn fs_event_debug_format() {
        let event = FsEvent::NewFile(PathBuf::from("/test/file.json"));
        let debug = format!("{:?}", event);
        assert!(debug.contains("NewFile"));
        assert!(debug.contains("file.json"));
    }
}
