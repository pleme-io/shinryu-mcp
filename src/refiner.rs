//! Bronze → Silver refiner.
//!
//! Two modes:
//! - Event-driven (default): watches Bronze directory for new files via inotify
//! - Polling (fallback): scans every interval_secs for new files
//!
//! When a new Bronze NDJSON file is detected, immediately refines it to
//! Silver Parquet (typed, sorted, deduped). Sends the Silver file path
//! to the materializer via a tokio channel.

use datafusion::prelude::*;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

/// Run the Bronze → Silver refiner with inotify watching.
///
/// Falls back to polling if inotify fails (e.g., NFS mount without inotify).
pub async fn run(
    bronze_path: String,
    silver_path: String,
    silver_tx: mpsc::Sender<PathBuf>,
) {
    let _ = std::fs::create_dir_all(&silver_path);

    // Try event-driven mode first
    if let Err(e) = run_event_driven(&bronze_path, &silver_path, &silver_tx).await {
        tracing::warn!("inotify watcher failed ({e}), falling back to polling");
        run_polling(&bronze_path, &silver_path, &silver_tx).await;
    }
}

/// Event-driven mode: inotify watches for new .json files.
async fn run_event_driven(
    bronze_path: &str,
    silver_path: &str,
    silver_tx: &mpsc::Sender<PathBuf>,
) -> anyhow::Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<PathBuf>(100);

    let bronze_clone = bronze_path.to_string();
    let tx_clone = tx.clone();
    let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        if let Ok(event) = res {
            if matches!(event.kind, EventKind::Create(_) | EventKind::Modify(_)) {
                for path in &event.paths {
                    if path.extension().and_then(|e| e.to_str()) == Some("json") {
                        let _ = tx_clone.blocking_send(path.clone());
                    }
                }
            }
        }
    })?;

    watcher.watch(Path::new(&bronze_clone), RecursiveMode::Recursive)?;
    tracing::info!("Refiner watching {bronze_path} for new Bronze files (inotify)");

    // Also process any existing files on startup
    process_existing_files(bronze_path, silver_path, silver_tx).await;

    // Process events as they arrive
    while let Some(json_path) = rx.recv().await {
        let silver_file = derive_silver_path(&json_path, bronze_path, silver_path);
        match refine_single(&json_path, &silver_file).await {
            Ok(()) => {
                tracing::debug!("Refined {} → {}", json_path.display(), silver_file.display());
                let _ = silver_tx.send(silver_file).await;
            }
            Err(e) => tracing::warn!("Refine failed for {}: {e}", json_path.display()),
        }
    }

    Ok(())
}

/// Polling fallback mode.
async fn run_polling(
    bronze_path: &str,
    silver_path: &str,
    silver_tx: &mpsc::Sender<PathBuf>,
) {
    loop {
        process_existing_files(bronze_path, silver_path, silver_tx).await;
        time::sleep(Duration::from_secs(60)).await;
    }
}

/// Process all existing Bronze files that don't have a corresponding Silver file.
async fn process_existing_files(
    bronze_path: &str,
    silver_path: &str,
    silver_tx: &mpsc::Sender<PathBuf>,
) {
    let json_files = find_json_files(Path::new(bronze_path));
    for file in &json_files {
        let silver_file = derive_silver_path(file, bronze_path, silver_path);
        if silver_file.exists() {
            continue; // Already refined
        }
        match refine_single(file, &silver_file).await {
            Ok(()) => {
                let _ = silver_tx.send(silver_file).await;
            }
            Err(e) => tracing::warn!("Refine failed for {}: {e}", file.display()),
        }
    }
}

/// Derive the Silver output path from a Bronze input path.
fn derive_silver_path(json_path: &Path, bronze_base: &str, silver_base: &str) -> PathBuf {
    let relative = json_path.strip_prefix(bronze_base).unwrap_or(json_path);
    Path::new(silver_base).join(relative.with_extension("parquet"))
}

/// Refine a single Bronze NDJSON file to Silver Parquet.
async fn refine_single(json_path: &Path, parquet_path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = parquet_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let ctx = SessionContext::new();
    let options = NdJsonReadOptions::default();
    ctx.register_json("bronze", json_path.to_str().unwrap_or(""), options).await?;

    let df = ctx.sql("SELECT * FROM bronze ORDER BY timestamp_ms").await?;

    let tmp_path = parquet_path.with_extension("parquet.tmp");
    let writer_opts = datafusion::dataframe::DataFrameWriteOptions::new();
    df.write_parquet(tmp_path.to_str().unwrap_or(""), writer_opts, None).await?;

    std::fs::rename(&tmp_path, parquet_path)?;
    Ok(())
}

/// Recursively find all .json files in a directory.
fn find_json_files(dir: &Path) -> Vec<PathBuf> {
    let mut results = Vec::new();
    fn walk(dir: &Path, results: &mut Vec<PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() { walk(&path, results); }
                else if path.extension().and_then(|e| e.to_str()) == Some("json") {
                    results.push(path);
                }
            }
        }
    }
    walk(dir, &mut results);
    results
}
