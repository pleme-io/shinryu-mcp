//! Bronze → Silver refiner.
//!
//! Consumes FsEvent::NewFile from the directory watcher broadcast channel.
//! Reads NDJSON from Bronze, writes Parquet to Silver.
//! Does a catchup scan on startup for files missed before the watcher started.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use datafusion::prelude::*;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use crate::metrics::Metrics;
use crate::watcher::FsEvent;

/// Run the refiner, consuming filesystem events and promoting Bronze → Silver.
pub async fn run(
    bronze_path: PathBuf,
    silver_path: PathBuf,
    mut fs_events: broadcast::Receiver<FsEvent>,
    silver_tx: mpsc::Sender<PathBuf>,
    metrics: Arc<Metrics>,
) {
    info!("Refiner started");

    // Ensure silver directory exists
    if let Err(e) = std::fs::create_dir_all(&silver_path) {
        error!(error = %e, "Failed to create silver directory");
        return;
    }

    let mut processed: HashSet<PathBuf> = HashSet::new();

    // Startup catchup: process any Bronze files that don't have Silver counterparts
    catchup_scan(&bronze_path, &silver_path, &mut processed, &silver_tx, &metrics).await;

    // Main event loop
    loop {
        match fs_events.recv().await {
            Ok(FsEvent::NewFile(path)) => {
                if !is_bronze_json(&path, &bronze_path) {
                    continue;
                }
                if processed.contains(&path) {
                    continue;
                }
                // Wait for file to stabilize (Vector may still be writing)
                if !file_is_stable(&path, Duration::from_secs(2)) {
                    debug!(?path, "File not stable yet, will retry on next event");
                    continue;
                }
                if let Err(e) = refine_file(&path, &bronze_path, &silver_path, &metrics).await {
                    warn!(error = %e, ?path, "Failed to refine file");
                    metrics.refine_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    processed.insert(path.clone());
                    let _ = silver_tx.send(silver_path.clone()).await;
                }
            }
            Ok(FsEvent::NewDirectory(_)) => {
                // New Bronze subdirectory — do a catchup scan
                catchup_scan(&bronze_path, &silver_path, &mut processed, &silver_tx, &metrics).await;
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(missed = n, "Refiner lagged, running catchup scan");
                catchup_scan(&bronze_path, &silver_path, &mut processed, &silver_tx, &metrics).await;
            }
            Err(broadcast::error::RecvError::Closed) => {
                info!("Watcher channel closed, refiner exiting");
                break;
            }
        }
    }
}

/// Scan Bronze for unprocessed .json files and refine them.
async fn catchup_scan(
    bronze_path: &Path,
    silver_path: &Path,
    processed: &mut HashSet<PathBuf>,
    silver_tx: &mpsc::Sender<PathBuf>,
    metrics: &Metrics,
) {
    if !bronze_path.exists() {
        return;
    }

    let json_files: Vec<PathBuf> = walkdir::WalkDir::new(bronze_path)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file() && e.path().extension().is_some_and(|ext| ext == "json"))
        .map(|e| e.path().to_path_buf())
        .collect();

    let mut refined = 0;
    for path in json_files {
        if processed.contains(&path) {
            continue;
        }
        if !file_is_stable(&path, Duration::from_secs(2)) {
            continue;
        }
        match refine_file(&path, bronze_path, silver_path, metrics).await {
            Ok(()) => {
                processed.insert(path);
                refined += 1;
            }
            Err(e) => {
                warn!(error = %e, "Catchup refine failed");
            }
        }
    }

    if refined > 0 {
        info!(refined, "Catchup scan complete");
        let _ = silver_tx.send(silver_path.to_path_buf()).await;
    }
}

/// Refine a single Bronze NDJSON file to Silver Parquet.
async fn refine_file(
    json_path: &Path,
    bronze_root: &Path,
    silver_root: &Path,
    metrics: &Metrics,
) -> anyhow::Result<()> {
    // Compute silver output path: mirror bronze directory structure
    let relative = json_path.strip_prefix(bronze_root)?;
    let parquet_name = relative.with_extension("parquet");
    let silver_file = silver_root.join(&parquet_name);

    // Skip if already refined
    if silver_file.exists() {
        return Ok(());
    }

    // Ensure parent directory exists
    if let Some(parent) = silver_file.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Read NDJSON via DataFusion
    let ctx = SessionContext::new();
    let json_dir = json_path.parent().unwrap_or(bronze_root);
    let filename = json_path.file_name().unwrap().to_string_lossy();

    ctx.register_json("bronze_src", json_dir.to_string_lossy().as_ref(),
        NdJsonReadOptions::default().file_extension(&filename),
    ).await?;

    let df = ctx.sql("SELECT * FROM bronze_src").await?;
    let batches = df.collect().await?;

    if batches.is_empty() {
        return Ok(());
    }

    // Write Parquet
    let schema = batches[0].schema();
    let tmp_path = silver_file.with_extension("tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    let mut total_rows = 0u64;
    for batch in &batches {
        total_rows += batch.num_rows() as u64;
        writer.write(batch)?;
    }
    writer.close()?;

    // Atomic rename
    std::fs::rename(&tmp_path, &silver_file)?;

    metrics.files_refined.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    metrics.rows_ingested.fetch_add(total_rows, std::sync::atomic::Ordering::Relaxed);

    info!(rows = total_rows, ?silver_file, "Refined Bronze → Silver");
    Ok(())
}

fn is_bronze_json(path: &Path, bronze_root: &Path) -> bool {
    path.starts_with(bronze_root)
        && path.extension().is_some_and(|ext| ext == "json")
}

fn file_is_stable(path: &Path, min_age: Duration) -> bool {
    path.metadata()
        .and_then(|m| m.modified())
        .map(|t| t.elapsed().unwrap_or_default() >= min_age)
        .unwrap_or(false)
}
