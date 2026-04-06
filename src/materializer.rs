//! Silver → Gold materializer.
//!
//! Receives notifications from the refiner when new Silver Parquet files
//! are written. Runs aggregate views and writes results to Gold.
//! Notifies the session manager to re-register tables.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::metrics::Metrics;
use crate::session::SessionEvent;
use crate::udfs;

/// Run the materializer, consuming Silver notifications and producing Gold views.
pub async fn run(
    silver_path: PathBuf,
    gold_path: PathBuf,
    mut silver_rx: mpsc::Receiver<PathBuf>,
    session_tx: mpsc::Sender<SessionEvent>,
    metrics: Arc<Metrics>,
) {
    info!("Materializer started");

    if let Err(e) = std::fs::create_dir_all(&gold_path) {
        error!(error = %e, "Failed to create gold directory");
        return;
    }

    while let Some(_) = silver_rx.recv().await {
        // Debounce: wait a moment then drain any pending notifications
        tokio::time::sleep(Duration::from_secs(5)).await;
        while silver_rx.try_recv().is_ok() {}

        info!("Materializing Gold views from Silver");

        match materialize(&silver_path, &gold_path, &metrics).await {
            Ok(()) => {
                metrics.files_materialized.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // Notify session manager to re-register
                let _ = session_tx.send(SessionEvent::GoldUpdated).await;
                let _ = session_tx.send(SessionEvent::SilverChanged).await;
                info!("Gold materialization complete");
            }
            Err(e) => {
                warn!(error = %e, "Gold materialization failed");
            }
        }
    }
}

async fn materialize(
    silver_path: &PathBuf,
    gold_path: &PathBuf,
    _metrics: &Metrics,
) -> anyhow::Result<()> {
    if !silver_path.exists() {
        return Ok(());
    }

    // Check if Silver has any Parquet files
    let has_parquet = walkdir::WalkDir::new(silver_path)
        .into_iter()
        .filter_map(Result::ok)
        .any(|e| e.file_type().is_file() && e.path().extension().is_some_and(|ext| ext == "parquet"));

    if !has_parquet {
        return Ok(());
    }

    let ctx = SessionContext::new();
    udfs::register_all(&ctx);

    let opts = ListingOptions::new(Arc::new(datafusion::datasource::file_format::parquet::ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_collect_stat(true);
    ctx.register_listing_table("events", silver_path.to_string_lossy().as_ref(), opts, None, None).await?;

    // Materialize experiment summaries
    let summaries_sql = r#"
        SELECT
            experiment_id,
            scenario,
            MAX(CAST(running AS BIGINT)) as max_running,
            MAX(CAST(injected AS BIGINT)) as max_injected,
            MAX(elapsed_ms) as duration_ms,
            MAX(CAST(peak_running AS BIGINT)) as peak_running,
            COUNT(*) as event_count
        FROM events
        WHERE event_type IS NOT NULL
        GROUP BY experiment_id, scenario
    "#;

    if let Err(e) = materialize_view(&ctx, summaries_sql, &gold_path.join("experiment_summaries.parquet")).await {
        warn!(error = %e, "Failed to materialize experiment_summaries");
    }

    // Materialize phase timings
    let phases_sql = r#"
        SELECT
            experiment_id,
            scenario,
            event_type,
            elapsed_ms as duration_ms,
            timestamp
        FROM events
        WHERE event_type = 'PHASE_COMPLETE'
        ORDER BY experiment_id, scenario, timestamp
    "#;

    if let Err(e) = materialize_view(&ctx, phases_sql, &gold_path.join("phase_timings.parquet")).await {
        warn!(error = %e, "Failed to materialize phase_timings");
    }

    Ok(())
}

async fn materialize_view(
    ctx: &SessionContext,
    sql: &str,
    output_path: &PathBuf,
) -> anyhow::Result<()> {
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(());
    }

    let schema = batches[0].schema();
    let tmp_path = output_path.with_extension("tmp");
    let file = std::fs::File::create(&tmp_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    for batch in &batches {
        writer.write(batch)?;
    }
    writer.close()?;

    std::fs::rename(&tmp_path, output_path)?;
    info!(?output_path, "Materialized Gold view");
    Ok(())
}
