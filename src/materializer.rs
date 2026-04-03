//! Silver → Gold materializer.
//!
//! Channel-driven: receives Silver file paths from the refiner,
//! materializes Gold views when new data arrives.
//! No polling — zero CPU when idle.

use datafusion::prelude::*;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

/// Run the materializer, driven by Silver file notifications from the refiner.
pub async fn run(
    silver_path: String,
    gold_path: String,
    mut silver_rx: mpsc::Receiver<PathBuf>,
) {
    let _ = std::fs::create_dir_all(&gold_path);

    tracing::info!("Materializer waiting for Silver file notifications");

    while let Some(_silver_file) = silver_rx.recv().await {
        // Debounce: drain any additional notifications that arrived
        while silver_rx.try_recv().is_ok() {}

        if let Err(e) = materialize_all(&silver_path, &gold_path).await {
            tracing::warn!("Materializer error: {e}");
        }
    }
}

/// Materialize all Gold views from Silver data.
async fn materialize_all(silver_path: &str, gold_path: &str) -> anyhow::Result<()> {
    let silver = Path::new(silver_path);
    if !silver.exists() {
        return Ok(());
    }

    let ctx = SessionContext::new();
    let options = ParquetReadOptions::default();
    ctx.register_parquet("events", silver_path, options).await?;

    crate::udfs::register_all(&ctx);

    let views: Vec<(&str, String)> = vec![
        ("experiment_summaries", crate::udfs::table_fns::burst_summary_sql("*").replace("'*'", "experiment_id")),
        ("phase_timings", crate::udfs::table_fns::phase_breakdown_sql("*").replace("'*'", "experiment_id")),
    ];

    for (name, sql) in &views {
        let gold_file = format!("{gold_path}/{name}.parquet");
        match materialize_view(&ctx, sql, &gold_file).await {
            Ok(()) => tracing::debug!("Materialized {name}"),
            Err(e) => tracing::warn!("Failed to materialize {name}: {e}"),
        }
    }

    Ok(())
}

/// Execute SQL and write result as Gold Parquet.
async fn materialize_view(ctx: &SessionContext, sql: &str, output_path: &str) -> anyhow::Result<()> {
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(());
    }

    let tmp_path = format!("{output_path}.tmp");
    let writer_opts = datafusion::dataframe::DataFrameWriteOptions::new();

    let schema = batches[0].schema();
    let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("_gold_tmp", std::sync::Arc::new(mem_table))?;

    let df = ctx.sql("SELECT * FROM _gold_tmp").await?;
    df.write_parquet(&tmp_path, writer_opts, None).await?;

    std::fs::rename(&tmp_path, output_path)?;
    ctx.deregister_table("_gold_tmp")?;

    Ok(())
}
