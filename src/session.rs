//! Managed DataFusion session with dynamic table re-registration.
//!
//! Uses ArcSwap for lock-free reads. Queries hold an Arc<SessionContext>
//! for their duration; the swap is atomic and old sessions live until
//! all queries complete.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Events that trigger session refresh.
#[derive(Debug, Clone)]
pub enum SessionEvent {
    /// New data in Bronze (NDJSON).
    BronzeChanged,
    /// New data in Silver (Parquet).
    SilverChanged,
    /// Gold views materialized.
    GoldUpdated,
}

/// A managed DataFusion session that refreshes when data changes.
pub struct ManagedSession {
    ctx: ArcSwap<SessionContext>,
    analytics_path: PathBuf,
}

impl ManagedSession {
    /// Create a new managed session and register initial tables.
    pub async fn new(analytics_path: &Path) -> anyhow::Result<Self> {
        let ctx = build_session(analytics_path).await?;
        Ok(Self {
            ctx: ArcSwap::new(Arc::new(ctx)),
            analytics_path: analytics_path.to_path_buf(),
        })
    }

    /// Get the current session context (lock-free).
    pub fn get(&self) -> Arc<SessionContext> {
        self.ctx.load_full()
    }

    /// Refresh the session by rebuilding and atomically swapping.
    pub async fn refresh(&self) -> anyhow::Result<()> {
        let new_ctx = build_session(&self.analytics_path).await?;
        self.ctx.store(Arc::new(new_ctx));
        info!("Session refreshed with latest data");
        Ok(())
    }
}

/// Build a DataFusion session with tables from all available tiers.
async fn build_session(analytics_path: &Path) -> anyhow::Result<SessionContext> {
    let ctx = SessionContext::new();

    // Register custom UDFs
    crate::udfs::register_all(&ctx);

    let bronze_path = analytics_path.join("bronze");
    let silver_path = analytics_path.join("silver");
    let gold_path = analytics_path.join("gold");

    // Prefer Silver (Parquet) > Bronze (NDJSON) > nothing
    if has_files(&silver_path, "parquet") {
        info!("Registering events from Silver (Parquet)");
        let opts = ListingOptions::new(Arc::new(
            datafusion::datasource::file_format::parquet::ParquetFormat::default(),
        ))
        .with_file_extension(".parquet")
        .with_collect_stat(true);
        ctx.register_listing_table(
            "events",
            silver_path.to_string_lossy().as_ref(),
            opts,
            None,
            None,
        )
        .await?;
    } else if has_files(&bronze_path, "json") {
        info!("Registering events from Bronze (NDJSON)");
        ctx.register_json(
            "events",
            bronze_path.to_string_lossy().as_ref(),
            NdJsonReadOptions::default().file_extension(".json"),
        )
        .await?;
    } else {
        info!("No data files found, registering empty events table");
        // Register empty table so queries don't fail
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("timestamp", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("event_type", arrow::datatypes::DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::new_empty(schema);
        ctx.register_batch("events", batch)?;
    }

    // Register Gold views if available
    if has_files(&gold_path, "parquet") {
        info!("Registering Gold views");
        let opts = ListingOptions::new(Arc::new(
            datafusion::datasource::file_format::parquet::ParquetFormat::default(),
        ))
        .with_file_extension(".parquet");
        let _ = ctx
            .register_listing_table(
                "gold_summaries",
                gold_path.to_string_lossy().as_ref(),
                opts,
                None,
                None,
            )
            .await;
    }

    // Also register Bronze as bronze_events for raw access
    if has_files(&bronze_path, "json") {
        let _ = ctx
            .register_json(
                "bronze_events",
                bronze_path.to_string_lossy().as_ref(),
                NdJsonReadOptions::default().file_extension(".json"),
            )
            .await;
    }

    Ok(ctx)
}

/// Run the session refresh loop, listening for data change events.
pub async fn run_refresh_loop(
    session: Arc<ManagedSession>,
    mut rx: mpsc::Receiver<SessionEvent>,
) {
    info!("Session refresh loop started");
    while let Some(event) = rx.recv().await {
        info!(?event, "Refreshing session");
        if let Err(e) = session.refresh().await {
            warn!(error = %e, "Session refresh failed");
        }
    }
}

/// Check if a directory has files with the given extension (recursive).
fn has_files(dir: &Path, extension: &str) -> bool {
    if !dir.exists() {
        return false;
    }
    walkdir::WalkDir::new(dir)
        .into_iter()
        .filter_map(Result::ok)
        .any(|e| {
            e.file_type().is_file()
                && e.path()
                    .extension()
                    .is_some_and(|ext| ext == extension)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn creates_session_with_empty_data() {
        let tmp = tempfile::TempDir::new().unwrap();
        for tier in ["bronze", "silver", "gold"] {
            std::fs::create_dir_all(tmp.path().join(tier)).unwrap();
        }
        let session = ManagedSession::new(tmp.path()).await.unwrap();
        let ctx = session.get();
        // Should have events table (empty)
        let result = ctx.sql("SELECT count(*) FROM events").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn reads_bronze_ndjson() {
        let tmp = tempfile::TempDir::new().unwrap();
        let bronze = tmp.path().join("bronze");
        std::fs::create_dir_all(&bronze).unwrap();
        std::fs::create_dir_all(tmp.path().join("silver")).unwrap();
        std::fs::create_dir_all(tmp.path().join("gold")).unwrap();

        // Write test NDJSON
        std::fs::write(
            bronze.join("test.json"),
            r#"{"event_type":"TEST","scenario":"s1"}
{"event_type":"TEST","scenario":"s2"}"#,
        )
        .unwrap();

        let session = ManagedSession::new(tmp.path()).await.unwrap();
        let ctx = session.get();
        let df = ctx
            .sql("SELECT count(*) as cnt FROM events")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let cnt = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 2);
    }
}
