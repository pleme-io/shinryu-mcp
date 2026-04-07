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

    // Prefer Silver (Parquet) > Bronze (NDJSON) > nothing.
    // Silver has a Hive-style partitioned layout: silver/signal_type=X/YYYY-MM-DD/HH.parquet
    // Each signal_type directory has its own schema, so register one table per signal_type
    // and expose them via a single `events` view that unions them.
    if has_files(&silver_path, "parquet") {
        info!("Registering events from Silver (Parquet)");

        // Discover signal_type partition directories
        let signal_dirs: Vec<(String, std::path::PathBuf)> = std::fs::read_dir(&silver_path)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().ok().map_or(false, |t| t.is_dir()))
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.strip_prefix("signal_type=")
                    .map(|s| (s.to_string(), e.path()))
            })
            .collect();

        info!(partition_count = signal_dirs.len(), "Discovered Silver partitions");

        let mut registered_tables = Vec::new();
        for (signal_type, dir) in &signal_dirs {
            // Find all parquet files in this signal_type partition (recursive across YYYY-MM-DD subdirs)
            let parquet_files: Vec<String> = walkdir::WalkDir::new(dir)
                .into_iter()
                .filter_map(Result::ok)
                .filter(|e| e.file_type().is_file() && e.path().extension().is_some_and(|x| x == "parquet"))
                .map(|e| e.path().to_string_lossy().to_string())
                .collect();

            if parquet_files.is_empty() {
                continue;
            }

            let table_name = format!("events_{signal_type}");

            // Register the entire signal_type partition as a listing table.
            // DataFusion reads all Parquet files in the directory tree
            // (YYYY-MM-DD/HH.parquet) as a single table. The first file's
            // schema is used; subsequent files with compatible schemas are
            // unioned automatically. This was previously register_parquet()
            // on the single newest file, which threw away all historical
            // experiment data.
            let listing_opts = ListingOptions::new(Arc::new(
                datafusion::datasource::file_format::parquet::ParquetFormat::default(),
            ))
            .with_file_extension(".parquet");
            match ctx
                .register_listing_table(
                    &table_name,
                    dir.to_string_lossy().as_ref(),
                    listing_opts,
                    None,
                    None,
                )
                .await
            {
                Ok(()) => {
                    info!(
                        %signal_type, %table_name,
                        file_count = parquet_files.len(),
                        "Registered Silver partition (listing table)"
                    );
                    registered_tables.push(table_name);
                }
                Err(e) => warn!(error = %e, %signal_type, "Failed to register Silver partition"),
            }
        }

        if registered_tables.is_empty() {
            info!("No Silver partitions registered, registering empty events table");
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("timestamp", arrow::datatypes::DataType::Utf8, true),
                arrow::datatypes::Field::new("event_type", arrow::datatypes::DataType::Utf8, true),
            ]));
            let batch = arrow::record_batch::RecordBatch::new_empty(schema);
            ctx.register_batch("events", batch)?;
        } else if registered_tables.len() == 1 {
            // Single partition: alias to `events` directly
            let view_sql = format!("CREATE VIEW events AS SELECT * FROM {}", registered_tables[0]);
            ctx.sql(&view_sql).await?;
        } else {
            // Multiple partitions: try UNION (may fail on schema mismatch).
            // Default to the largest partition (signal_type=event preferred).
            let preferred = registered_tables
                .iter()
                .find(|t| t.contains("event"))
                .or_else(|| registered_tables.first())
                .cloned()
                .unwrap();
            let view_sql = format!("CREATE VIEW events AS SELECT * FROM {preferred}");
            ctx.sql(&view_sql).await?;
            info!(%preferred, "Created events view (preferred partition)");
        }
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

    #[tokio::test]
    async fn refresh_swaps_session_context() {
        let tmp = tempfile::TempDir::new().unwrap();
        for tier in ["bronze", "silver", "gold"] {
            std::fs::create_dir_all(tmp.path().join(tier)).unwrap();
        }
        let session = ManagedSession::new(tmp.path()).await.unwrap();
        let ctx1 = session.get();

        session.refresh().await.unwrap();
        let ctx2 = session.get();
        assert!(
            !Arc::ptr_eq(&ctx1, &ctx2),
            "refresh should produce a new SessionContext"
        );
    }

    #[tokio::test]
    async fn get_returns_arc_that_outlives_refresh() {
        let tmp = tempfile::TempDir::new().unwrap();
        for tier in ["bronze", "silver", "gold"] {
            std::fs::create_dir_all(tmp.path().join(tier)).unwrap();
        }
        let session = ManagedSession::new(tmp.path()).await.unwrap();
        let old_ctx = session.get();
        session.refresh().await.unwrap();
        // Old context should still be usable
        let result = old_ctx.sql("SELECT 1 as x").await;
        assert!(result.is_ok(), "old context should remain usable after refresh");
    }

    #[tokio::test]
    async fn session_has_udfs_registered() {
        let tmp = tempfile::TempDir::new().unwrap();
        for tier in ["bronze", "silver", "gold"] {
            std::fs::create_dir_all(tmp.path().join(tier)).unwrap();
        }
        let session = ManagedSession::new(tmp.path()).await.unwrap();
        let ctx = session.get();
        let df = ctx.sql("SELECT tumbling_window(1000, 500) as tw").await;
        assert!(df.is_ok(), "tumbling_window UDF should be available");
    }

    #[tokio::test]
    async fn run_refresh_loop_exits_when_sender_dropped() {
        let tmp = tempfile::TempDir::new().unwrap();
        for tier in ["bronze", "silver", "gold"] {
            std::fs::create_dir_all(tmp.path().join(tier)).unwrap();
        }
        let session = Arc::new(ManagedSession::new(tmp.path()).await.unwrap());
        let (tx, rx) = mpsc::channel(4);

        let handle = tokio::spawn(run_refresh_loop(session, rx));
        drop(tx);
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        assert!(result.is_ok(), "loop should exit when sender is dropped");
    }

    #[test]
    fn has_files_empty_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        assert!(!has_files(tmp.path(), "json"));
    }

    #[test]
    fn has_files_nonexistent_dir() {
        assert!(!has_files(Path::new("/nonexistent_xyz"), "json"));
    }

    #[test]
    fn has_files_finds_matching_extension() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::write(tmp.path().join("data.parquet"), b"fake").unwrap();
        assert!(has_files(tmp.path(), "parquet"));
        assert!(!has_files(tmp.path(), "json"));
    }

    #[test]
    fn has_files_recursive_in_subdirs() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sub = tmp.path().join("a").join("b");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("deep.json"), b"{}").unwrap();
        assert!(has_files(tmp.path(), "json"));
    }
}
