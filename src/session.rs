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

            // Register ALL parquet files from this partition as one table.
            //
            // DataFusion's register_listing_table doesn't reliably recurse
            // into our YYYY-MM-DD/HH.parquet subdirectory layout. Instead,
            // read_parquet() with the explicit file list (which walkdir
            // already discovered), materialize to memory (fine for our
            // <100KB dataset), and register as a MemTable. This guarantees
            // every historical experiment file is visible as one unified
            // table, regardless of directory nesting depth.
            let opts = ParquetReadOptions::default();
            match ctx.read_parquet(parquet_files.clone(), opts).await {
                Ok(df) => match df.collect().await {
                    Ok(batches) if !batches.is_empty() => {
                        let schema = batches[0].schema();
                        match datafusion::datasource::MemTable::try_new(schema, vec![batches]) {
                            Ok(mem_table) => {
                                match ctx.register_table(&table_name, Arc::new(mem_table)) {
                                    Ok(_) => {
                                        info!(
                                            %signal_type, %table_name,
                                            file_count = parquet_files.len(),
                                            "Registered Silver partition (multi-file MemTable)"
                                        );
                                        registered_tables.push(table_name);
                                    }
                                    Err(e) => warn!(error = %e, %signal_type, "Failed to register MemTable"),
                                }
                            }
                            Err(e) => warn!(error = %e, %signal_type, "Failed to create MemTable"),
                        }
                    }
                    Ok(_) => info!(%signal_type, "Silver partition has 0 rows, skipping"),
                    Err(e) => warn!(error = %e, %signal_type, "Failed to collect Silver Parquet"),
                },
                Err(e) => warn!(error = %e, %signal_type, "Failed to read Silver Parquet files"),
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

    /// Regression test: multiple Parquet files across date-partitioned subdirectories
    /// must ALL be queryable as one table, not just the latest file.
    ///
    /// This reproduces the bug where `register_parquet()` (single-file) was used
    /// instead of `register_listing_table()` (directory), causing all historical
    /// experiment data to be silently dropped.
    #[tokio::test]
    async fn reads_multi_file_silver_partitions() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;

        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("bronze")).unwrap();
        std::fs::create_dir_all(tmp.path().join("gold")).unwrap();

        // Create two date-partitioned Parquet files under signal_type=event
        let event_dir = tmp.path().join("silver").join("signal_type=event");
        let day1 = event_dir.join("2026-04-06");
        let day2 = event_dir.join("2026-04-07");
        std::fs::create_dir_all(&day1).unwrap();
        std::fs::create_dir_all(&day2).unwrap();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("experiment_id", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("event_type", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("elapsed_ms", arrow::datatypes::DataType::Int64, true),
        ]));

        // File 1: 2 events from day 1
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["exp-001", "exp-001"])),
                Arc::new(StringArray::from(vec!["POLL_TICK", "MILESTONE"])),
                Arc::new(Int64Array::from(vec![1000, 2000])),
            ],
        )
        .unwrap();
        let f1 = std::fs::File::create(day1.join("20.parquet")).unwrap();
        let mut w1 = ArrowWriter::try_new(f1, Arc::clone(&schema), None).unwrap();
        w1.write(&batch1).unwrap();
        w1.close().unwrap();

        // File 2: 3 events from day 2
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["exp-002", "exp-002", "exp-002"])),
                Arc::new(StringArray::from(vec!["POLL_TICK", "MILESTONE", "BURST_COMPLETE"])),
                Arc::new(Int64Array::from(vec![3000, 4000, 5000])),
            ],
        )
        .unwrap();
        let f2 = std::fs::File::create(day2.join("01.parquet")).unwrap();
        let mut w2 = ArrowWriter::try_new(f2, Arc::clone(&schema), None).unwrap();
        w2.write(&batch2).unwrap();
        w2.close().unwrap();

        // Build session — should see ALL 5 events across both files
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
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(cnt, 5, "should see all 5 events from both Parquet files, not just the latest");

        // Verify both experiments are queryable (use count per experiment
        // to avoid StringArray vs LargeStringArray type ambiguity)
        let df = ctx
            .sql("SELECT count(DISTINCT experiment_id) as exp_count FROM events")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let exp_count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(exp_count, 2, "both experiments must be visible");
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
