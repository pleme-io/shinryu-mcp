//! Three-tier DataFusion query engine: Bronze (NDJSON), Silver (Parquet), Gold (materialized views).

use datafusion::prelude::*;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use arrow::datatypes::DataType;
use std::path::Path;
use std::sync::Arc;

/// Create a DataFusion session with three-tier table registration.
///
/// - `bronze_events` — raw NDJSON (for replay/debugging)
/// - `events` — Silver Parquet (default, for analysis)
/// - Gold views — pre-computed Parquet (experiment_summaries, etc.)
pub async fn create_session(analytics_path: &str) -> anyhow::Result<SessionContext> {
    let ctx = SessionContext::new();

    let bronze_path = format!("{analytics_path}/bronze");
    let silver_path = format!("{analytics_path}/silver");
    let gold_path = format!("{analytics_path}/gold");

    // Register Bronze (NDJSON, schema-on-read)
    register_ndjson_table(&ctx, "bronze_events", &bronze_path).await;

    // Register Silver or fall back to Bronze as the default "events" table
    let silver_has_data = has_files_recursive(&silver_path, "parquet");

    if silver_has_data {
        register_parquet_table(&ctx, "events", &silver_path).await;
    } else {
        // Silver not yet populated — use Bronze directly
        // Bronze has Hive-style dirs (signal_type=X/) — register each explicitly
        let options = NdJsonReadOptions::default();
        match ctx.register_json("events", &format!("{bronze_path}/"), options).await {
            Ok(()) => tracing::info!("Registered events from Bronze ({bronze_path})"),
            Err(e) => {
                tracing::warn!("Failed to register events from Bronze: {e}");
                // Fallback: register bronze_events alias as events
                if let Some(table) = ctx.table_provider("bronze_events").await.ok() {
                    let _ = ctx.register_table("events", table);
                    tracing::info!("Using bronze_events alias as events");
                }
            }
        }
    }

    // Register Gold views (each a separate Parquet file → table)
    for name in &["experiment_summaries", "pod_timeline", "bottleneck_scores", "network_aggregates", "phase_timings"] {
        let gold_file = format!("{gold_path}/{name}.parquet");
        if Path::new(&gold_file).exists() {
            let options = ParquetReadOptions::default();
            if ctx.register_parquet(*name, &gold_file, options).await.is_ok() {
                tracing::info!("Registered Gold view: {name}");
            }
        }
    }

    Ok(ctx)
}

/// Register an NDJSON directory as a table (schema-on-read).
async fn register_ndjson_table(ctx: &SessionContext, table_name: &str, path: &str) {
    let dir = Path::new(path);
    if dir.exists() && dir.read_dir().map(|mut d| d.next().is_some()).unwrap_or(false) {
        let json_format = Arc::new(JsonFormat::default());
        let listing_options = ListingOptions::new(json_format)
            .with_file_extension(".json");

        if let Ok(table_url) = ListingTableUrl::parse(path) {
            let config = ListingTableConfig::new(table_url)
                .with_listing_options(listing_options);

            if let Ok(config) = config.infer(&ctx.state()).await {
                if let Ok(table) = ListingTable::try_new(config) {
                    let _ = ctx.register_table(table_name, Arc::new(table));
                    tracing::info!("Registered {table_name} from {path} (NDJSON)");
                }
            }
        }
    }
}

/// Register a Parquet directory as a table.
async fn register_parquet_table(ctx: &SessionContext, table_name: &str, path: &str) {
    let dir = Path::new(path);
    if dir.exists() && dir.read_dir().map(|mut d| d.next().is_some()).unwrap_or(false) {
        let options = ParquetReadOptions::default();
        if ctx.register_parquet(table_name, path, options).await.is_ok() {
            tracing::info!("Registered {table_name} from {path} (Parquet)");
        }
    }
}

/// Check if a directory contains any files with the given extension (recursively).
fn has_files_recursive(dir: &str, ext: &str) -> bool {
    let path = Path::new(dir);
    if !path.exists() { return false; }
    fn check(dir: &Path, ext: &str) -> bool {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() && check(&p, ext) { return true; }
                if p.extension().and_then(|e| e.to_str()) == Some(ext) { return true; }
            }
        }
        false
    }
    check(path, ext)
}

/// Execute a SQL query and return results as formatted text.
pub async fn execute_sql(ctx: &SessionContext, sql: &str) -> anyhow::Result<String> {
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok("No results.".to_string());
    }

    let formatted = arrow::util::pretty::pretty_format_batches(&batches)?;
    Ok(formatted.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execute_sql_returns_formatted_output() {
        let ctx = SessionContext::new();
        let result = execute_sql(&ctx, "SELECT 1 as n, 'hello' as msg").await.unwrap();
        assert!(result.contains("1"), "should contain the numeric value");
        assert!(result.contains("hello"), "should contain the string value");
    }

    #[tokio::test]
    async fn execute_sql_empty_result_returns_no_results() {
        let ctx = SessionContext::new();
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Int32, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::new_empty(schema);
        ctx.register_batch("empty_tbl", batch).unwrap();
        let result = execute_sql(&ctx, "SELECT * FROM empty_tbl").await.unwrap();
        assert_eq!(result, "No results.");
    }

    #[tokio::test]
    async fn execute_sql_invalid_sql_returns_error() {
        let ctx = SessionContext::new();
        let result = execute_sql(&ctx, "THIS IS NOT SQL").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_sql_reference_missing_table_returns_error() {
        let ctx = SessionContext::new();
        let result = execute_sql(&ctx, "SELECT * FROM nonexistent_table").await;
        assert!(result.is_err());
    }

    #[test]
    fn has_files_recursive_empty_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        assert!(!has_files_recursive(tmp.path().to_str().unwrap(), "json"));
    }

    #[test]
    fn has_files_recursive_nonexistent_dir() {
        assert!(!has_files_recursive("/tmp/definitely_not_existing_dir_xyz", "json"));
    }

    #[test]
    fn has_files_recursive_finds_nested_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let nested = tmp.path().join("sub1").join("sub2");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("data.parquet"), b"fake").unwrap();
        assert!(has_files_recursive(tmp.path().to_str().unwrap(), "parquet"));
        assert!(!has_files_recursive(tmp.path().to_str().unwrap(), "json"));
    }

    #[test]
    fn has_files_recursive_ignores_wrong_extension() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::write(tmp.path().join("data.txt"), b"hello").unwrap();
        assert!(!has_files_recursive(tmp.path().to_str().unwrap(), "json"));
    }

    #[tokio::test]
    async fn create_session_with_empty_dirs() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("bronze")).unwrap();
        std::fs::create_dir_all(tmp.path().join("silver")).unwrap();
        std::fs::create_dir_all(tmp.path().join("gold")).unwrap();
        let ctx = create_session(tmp.path().to_str().unwrap()).await.unwrap();
        let result = execute_sql(&ctx, "SELECT 1 as x").await.unwrap();
        assert!(result.contains("1"));
    }

    #[tokio::test]
    async fn create_session_nonexistent_path_still_creates_context() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("nonexistent");
        let ctx = create_session(path.to_str().unwrap()).await.unwrap();
        let result = execute_sql(&ctx, "SELECT 42 as answer").await.unwrap();
        assert!(result.contains("42"));
    }
}
