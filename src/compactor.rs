//! Background Parquet compaction and data lifecycle management.
//!
//! Hot tier (0-1h): NDJSON files written by Vector
//! Warm tier (1h-7d): Parquet files (snappy compressed, with statistics)
//! Cold tier (7d+): Configurable archive
//! Delete: 30-day TTL

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::time;

/// Run the compaction loop as a background task.
///
/// Every 5 minutes, scans for NDJSON files older than 1 hour and compacts
/// them to Parquet format with snappy compression.
pub async fn run_compaction_loop(analytics_path: String) {
    let interval = Duration::from_secs(300); // 5 minutes

    loop {
        if let Err(e) = compact_old_files(&analytics_path).await {
            tracing::warn!("Compaction error: {e}");
        }
        if let Err(e) = cleanup_expired(&analytics_path, 30).await {
            tracing::warn!("Lifecycle cleanup error: {e}");
        }
        time::sleep(interval).await;
    }
}

/// Compact NDJSON files older than `max_age` to Parquet.
async fn compact_old_files(analytics_path: &str) -> anyhow::Result<()> {
    let max_age = Duration::from_secs(3600); // 1 hour
    let path = Path::new(analytics_path);

    if !path.exists() {
        return Ok(());
    }

    let json_files = find_files_older_than(path, ".json", max_age)?;

    if json_files.is_empty() {
        return Ok(());
    }

    tracing::info!("Compacting {} NDJSON files to Parquet", json_files.len());

    for file in &json_files {
        if let Err(e) = compact_single_file(file).await {
            tracing::warn!("Failed to compact {}: {e}", file.display());
        }
    }

    Ok(())
}

/// Compact a single NDJSON file to Parquet.
async fn compact_single_file(json_path: &Path) -> anyhow::Result<()> {
    let parquet_path = json_path.with_extension("parquet");
    let tmp_path = json_path.with_extension("parquet.tmp");

    // Read NDJSON via DataFusion
    let ctx = datafusion::prelude::SessionContext::new();
    let options = datafusion::prelude::NdJsonReadOptions::default();
    ctx.register_json("source", json_path.to_str().unwrap_or(""), options).await?;

    let df = ctx.sql("SELECT * FROM source").await?;

    // Write as Parquet with snappy compression
    let writer_opts = datafusion::dataframe::DataFrameWriteOptions::new();
    df.write_parquet(tmp_path.to_str().unwrap_or(""), writer_opts, None).await?;

    // Atomic rename
    std::fs::rename(&tmp_path, &parquet_path)?;

    // Delete original NDJSON
    std::fs::remove_file(json_path)?;

    tracing::info!("Compacted {} → {}", json_path.display(), parquet_path.display());
    Ok(())
}

/// Delete files older than `max_days`.
async fn cleanup_expired(analytics_path: &str, max_days: u64) -> anyhow::Result<()> {
    let max_age = Duration::from_secs(max_days * 86400);
    let path = Path::new(analytics_path);

    if !path.exists() {
        return Ok(());
    }

    let expired_json = find_files_older_than(path, ".json", max_age)?;
    let expired_parquet = find_files_older_than(path, ".parquet", max_age)?;

    let total = expired_json.len() + expired_parquet.len();
    if total == 0 {
        return Ok(());
    }

    for file in expired_json.iter().chain(expired_parquet.iter()) {
        std::fs::remove_file(file)?;
    }

    tracing::info!("Lifecycle cleanup: removed {total} expired files (>{max_days}d)");
    Ok(())
}

/// Find files with given extension older than `max_age`.
fn find_files_older_than(dir: &Path, extension: &str, max_age: Duration) -> anyhow::Result<Vec<PathBuf>> {
    let now = SystemTime::now();
    let mut results = Vec::new();

    fn walk(dir: &Path, ext: &str, now: SystemTime, max_age: Duration, results: &mut Vec<PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, ext, now, max_age, results);
                } else if path.extension().and_then(|e| e.to_str()) == Some(ext.trim_start_matches('.')) {
                    if let Ok(metadata) = path.metadata() {
                        if let Ok(modified) = metadata.modified() {
                            if let Ok(age) = now.duration_since(modified) {
                                if age > max_age {
                                    results.push(path);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    walk(dir, extension, now, max_age, &mut results);
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn age_file(path: &Path, age: Duration) {
        let past = std::time::SystemTime::now() - age;
        let filetime = filetime::FileTime::from_system_time(past);
        filetime::set_file_mtime(path, filetime).unwrap();
    }

    #[test]
    fn find_files_nonexistent_dir() {
        let results = find_files_older_than(Path::new("/nonexistent_xyz"), ".json", Duration::from_secs(0)).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn find_files_empty_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        let results = find_files_older_than(tmp.path(), ".json", Duration::from_secs(0)).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn find_files_filters_by_extension() {
        let tmp = tempfile::TempDir::new().unwrap();
        fs::write(tmp.path().join("a.json"), "{}").unwrap();
        fs::write(tmp.path().join("b.parquet"), "fake").unwrap();

        age_file(&tmp.path().join("a.json"), Duration::from_secs(7200));
        age_file(&tmp.path().join("b.parquet"), Duration::from_secs(7200));

        let json_files = find_files_older_than(tmp.path(), ".json", Duration::from_secs(3600)).unwrap();
        assert_eq!(json_files.len(), 1);
        assert!(json_files[0].to_string_lossy().contains("a.json"));

        let pq_files = find_files_older_than(tmp.path(), ".parquet", Duration::from_secs(3600)).unwrap();
        assert_eq!(pq_files.len(), 1);
    }

    #[test]
    fn find_files_respects_max_age() {
        let tmp = tempfile::TempDir::new().unwrap();
        let old_file = tmp.path().join("old.json");
        let new_file = tmp.path().join("new.json");
        fs::write(&old_file, "{}").unwrap();
        fs::write(&new_file, "{}").unwrap();

        age_file(&old_file, Duration::from_secs(7200));

        let results = find_files_older_than(tmp.path(), ".json", Duration::from_secs(3600)).unwrap();
        assert_eq!(results.len(), 1, "only old file should match");
        assert_eq!(results[0], old_file);
    }

    #[test]
    fn find_files_recursive_in_subdirs() {
        let tmp = tempfile::TempDir::new().unwrap();
        let subdir = tmp.path().join("sub1").join("sub2");
        fs::create_dir_all(&subdir).unwrap();
        let nested = subdir.join("deep.json");
        fs::write(&nested, "{}").unwrap();

        age_file(&nested, Duration::from_secs(7200));

        let results = find_files_older_than(tmp.path(), ".json", Duration::from_secs(3600)).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn find_files_extension_with_or_without_dot() {
        let tmp = tempfile::TempDir::new().unwrap();
        let f = tmp.path().join("test.json");
        fs::write(&f, "{}").unwrap();
        age_file(&f, Duration::from_secs(7200));

        let with_dot = find_files_older_than(tmp.path(), ".json", Duration::from_secs(3600)).unwrap();
        let without_dot = find_files_older_than(tmp.path(), "json", Duration::from_secs(3600)).unwrap();
        assert_eq!(with_dot.len(), without_dot.len(), "both formats should find the same files");
    }

    #[tokio::test]
    async fn cleanup_expired_nonexistent_path_is_ok() {
        let result = cleanup_expired("/tmp/shinryu_nonexistent_xyz", 30).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn compact_old_files_nonexistent_path_is_ok() {
        let result = compact_old_files("/tmp/shinryu_nonexistent_xyz").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn compact_old_files_empty_dir_is_ok() {
        let tmp = tempfile::TempDir::new().unwrap();
        let result = compact_old_files(tmp.path().to_str().unwrap()).await;
        assert!(result.is_ok());
    }
}
