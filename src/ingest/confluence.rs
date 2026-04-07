//! Generic dataset ingestion into Bronze tier.
//!
//! Reads NDJSON dataset files from configured paths and copies them
//! into the Bronze layer. No hardcoded data — all driven by shikumi config.

use serde::Deserialize;
use std::path::Path;

/// A configured dataset to ingest.
#[derive(Debug, Deserialize)]
pub struct DatasetConfig {
    pub name: String,
    pub data_file: String,
}

/// Ingest all configured datasets into Bronze.
///
/// Reads each dataset's NDJSON file and writes it to
/// `<bronze_path>/signal_type=experiment_result/<name>.json`
pub fn ingest_datasets(datasets: &[DatasetConfig], bronze_path: &str) -> anyhow::Result<usize> {
    let output_dir = format!("{bronze_path}/signal_type=experiment_result");
    std::fs::create_dir_all(&output_dir)?;

    let mut total = 0usize;

    for dataset in datasets {
        let source = Path::new(&dataset.data_file);
        if !source.exists() {
            tracing::warn!("Dataset file not found: {} ({})", dataset.name, dataset.data_file);
            continue;
        }

        let dest = format!("{output_dir}/{}.json", dataset.name);
        std::fs::copy(source, &dest)?;

        let lines = std::fs::read_to_string(source)?
            .lines()
            .filter(|l| !l.trim().is_empty())
            .count();

        tracing::info!("Ingested dataset '{}': {} records → {}", dataset.name, lines, dest);
        total += lines;
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_empty_datasets_list() {
        let tmp = tempfile::TempDir::new().unwrap();
        let bronze = tmp.path().join("bronze");
        let total = ingest_datasets(&[], bronze.to_str().unwrap()).unwrap();
        assert_eq!(total, 0);
    }

    #[test]
    fn ingest_creates_output_dir() {
        let tmp = tempfile::TempDir::new().unwrap();
        let bronze = tmp.path().join("bronze");
        ingest_datasets(&[], bronze.to_str().unwrap()).unwrap();
        assert!(bronze.join("signal_type=experiment_result").exists());
    }

    #[test]
    fn ingest_skips_missing_files() {
        let tmp = tempfile::TempDir::new().unwrap();
        let bronze = tmp.path().join("bronze");
        let datasets = vec![DatasetConfig {
            name: "missing".to_string(),
            data_file: "/tmp/shinryu_nonexistent_xyz.json".to_string(),
        }];
        let total = ingest_datasets(&datasets, bronze.to_str().unwrap()).unwrap();
        assert_eq!(total, 0);
    }

    #[test]
    fn ingest_copies_file_and_counts_lines() {
        let tmp = tempfile::TempDir::new().unwrap();
        let src = tmp.path().join("source.json");
        std::fs::write(&src, "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n").unwrap();

        let bronze = tmp.path().join("bronze");
        let datasets = vec![DatasetConfig {
            name: "test-data".to_string(),
            data_file: src.to_str().unwrap().to_string(),
        }];
        let total = ingest_datasets(&datasets, bronze.to_str().unwrap()).unwrap();
        assert_eq!(total, 3);
        assert!(bronze.join("signal_type=experiment_result/test-data.json").exists());
    }

    #[test]
    fn ingest_ignores_blank_lines_in_count() {
        let tmp = tempfile::TempDir::new().unwrap();
        let src = tmp.path().join("source.json");
        std::fs::write(&src, "{\"a\":1}\n\n{\"b\":2}\n  \n").unwrap();

        let bronze = tmp.path().join("bronze");
        let datasets = vec![DatasetConfig {
            name: "sparse".to_string(),
            data_file: src.to_str().unwrap().to_string(),
        }];
        let total = ingest_datasets(&datasets, bronze.to_str().unwrap()).unwrap();
        assert_eq!(total, 2, "blank/whitespace-only lines should not be counted");
    }

    #[test]
    fn ingest_multiple_datasets() {
        let tmp = tempfile::TempDir::new().unwrap();
        let src1 = tmp.path().join("a.json");
        let src2 = tmp.path().join("b.json");
        std::fs::write(&src1, "{\"x\":1}\n").unwrap();
        std::fs::write(&src2, "{\"y\":1}\n{\"y\":2}\n").unwrap();

        let bronze = tmp.path().join("bronze");
        let datasets = vec![
            DatasetConfig { name: "ds1".to_string(), data_file: src1.to_str().unwrap().to_string() },
            DatasetConfig { name: "ds2".to_string(), data_file: src2.to_str().unwrap().to_string() },
        ];
        let total = ingest_datasets(&datasets, bronze.to_str().unwrap()).unwrap();
        assert_eq!(total, 3);
    }
}
