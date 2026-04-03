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
