//! Dataset ingestion into the Bronze tier.
//!
//! Reads NDJSON dataset files from shikumi/YAML configuration and copies
//! them into the Bronze layer. Fully generic — no hardcoded data.

pub mod confluence;
