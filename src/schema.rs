//! Unified Arrow schema for all Shinryū signals.
//!
//! Every signal (log, metric, event, flow, pprof) becomes a row in this schema.
//! DataFusion reads NDJSON/Parquet files that conform to this schema.

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Build the unified events schema.
pub fn events_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // Core fields
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("timestamp_ms", DataType::Int64, true),     // Epoch ms for fast JOINs
        Field::new("cluster", DataType::Utf8, true),
        Field::new("experiment_id", DataType::Utf8, true),
        Field::new("signal_type", DataType::Utf8, true),       // event, log, metric, flow
        Field::new("source", DataType::Utf8, true),
        Field::new("delivery_tier", DataType::Utf8, true),

        // K8s context
        Field::new("namespace", DataType::Utf8, true),
        Field::new("pod", DataType::Utf8, true),
        Field::new("container", DataType::Utf8, true),         // Was missing
        Field::new("node", DataType::Utf8, true),
        Field::new("app", DataType::Utf8, true),

        // Log fields
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("event_type", DataType::Utf8, true),
        Field::new("event_source", DataType::Utf8, true),      // Was missing

        // Metric fields
        Field::new("metric_name", DataType::Utf8, true),
        Field::new("metric_value", DataType::Float64, true),

        // Hubble flow fields
        Field::new("src_pod", DataType::Utf8, true),
        Field::new("src_namespace", DataType::Utf8, true),
        Field::new("dst_pod", DataType::Utf8, true),
        Field::new("dst_namespace", DataType::Utf8, true),
        Field::new("dst_port", DataType::UInt16, true),
        Field::new("protocol", DataType::Utf8, true),
        Field::new("verdict", DataType::Utf8, true),
        Field::new("l7_type", DataType::Utf8, true),
        Field::new("dns_query", DataType::Utf8, true),
        Field::new("http_method", DataType::Utf8, true),
        Field::new("http_url", DataType::Utf8, true),
        Field::new("http_code", DataType::UInt16, true),
        Field::new("akeyless_flow", DataType::Boolean, true),
        Field::new("flow_direction", DataType::Utf8, true),

        // burst-forge fields (original names from events.rs)
        Field::new("scenario", DataType::Utf8, true),
        Field::new("running", DataType::UInt32, true),
        Field::new("pending", DataType::UInt32, true),
        Field::new("failed", DataType::UInt32, true),
        Field::new("injected", DataType::UInt32, true),
        Field::new("injection_rate", DataType::Float64, true),
        Field::new("injection_rate_pct", DataType::Float64, true),
        Field::new("elapsed_ms", DataType::UInt64, true),
        Field::new("peak_running", DataType::UInt32, true),

        // burst-forge enriched fields (burst_ prefix from Vector transform)
        Field::new("burst_replicas", DataType::UInt32, true),
        Field::new("burst_running", DataType::UInt32, true),
        Field::new("burst_injected", DataType::UInt32, true),
        Field::new("burst_success_rate", DataType::Float64, true),
        Field::new("burst_first_ready_ms", DataType::UInt64, true),
        Field::new("burst_ready_ms", DataType::UInt64, true),
        Field::new("burst_admission_rate", DataType::Float64, true),
        Field::new("burst_gw_throughput", DataType::Float64, true),

        // Pod detail fields
        Field::new("restart_count", DataType::UInt32, true),
        Field::new("state_reason", DataType::Utf8, true),
        Field::new("qos_class", DataType::Utf8, true),

        // Prediction fields (from burst-forge scaling formulas)
        Field::new("predicted_gw_replicas", DataType::UInt32, true),
        Field::new("predicted_wh_replicas", DataType::UInt32, true),
        Field::new("predicted_min_secs", DataType::Float64, true),
        Field::new("predicted_throughput", DataType::Float64, true),
        Field::new("prediction_formula", DataType::Utf8, true),
        Field::new("prediction_verdict", DataType::Utf8, true),     // FASTER/ON_TARGET/SLOWER/UNDER_PROVISIONED
        Field::new("prediction_error_pct", DataType::Float64, true),

        // Akeyless-specific
        Field::new("gw_request_duration_ms", DataType::UInt32, true),
        Field::new("gw_operation", DataType::Utf8, true),
        Field::new("wh_admission_duration_ms", DataType::UInt32, true),

        // Raw JSON for anything not in the schema
        Field::new("raw", DataType::Utf8, true),
    ]))
}
