//! Pipeline metrics for Shinryu.
//!
//! Tracks files processed, rows ingested, queries executed, and latencies.
//! Exposes Prometheus text format on /metrics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Pipeline and query metrics.
pub struct Metrics {
    pub files_refined: AtomicU64,
    pub files_materialized: AtomicU64,
    pub rows_ingested: AtomicU64,
    pub refine_errors: AtomicU64,
    pub queries_executed: AtomicU64,
    pub query_errors: AtomicU64,
    pub events_received: AtomicU64,
    query_latencies_ms: Mutex<Vec<f64>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            files_refined: AtomicU64::new(0),
            files_materialized: AtomicU64::new(0),
            rows_ingested: AtomicU64::new(0),
            refine_errors: AtomicU64::new(0),
            queries_executed: AtomicU64::new(0),
            query_errors: AtomicU64::new(0),
            events_received: AtomicU64::new(0),
            query_latencies_ms: Mutex::new(Vec::with_capacity(1000)),
        }
    }

    pub fn record_query_latency(&self, ms: f64) {
        if let Ok(mut latencies) = self.query_latencies_ms.lock() {
            latencies.push(ms);
            // Keep bounded
            if latencies.len() > 10_000 {
                latencies.drain(..5_000);
            }
        }
    }

    /// Render metrics in Prometheus text format.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::with_capacity(1024);

        let counters = [
            ("shinryu_files_refined_total", &self.files_refined, "Total files refined from Bronze to Silver"),
            ("shinryu_files_materialized_total", &self.files_materialized, "Total files materialized from Silver to Gold"),
            ("shinryu_rows_ingested_total", &self.rows_ingested, "Total rows ingested into Silver"),
            ("shinryu_refine_errors_total", &self.refine_errors, "Total refiner errors"),
            ("shinryu_queries_executed_total", &self.queries_executed, "Total SQL queries executed"),
            ("shinryu_query_errors_total", &self.query_errors, "Total query errors"),
            ("shinryu_events_received_total", &self.events_received, "Total filesystem events received"),
        ];

        for (name, counter, help) in &counters {
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} counter\n{name} {}\n", counter.load(Ordering::Relaxed)));
        }

        // Query latency percentiles
        if let Ok(latencies) = self.query_latencies_ms.lock() {
            if !latencies.is_empty() {
                let mut sorted = latencies.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let p50 = sorted[sorted.len() / 2] / 1000.0;
                let p99 = sorted[(sorted.len() as f64 * 0.99) as usize] / 1000.0;
                let max = sorted.last().copied().unwrap_or(0.0) / 1000.0;

                out.push_str("# HELP shinryu_query_duration_seconds Query execution duration\n");
                out.push_str("# TYPE shinryu_query_duration_seconds summary\n");
                out.push_str(&format!("shinryu_query_duration_seconds{{quantile=\"0.5\"}} {p50:.4}\n"));
                out.push_str(&format!("shinryu_query_duration_seconds{{quantile=\"0.99\"}} {p99:.4}\n"));
                out.push_str(&format!("shinryu_query_duration_seconds{{quantile=\"1.0\"}} {max:.4}\n"));
                out.push_str(&format!("shinryu_query_duration_seconds_count {}\n", sorted.len()));
            }
        }

        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_format() {
        let m = Metrics::new();
        m.files_refined.store(42, Ordering::Relaxed);
        m.queries_executed.store(10, Ordering::Relaxed);
        m.record_query_latency(100.0);
        m.record_query_latency(200.0);

        let out = m.to_prometheus();
        assert!(out.contains("shinryu_files_refined_total 42"));
        assert!(out.contains("shinryu_queries_executed_total 10"));
        assert!(out.contains("shinryu_query_duration_seconds"));
    }

    #[test]
    fn latency_bounded() {
        let m = Metrics::new();
        for i in 0..15_000 {
            m.record_query_latency(i as f64);
        }
        let latencies = m.query_latencies_ms.lock().unwrap();
        assert!(latencies.len() <= 10_001);
    }

    #[test]
    fn prometheus_no_summary_when_empty() {
        let m = Metrics::new();
        let out = m.to_prometheus();
        assert!(out.contains("shinryu_files_refined_total 0"));
        assert!(
            !out.contains("shinryu_query_duration_seconds"),
            "should not emit summary section when no latencies recorded"
        );
    }

    #[test]
    fn prometheus_single_latency() {
        let m = Metrics::new();
        m.record_query_latency(42.0);
        let out = m.to_prometheus();
        assert!(out.contains("shinryu_query_duration_seconds_count 1"));
        assert!(out.contains("quantile=\"0.5\""));
        assert!(out.contains("quantile=\"0.99\""));
    }

    #[test]
    fn prometheus_p99_index_for_two_values() {
        let m = Metrics::new();
        m.record_query_latency(100.0);
        m.record_query_latency(200.0);
        let out = m.to_prometheus();
        assert!(out.contains("shinryu_query_duration_seconds_count 2"));
    }

    #[test]
    fn all_counters_appear_in_output() {
        let m = Metrics::new();
        let out = m.to_prometheus();
        let expected = [
            "shinryu_files_refined_total",
            "shinryu_files_materialized_total",
            "shinryu_rows_ingested_total",
            "shinryu_refine_errors_total",
            "shinryu_queries_executed_total",
            "shinryu_query_errors_total",
            "shinryu_events_received_total",
        ];
        for name in &expected {
            assert!(out.contains(name), "Missing counter: {name}");
        }
    }

    #[test]
    fn counter_increments_are_reflected() {
        let m = Metrics::new();
        m.files_refined.store(5, Ordering::Relaxed);
        m.query_errors.store(3, Ordering::Relaxed);
        m.rows_ingested.store(1000, Ordering::Relaxed);
        let out = m.to_prometheus();
        assert!(out.contains("shinryu_files_refined_total 5"));
        assert!(out.contains("shinryu_query_errors_total 3"));
        assert!(out.contains("shinryu_rows_ingested_total 1000"));
    }

    #[test]
    fn default_creates_same_as_new() {
        let m1 = Metrics::new();
        let m2 = Metrics::default();
        assert_eq!(m1.to_prometheus(), m2.to_prometheus());
    }
}
