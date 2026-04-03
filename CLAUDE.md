# shinryu-mcp

Unified analytical query plane for the Shinryū data platform. Embeds Apache
DataFusion to provide ANSI SQL over all observability signals stored as
NDJSON/Parquet files. Single binary, zero infrastructure.

## Architecture

```
Vector → Bronze (NDJSON) → Refiner → Silver (Parquet) → Materializer → Gold (views)
                                                ↓
                                        DataFusion (embedded)
                                        ANSI SQL + custom UDFs
                                        MCP tools for Claude
```

All 13 signal sources converge into one unified schema queryable with
standard ANSI SQL JOINs, CTEs, window functions, and aggregations.

## Build & Run

```bash
cargo build --release

# One-shot SQL query
shinryu-mcp --sql 'SELECT count(*) FROM events'

# Pre-built analysis
shinryu-mcp --experiment exp-001 --analysis timeline
shinryu-mcp --experiment exp-001 --analysis bottleneck
shinryu-mcp --experiment exp-001 --analysis configs
shinryu-mcp --experiment exp-001 --analysis scaling

# Ingest datasets
shinryu-mcp --ingest --datasets-config datasets.yaml

# Compare experiments
shinryu-mcp --experiment exp-001 --analysis compare --compare-with exp-002
```

## Three-Tier Data Lakehouse

| Tier | Format | Latency | Retention | Table |
|------|--------|---------|-----------|-------|
| Bronze | NDJSON | 30s (Vector) | 7d | `bronze_events` |
| Silver | Parquet | 1min (refiner) | 30d | `events` (default) |
| Gold | Parquet | 5min (materializer) | 90d | Named views |

The refiner is **event-driven** (inotify watches Bronze for new files).
The materializer is **channel-driven** (triggered by refiner output).
Both have zero CPU usage when idle.

## MCP Tools

| Tool | Input | What |
|------|-------|------|
| `query` | SQL string | Execute arbitrary ANSI SQL |
| `timeline` | experiment_id | Chronological event stream |
| `bottleneck` | experiment_id | Cross-signal correlation |
| `compare` | exp_a, exp_b | Side-by-side comparison |
| `network` | experiment_id | Hubble flow summary |
| `summary` | experiment_id | Per-scenario burst summary |
| `phases` | experiment_id | Phase duration breakdown |
| `configs` | — | All experiments ranked by speed |
| `scaling` | — | Pods-per-GW throughput analysis |

## Custom UDFs

| Function | Signature | Purpose |
|----------|-----------|---------|
| `tumbling_window` | `(timestamp_ms, window_ms) → Int64` | Time bucketing |
| `asof_nearest` | `(ts1, ts2, tolerance_ms) → Int64` | ASOF JOIN helper |

## Schema

All signals in one table (`events`):

**Core:** timestamp, timestamp_ms, cluster, experiment_id, signal_type, source
**K8s:** namespace, pod, container, node, app
**Logs:** level, message, event_type, event_source
**Metrics:** metric_name, metric_value
**Flows:** src_pod, dst_pod, verdict, protocol, l7_type, dns_query, http_method, akeyless_flow
**burst-forge:** scenario, running, pending, injected, injection_rate, elapsed_ms, peak_running
**burst-forge enriched:** burst_replicas, burst_running, burst_injected, burst_success_rate
**Pod details:** restart_count, state_reason, qos_class
**Akeyless:** gw_request_duration_ms, gw_operation, wh_admission_duration_ms
**Raw:** raw (full JSON fallback)

## Example SQL Queries

```sql
-- All experiments ranked by speed
SELECT scenario, gateway_replicas as gw, pods_requested as pods,
       elapsed_ms/1000 as secs, injection_rate as rate
FROM events WHERE signal_type = 'experiment_result'
ORDER BY elapsed_ms LIMIT 10

-- Cross-signal JOIN: CPU at moment of subprocess kills
SELECT k.event_type, m.metric_value as cpu,
       asof_nearest(k.timestamp_ms, m.timestamp_ms, 10000) as delta_ms
FROM events k JOIN events m
  ON asof_nearest(k.timestamp_ms, m.timestamp_ms, 10000) IS NOT NULL
WHERE k.event_type = 'subprocess_kill'
  AND m.signal_type = 'metric'

-- Pods per second throughput by config
SELECT scenario, gateway_replicas as gw,
       CAST(pods_requested AS DOUBLE) / (CAST(elapsed_ms AS DOUBLE) / 1000.0) as pods_per_sec
FROM events WHERE signal_type = 'experiment_result' AND injection_rate >= 99
ORDER BY pods_per_sec DESC

-- Events in 5-second windows
SELECT tumbling_window(timestamp_ms, 5000) as window, count(*) as events
FROM events WHERE experiment_id = 'exp-001' GROUP BY window ORDER BY window
```

## Dataset Ingestion

Configure datasets via `datasets.yaml`:
```yaml
datasets:
  - name: my-experiment-data
    data_file: datasets/my-experiments.json
```

Each dataset is an NDJSON file with one JSON object per line following the
unified schema. Run `shinryu-mcp --ingest --datasets-config datasets.yaml`
to load into Bronze.

## Module Map

| Module | Purpose |
|--------|---------|
| `main.rs` | CLI + MCP server entry point |
| `query.rs` | Three-tier DataFusion session (Bronze/Silver/Gold) |
| `schema.rs` | Unified Arrow schema (50+ fields) |
| `refiner.rs` | Bronze→Silver (inotify event-driven) |
| `materializer.rs` | Silver→Gold (channel-driven from refiner) |
| `compactor.rs` | TTL lifecycle (30-day expiry) |
| `udfs/scalar.rs` | tumbling_window, asof_nearest |
| `udfs/table_fns/` | Pre-built SQL for 9 analytical views |
| `tools/` | MCP tool implementations |
| `ingest/` | Config-driven dataset ingestion |

## Configuration

Via shikumi YAML (`datasets.yaml`) or CLI flags. All paths configurable:
- `--analytics-path` — Bronze/Silver/Gold root
- `--datasets-config` — dataset YAML file
- `--sql` — one-shot SQL query
- `--experiment` + `--analysis` — pre-built analysis

## Dependencies

- `datafusion 45` — Apache DataFusion query engine (ANSI SQL)
- `arrow 54` / `parquet 54` — Apache Arrow columnar format
- `notify 7` — inotify file watcher for event-driven refiner
- `kaname` — MCP server scaffold (rmcp 0.15)
- `shikumi` — Configuration discovery
- `serde_yaml` — YAML config parsing
