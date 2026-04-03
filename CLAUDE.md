# shinryu-mcp

Unified analytical query plane for the Shinryū data platform. Embeds Apache
DataFusion to provide ANSI SQL queries over all observability signals stored
as NDJSON/Parquet files. Single binary, zero infrastructure.

## Architecture

```
Vector → NDJSON files → DataFusion (embedded) → SQL results
```

All 13 signal sources converge into one unified schema queryable with
standard ANSI SQL JOINs, CTEs, window functions, and aggregations.

## Build & Run

```bash
cargo build --release
shinryu-mcp --sql 'SELECT count(*) FROM events'
shinryu-mcp --experiment exp-001 --analysis timeline
shinryu-mcp --experiment exp-001 --analysis bottleneck
```

## MCP Tools

| Tool | Input | What |
|------|-------|------|
| `query` | SQL string | Execute arbitrary ANSI SQL |
| `timeline` | experiment_id | Chronological event stream |
| `bottleneck` | experiment_id | Cross-signal correlation |
| `compare` | exp_a, exp_b | Side-by-side comparison |
| `network` | experiment_id | Hubble flow summary |

## Schema

All signals in one table (`events`):
- Core: timestamp, cluster, experiment_id, signal_type, source
- K8s: namespace, pod, node, app
- Logs: level, message, event_type
- Metrics: metric_name, metric_value
- Flows: src_pod, dst_pod, verdict, protocol, dns_query, http_method
- burst-forge: scenario, running, pending, injected, injection_rate
- Akeyless: gw_request_duration_ms, wh_admission_duration_ms

## Dependencies

- `datafusion` — Apache DataFusion query engine (ANSI SQL)
- `arrow` / `parquet` — Apache Arrow columnar format
- `kaname` — MCP server scaffold (rmcp 0.15)
- `shikumi` — Configuration discovery
