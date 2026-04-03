//! MCP tool: execute arbitrary ANSI SQL against all signals.

use datafusion::prelude::SessionContext;

/// Execute a SQL query against the unified events table.
///
/// All signals (logs, metrics, events, flows, profiles) are in one table.
/// Use standard ANSI SQL with JOINs, CTEs, window functions, aggregations.
///
/// # Examples
///
/// ```sql
/// -- Timeline of all signals for an experiment
/// SELECT timestamp, signal_type, event_type, message
/// FROM events WHERE experiment_id = 'exp-001'
/// ORDER BY timestamp LIMIT 500
///
/// -- Cross-signal JOIN: CPU at moment of subprocess kills
/// SELECT k.timestamp, k.message, m.metric_value as cpu
/// FROM events k JOIN events m
///   ON m.timestamp BETWEEN k.timestamp - INTERVAL '5' SECOND AND k.timestamp
/// WHERE k.event_type = 'subprocess_kill'
///   AND m.signal_type = 'metric' AND m.metric_name LIKE '%cpu%'
/// ```
pub async fn query(ctx: &SessionContext, sql: &str) -> anyhow::Result<String> {
    crate::query::execute_sql(ctx, sql).await
}
