//! MCP tool: chronological experiment timeline across all signal types.

use datafusion::prelude::SessionContext;

/// Get a chronological timeline of all events for an experiment.
///
/// Returns all signal types (logs, metrics, events, flows) in timestamp order.
/// Uses SELECT * to work with any schema — DataFusion infers columns from NDJSON.
pub async fn timeline(ctx: &SessionContext, experiment_id: &str, limit: u32) -> anyhow::Result<String> {
    let sql = format!(
        r#"
        SELECT *
        FROM events
        WHERE experiment_id = '{experiment_id}'
        ORDER BY timestamp
        LIMIT {limit}
        "#
    );
    crate::query::execute_sql(ctx, &sql).await
}
