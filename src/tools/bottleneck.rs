//! MCP tool: cross-signal bottleneck analysis.

use datafusion::prelude::SessionContext;

/// Identify bottlenecks by correlating critical events with metrics.
///
/// Ranks bottlenecks by severity: CPU saturation, subprocess kills,
/// connectivity cycling, network drops, pod restarts.
pub async fn bottleneck(ctx: &SessionContext, experiment_id: &str) -> anyhow::Result<String> {
    let sql = crate::udfs::table_fns::bottleneck_rank_sql(experiment_id);
    crate::query::execute_sql(ctx, &sql).await
}
