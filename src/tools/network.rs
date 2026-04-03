//! MCP tool: Hubble network flow summary for an experiment.

use datafusion::prelude::SessionContext;

/// Summarize network flows grouped by source/destination, verdict, protocol.
pub async fn network(ctx: &SessionContext, experiment_id: &str) -> anyhow::Result<String> {
    let sql = crate::udfs::table_fns::flow_summary_sql(experiment_id);
    crate::query::execute_sql(ctx, &sql).await
}
