//! MCP tool: compare two experiments side by side.

use datafusion::prelude::SessionContext;

/// Compare two experiments by their BURST_COMPLETE results.
/// Shows injection rate, duration, and improvement verdict per scenario.
pub async fn compare(ctx: &SessionContext, exp_a: &str, exp_b: &str) -> anyhow::Result<String> {
    let sql = crate::udfs::table_fns::experiment_diff_sql(exp_a, exp_b);
    crate::query::execute_sql(ctx, &sql).await
}
