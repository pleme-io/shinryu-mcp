//! MCP server for Shinryū analytical query plane.
//!
//! 9 tools exposing DataFusion SQL queries over the unified events table.
//! Pattern follows zoekt-mcp: `#[tool_router]` + `#[tool_handler]` from rmcp.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use rmcp::{
    ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
    transport::{stdio, streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService}},
};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::{query, tools, udfs};

// ── Tool input types ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct QueryInput {
    #[schemars(description = "ANSI SQL query against the unified events table. Supports JOINs, CTEs, window functions, aggregations. Custom UDFs: tumbling_window(timestamp_ms, window_ms), asof_nearest(ts1, ts2, tolerance_ms).")]
    pub sql: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TimelineInput {
    #[schemars(description = "Experiment ID (e.g. 'phase1-gw-memory-sweep-20260403T1430z')")]
    pub experiment_id: String,
    #[schemars(description = "Max events to return (default 500)")]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BottleneckInput {
    #[schemars(description = "Experiment ID to analyze for bottlenecks")]
    pub experiment_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CompareInput {
    #[schemars(description = "First experiment ID")]
    pub experiment_a: String,
    #[schemars(description = "Second experiment ID to compare against")]
    pub experiment_b: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct NetworkInput {
    #[schemars(description = "Experiment ID for Hubble network flow analysis")]
    pub experiment_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExperimentInput {
    #[schemars(description = "Experiment ID")]
    pub experiment_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct EmptyInput {}

// ── MCP Server ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ShinryuMcp {
    ctx: Arc<SessionContext>,
    tool_router: ToolRouter<Self>,
}

impl std::fmt::Debug for ShinryuMcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShinryuMcp").finish()
    }
}

#[tool_router]
impl ShinryuMcp {
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self {
            ctx,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Execute arbitrary ANSI SQL against all observability signals (logs, metrics, flows, experiment results). The unified 'events' table has 50+ fields. Custom UDFs: tumbling_window(timestamp_ms, window_ms), asof_nearest(ts1, ts2, tolerance_ms).")]
    async fn query(&self, Parameters(input): Parameters<QueryInput>) -> String {
        match tools::query::query(&self.ctx, &input.sql).await {
            Ok(result) => result,
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Chronological event stream for an experiment — all signal types (burst events, metrics, flows, pod states) ordered by time.")]
    async fn timeline(&self, Parameters(input): Parameters<TimelineInput>) -> String {
        match tools::timeline::timeline(&self.ctx, &input.experiment_id, input.limit.unwrap_or(500)).await {
            Ok(result) => result,
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Cross-signal bottleneck analysis — correlates CPU spikes, subprocess kills, pod failures, and connectivity events to identify root causes.")]
    async fn bottleneck(&self, Parameters(input): Parameters<BottleneckInput>) -> String {
        match tools::bottleneck::bottleneck(&self.ctx, &input.experiment_id).await {
            Ok(result) => result,
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Side-by-side comparison of two experiments — duration, success rate, throughput, pod counts, injection rates.")]
    async fn compare(&self, Parameters(input): Parameters<CompareInput>) -> String {
        match tools::compare::compare(&self.ctx, &input.experiment_a, &input.experiment_b).await {
            Ok(result) => result,
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Hubble network flow summary — traffic by source/destination pod, verdict (FORWARDED/DROPPED), protocol, DNS queries, HTTP methods.")]
    async fn network(&self, Parameters(input): Parameters<NetworkInput>) -> String {
        match tools::network::network(&self.ctx, &input.experiment_id).await {
            Ok(result) => result,
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Per-scenario burst summary — pods/sec, success rate, elapsed time, gateway/webhook counts.")]
    async fn summary(&self, Parameters(input): Parameters<ExperimentInput>) -> String {
        let sql = udfs::table_fns::burst_summary_sql(&input.experiment_id);
        match query::execute_sql(&self.ctx, &sql).await {
            Ok(result) => result,
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Phase duration breakdown — RESET, WARMUP, EXECUTION timing for each scenario in an experiment.")]
    async fn phases(&self, Parameters(input): Parameters<ExperimentInput>) -> String {
        let sql = udfs::table_fns::phase_breakdown_sql(&input.experiment_id);
        match query::execute_sql(&self.ctx, &sql).await {
            Ok(result) => result,
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "All experiments ranked by speed — fastest injection rates across all recorded experiments.")]
    async fn configs(&self, Parameters(_input): Parameters<EmptyInput>) -> String {
        let sql = udfs::table_fns::config_comparison_sql();
        match query::execute_sql(&self.ctx, &sql).await {
            Ok(result) => result,
            Err(e) => format!("Error: {e}"),
        }
    }

    #[tool(description = "Pods-per-gateway throughput analysis — how injection speed scales with gateway replica count.")]
    async fn scaling(&self, Parameters(_input): Parameters<EmptyInput>) -> String {
        let sql = udfs::table_fns::scaling_formulas_sql();
        match query::execute_sql(&self.ctx, &sql).await {
            Ok(result) => result,
            Err(e) => format!("Error: {e}"),
        }
    }
}

#[tool_handler]
impl ServerHandler for ShinryuMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "Shinryū analytical query plane — ANSI SQL over all observability signals. \
                 Query the unified 'events' table containing logs, metrics, Hubble flows, \
                 burst-forge experiment results, and pod state details. Custom UDFs: \
                 tumbling_window(timestamp_ms, window_ms) for time bucketing, \
                 asof_nearest(ts1, ts2, tolerance_ms) for cross-signal correlation."
                    .into(),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

/// Run the MCP server on stdio transport (for local Claude integration).
pub async fn run(ctx: Arc<SessionContext>) -> anyhow::Result<()> {
    let server = ShinryuMcp::new(ctx).serve(stdio()).await
        .map_err(|e| anyhow::anyhow!("MCP server failed to start: {e}"))?;
    server.waiting().await
        .map_err(|e| anyhow::anyhow!("MCP server error: {e}"))?;
    Ok(())
}

/// Run the MCP server on HTTP transport (for K8s daemon mode).
/// Accepts MCP JSON-RPC over HTTP with SSE streaming responses.
pub async fn run_http(ctx: Arc<SessionContext>, port: u16) -> anyhow::Result<()> {
    let ct = CancellationToken::new();

    let service: StreamableHttpService<ShinryuMcp> = StreamableHttpService::new(
        move || Ok(ShinryuMcp::new(ctx.clone())),
        Default::default(),
        StreamableHttpServerConfig {
            stateful_mode: true,
            cancellation_token: ct.child_token(),
            ..Default::default()
        },
    );

    let router = axum::Router::new().nest_service("/mcp", service);
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await
        .map_err(|e| anyhow::anyhow!("MCP HTTP server failed to bind to {addr}: {e}"))?;

    tracing::info!("MCP HTTP server listening on {addr}/mcp");

    axum::serve(listener, router)
        .with_graceful_shutdown(async move { ct.cancelled_owned().await })
        .await
        .map_err(|e| anyhow::anyhow!("MCP HTTP server error: {e}"))?;

    Ok(())
}
