//! shinryu-mcp — Unified analytical query plane for the Shinryū data platform.
//!
//! Embeds Apache DataFusion to provide ANSI SQL queries over all observability
//! signals (logs, metrics, events, flows, profiles) stored as NDJSON files.
//!
//! MCP tools:
//! - query: Execute arbitrary ANSI SQL
//! - timeline: Chronological experiment event stream
//! - bottleneck: Cross-signal correlation to identify bottlenecks
//! - compare: Side-by-side experiment comparison
//! - network: Hubble flow summary

mod compactor;
mod health;
mod http;
mod ingest;
mod materializer;
mod mcp;
mod metrics;
mod query;
mod refiner;
mod schema;
mod session;
mod tools;
mod udfs;
mod watcher;

use std::sync::Arc;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "shinryu-mcp",
    about = "Shinryū analytical query plane — ANSI SQL over all observability signals",
    version
)]
struct Cli {
    /// Path to analytics NDJSON files (Vector output directory)
    #[arg(long, default_value = "/var/lib/vector/analytics")]
    analytics_path: String,

    /// Run a one-shot SQL query (non-MCP mode for testing)
    #[arg(long)]
    sql: Option<String>,

    /// Experiment ID for pre-built analysis tools
    #[arg(long)]
    experiment: Option<String>,

    /// Analysis mode: timeline, bottleneck, compare, network
    #[arg(long)]
    analysis: Option<String>,

    /// Second experiment ID (for compare mode)
    #[arg(long)]
    compare_with: Option<String>,

    /// Ingest configured datasets into Bronze tier
    #[arg(long)]
    ingest: bool,

    /// Dataset config file (YAML with datasets list)
    #[arg(long, default_value = "datasets.yaml")]
    datasets_config: String,

    /// Result limit
    #[arg(long, default_value = "500")]
    limit: u32,

    /// Daemon mode: run refiner/materializer + health endpoint without MCP server.
    /// Use for K8s Deployments where no stdio MCP client is attached.
    #[arg(long)]
    daemon: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    // Create DataFusion session with events table
    // Create DataFusion session and register custom UDFs
    let ctx = query::create_session(&cli.analytics_path).await?;
    udfs::register_all(&ctx);

    // Bronze/Silver/Gold data lakehouse paths
    let analytics = std::path::Path::new(&cli.analytics_path);
    let bronze_path = analytics.join("bronze");
    let silver_path = analytics.join("silver");
    let gold_path = analytics.join("gold");

    // Shared metrics
    let metrics = Arc::new(metrics::Metrics::new());

    // Directory watcher with broadcast event bus
    let dir_watcher = watcher::DirectoryWatcher::new(analytics, watcher::WatcherConfig::default())?;
    let fs_events = dir_watcher.subscribe();

    // Channel: refiner → materializer (event-driven, no polling)
    let (silver_tx, silver_rx) = tokio::sync::mpsc::channel(100);

    // Channel: materializer → session refresh
    let (session_tx, session_rx) = tokio::sync::mpsc::channel(32);

    // Spawn Bronze → Silver refiner (broadcast consumer)
    {
        let bronze = bronze_path.clone();
        let silver = silver_path.clone();
        let m = Arc::clone(&metrics);
        tokio::spawn(async move {
            refiner::run(bronze, silver, fs_events, silver_tx, m).await;
        });
    }

    // Spawn Silver → Gold materializer (channel-driven from refiner)
    {
        let silver = silver_path.clone();
        let gold = gold_path.clone();
        let m = Arc::clone(&metrics);
        tokio::spawn(async move {
            materializer::run(silver, gold, silver_rx, session_tx, m).await;
        });
    }

    // Spawn session refresh loop
    let managed_session = Arc::new(session::ManagedSession::new(analytics).await?);
    {
        let ms = Arc::clone(&managed_session);
        tokio::spawn(async move {
            session::run_refresh_loop(ms, session_rx).await;
        });
    }

    // Keep the watcher alive for the lifetime of the process
    let _watcher_guard = dir_watcher;

    // Spawn TTL lifecycle cleanup (30-day expiry)
    let lifecycle_path = cli.analytics_path.clone();
    tokio::spawn(async move {
        compactor::run_compaction_loop(lifecycle_path).await;
    });

    // Ingest configured datasets into Bronze
    if cli.ingest {
        let config_path = &cli.datasets_config;
        let config_content = std::fs::read_to_string(config_path)
            .map_err(|e| anyhow::anyhow!("Failed to read datasets config '{config_path}': {e}"))?;

        #[derive(serde::Deserialize)]
        struct DatasetsFile {
            datasets: Vec<ingest::confluence::DatasetConfig>,
        }

        let config: DatasetsFile = serde_yaml::from_str(&config_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse datasets config: {e}"))?;

        let count = ingest::confluence::ingest_datasets(&config.datasets, &bronze_path.to_string_lossy())?;
        println!("Ingested {count} records from {} datasets into Bronze", config.datasets.len());
        println!("Refiner will produce Silver Parquet on next cycle");
        return Ok(());
    }

    // One-shot SQL mode (for testing without MCP)
    if let Some(sql) = &cli.sql {
        let result = tools::query::query(&ctx, sql).await?;
        println!("{result}");
        return Ok(());
    }

    // Pre-built analysis mode
    if let Some(analysis) = &cli.analysis {
        let experiment = cli.experiment.as_deref()
            .ok_or_else(|| anyhow::anyhow!("--experiment required for analysis mode"))?;

        let result = match analysis.as_str() {
            "timeline" => tools::timeline::timeline(&ctx, experiment, cli.limit).await?,
            "bottleneck" => tools::bottleneck::bottleneck(&ctx, experiment).await?,
            "compare" => {
                let other = cli.compare_with.as_deref()
                    .ok_or_else(|| anyhow::anyhow!("--compare-with required for compare mode"))?;
                tools::compare::compare(&ctx, experiment, other).await?
            }
            "network" => tools::network::network(&ctx, experiment).await?,
            "summary" => {
                let sql = udfs::table_fns::burst_summary_sql(experiment);
                query::execute_sql(&ctx, &sql).await?
            }
            "phases" => {
                let sql = udfs::table_fns::phase_breakdown_sql(experiment);
                query::execute_sql(&ctx, &sql).await?
            }
            "configs" => {
                let sql = udfs::table_fns::config_comparison_sql();
                query::execute_sql(&ctx, &sql).await?
            }
            "scaling" => {
                let sql = udfs::table_fns::scaling_formulas_sql();
                query::execute_sql(&ctx, &sql).await?
            }
            _ => anyhow::bail!("Unknown analysis mode: {analysis}. Use: timeline, bottleneck, compare, network, summary, phases, configs, scaling"),
        };
        println!("{result}");
        return Ok(());
    }

    tracing::info!(?bronze_path, "Bronze tier");
    tracing::info!(?silver_path, "Silver tier");
    tracing::info!(?gold_path, "Gold tier");

    // Health endpoint for K8s probes (port 8081)
    tokio::spawn(async { health::serve(8081).await });

    if cli.daemon {
        // Daemon mode: HTTP MCP server on port 9999 + REST query + health + refiner/materializer.
        // Endpoints:
        //   - /mcp     — MCP JSON-RPC over HTTP+SSE (stateful sessions, for Claude)
        //   - /query   — POST stateless SQL query, returns JSON (for cluster-internal use)
        //   - /health  — K8s liveness/readiness
        //   - /metrics — Prometheus metrics
        tracing::info!("shinryu-mcp running in daemon mode on port 9999");
        mcp::run_http(managed_session, metrics, 9999).await?;
    } else {
        // Local mode (default): stdio MCP transport for direct Claude integration.
        tracing::info!("shinryu-mcp starting MCP server on stdio");
        mcp::run(Arc::new(ctx)).await?;
    }

    Ok(())
}
