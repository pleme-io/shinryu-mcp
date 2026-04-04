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
mod ingest;
mod materializer;
mod mcp;
mod query;
mod refiner;
mod schema;
mod tools;
mod udfs;

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
    let bronze_path = format!("{}/bronze", cli.analytics_path);
    let silver_path = format!("{}/silver", cli.analytics_path);
    let gold_path = format!("{}/gold", cli.analytics_path);

    // Channel: refiner → materializer (event-driven, no polling)
    let (silver_tx, silver_rx) = tokio::sync::mpsc::channel(100);

    // Spawn Bronze → Silver refiner (inotify event-driven)
    let refiner_bronze = bronze_path.clone();
    let refiner_silver = silver_path.clone();
    tokio::spawn(async move {
        refiner::run(refiner_bronze, refiner_silver, silver_tx).await;
    });

    // Spawn Silver → Gold materializer (channel-driven from refiner)
    let mat_silver = silver_path.clone();
    let mat_gold = gold_path.clone();
    tokio::spawn(async move {
        materializer::run(mat_silver, mat_gold, silver_rx).await;
    });

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

        let count = ingest::confluence::ingest_datasets(&config.datasets, &bronze_path)?;
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

    // MCP server mode (default) — run MCP server + health endpoint + background tasks
    tracing::info!("shinryu-mcp starting MCP server");
    tracing::info!("Bronze: {bronze_path}");
    tracing::info!("Silver: {silver_path}");
    tracing::info!("Gold: {gold_path}");

    // Health endpoint for K8s probes (port 8081)
    tokio::spawn(async { health::serve(8081).await });

    // MCP server on stdio (blocks until client disconnects or signal)
    mcp::run(Arc::new(ctx)).await?;

    Ok(())
}
