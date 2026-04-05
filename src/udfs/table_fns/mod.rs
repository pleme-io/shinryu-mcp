//! Table functions for pre-built analytical queries.
//!
//! Each function executes a complex SQL query and returns results as a virtual table.
//! Called via `SELECT * FROM function_name('arg')`.
//!
//! Currently implemented as SQL-based helpers (not DataFusion TableProvider).
//! The MCP tools in `src/tools/` call these directly.

/// Pre-built SQL for burst experiment summary.
pub fn burst_summary_sql(experiment_id: &str) -> String {
    format!(r#"
        SELECT
            scenario,
            MAX(running) as max_running,
            MAX(injected) as max_injected,
            MAX(injection_rate_pct) as injection_rate_pct,
            MAX(elapsed_ms) as duration_ms,
            MAX(peak_running) as peak_running,
            COUNT(CASE WHEN event_type = 'subprocess_kill' THEN 1 END) as kill_count,
            COUNT(CASE WHEN verdict = 'DROPPED' THEN 1 END) as drop_count
        FROM events
        WHERE experiment_id = '{experiment_id}'
            AND (event_type IN ('POLL_TICK', 'BURST_COMPLETE', 'subprocess_kill')
                 OR signal_type = 'flow')
        GROUP BY scenario
        ORDER BY scenario
    "#)
}

/// Pre-built SQL for experiment comparison.
pub fn experiment_diff_sql(exp_a: &str, exp_b: &str) -> String {
    format!(r#"
        WITH a AS (
            SELECT scenario, injection_rate_pct, elapsed_ms, running, injected
            FROM events WHERE experiment_id = '{exp_a}' AND event_type = 'BURST_COMPLETE'
        ),
        b AS (
            SELECT scenario, injection_rate_pct, elapsed_ms, running, injected
            FROM events WHERE experiment_id = '{exp_b}' AND event_type = 'BURST_COMPLETE'
        )
        SELECT
            COALESCE(a.scenario, b.scenario) as scenario,
            a.injection_rate_pct as "{exp_a}_rate",
            b.injection_rate_pct as "{exp_b}_rate",
            b.injection_rate_pct - a.injection_rate_pct as rate_delta,
            a.elapsed_ms as "{exp_a}_ms",
            b.elapsed_ms as "{exp_b}_ms",
            CAST(b.elapsed_ms AS BIGINT) - CAST(a.elapsed_ms AS BIGINT) as ms_delta,
            CASE
                WHEN b.injection_rate_pct > a.injection_rate_pct + 1 THEN 'BETTER'
                WHEN b.injection_rate_pct < a.injection_rate_pct - 1 THEN 'WORSE'
                ELSE 'SAME'
            END as verdict
        FROM a FULL OUTER JOIN b ON a.scenario = b.scenario
        ORDER BY scenario
    "#)
}

/// Pre-built SQL for bottleneck ranking.
pub fn bottleneck_rank_sql(experiment_id: &str) -> String {
    format!(r#"
        WITH kills AS (
            SELECT COUNT(*) as cnt FROM events
            WHERE experiment_id = '{experiment_id}' AND event_type = 'subprocess_kill'
        ),
        connectivity AS (
            SELECT COUNT(*) as cnt FROM events
            WHERE experiment_id = '{experiment_id}' AND event_type IN ('connectivity_lost', 'connectivity_resumed')
        ),
        drops AS (
            SELECT COUNT(*) as cnt FROM events
            WHERE experiment_id = '{experiment_id}' AND signal_type = 'flow' AND verdict = 'DROPPED'
        ),
        high_cpu AS (
            SELECT COUNT(*) as cnt FROM events
            WHERE experiment_id = '{experiment_id}' AND signal_type = 'metric'
                AND metric_name LIKE '%cpu%' AND metric_value > 0.8
        ),
        restarts AS (
            SELECT COALESCE(SUM(restart_count), 0) as cnt FROM events
            WHERE experiment_id = '{experiment_id}' AND restart_count > 0
        )
        SELECT 1 as rank, 'CPU_SATURATION' as bottleneck, high_cpu.cnt as evidence, 'Increase GW CPU limit' as recommendation FROM high_cpu WHERE high_cpu.cnt > 0
        UNION ALL
        SELECT 2, 'SUBPROCESS_KILLS', kills.cnt, 'CPU pressure killing curl subprocesses' FROM kills WHERE kills.cnt > 0
        UNION ALL
        SELECT 3, 'CONNECTIVITY_CYCLING', connectivity.cnt, 'GW losing/resuming connectivity under load' FROM connectivity WHERE connectivity.cnt > 0
        UNION ALL
        SELECT 4, 'NETWORK_DROPS', drops.cnt, 'Packets dropped (check NetworkPolicy/Cilium)' FROM drops WHERE drops.cnt > 0
        UNION ALL
        SELECT 5, 'POD_RESTARTS', restarts.cnt, 'Pods crashing during injection' FROM restarts WHERE restarts.cnt > 0
        ORDER BY rank
    "#)
}

/// Pre-built SQL for phase timing breakdown.
pub fn phase_breakdown_sql(experiment_id: &str) -> String {
    format!(r#"
        SELECT
            scenario,
            event_type,
            elapsed_ms as duration_ms
        FROM events
        WHERE experiment_id = '{experiment_id}'
            AND event_type = 'PHASE_COMPLETE'
        ORDER BY scenario, timestamp_ms
    "#)
}

/// Pre-built SQL for Hubble flow summary.
pub fn flow_summary_sql(experiment_id: &str) -> String {
    format!(r#"
        SELECT
            COALESCE(src_pod, 'external') as source,
            COALESCE(dst_pod, 'external') as destination,
            protocol,
            verdict,
            l7_type,
            COUNT(*) as flow_count,
            CASE WHEN akeyless_flow = true THEN 'AKEYLESS' ELSE '' END as akeyless
        FROM events
        WHERE experiment_id = '{experiment_id}' AND signal_type = 'flow'
        GROUP BY src_pod, dst_pod, protocol, verdict, l7_type, akeyless_flow
        ORDER BY flow_count DESC
        LIMIT 50
    "#)
}

// ── Prediction accuracy views ──────────────────────────────────

/// Pre-built SQL for prediction accuracy analysis — compares scaling formula
/// predictions against actual burst results.
pub fn prediction_accuracy_sql(experiment_id: &str) -> String {
    format!(r#"
        SELECT
            scenario,
            predicted_gw_replicas,
            CAST(burst_replicas AS DOUBLE) / NULLIF(CAST(elapsed_ms AS DOUBLE) / 1000.0, 0) as actual_throughput,
            predicted_throughput as predicted_throughput,
            predicted_min_secs,
            CAST(elapsed_ms AS DOUBLE) / 1000.0 as actual_secs,
            prediction_verdict as verdict,
            prediction_error_pct as error_pct,
            prediction_formula as formula,
            CASE
                WHEN predicted_gw_replicas > burst_replicas THEN 'OVER_PROVISIONED'
                WHEN predicted_gw_replicas < burst_replicas THEN 'UNDER_PROVISIONED'
                ELSE 'MATCHED'
            END as gw_provision_verdict
        FROM events
        WHERE experiment_id = '{experiment_id}'
            AND event_type = 'BURST_COMPLETE'
            AND predicted_gw_replicas IS NOT NULL
        ORDER BY scenario
    "#)
}

// ── Experiment analysis views ───────────────────────────────────

/// Config comparison: rank all experiment configs by speed for a given pod count.
pub fn config_comparison_sql() -> String {
    r#"
        SELECT
            experiment_id,
            scenario,
            gateway_replicas as gw,
            webhook_replicas as wh,
            pods_requested,
            running as pods_running,
            elapsed_ms as duration_ms,
            injection_rate,
            init_containers,
            CAST(elapsed_ms AS DOUBLE) / 1000.0 as duration_secs,
            CAST(pods_requested AS DOUBLE) / NULLIF(CAST(elapsed_ms AS DOUBLE) / 1000.0, 0) as pods_per_sec,
            RANK() OVER (PARTITION BY pods_requested ORDER BY elapsed_ms) as speed_rank
        FROM events
        WHERE signal_type = 'experiment_result'
        ORDER BY pods_requested, speed_rank
    "#.to_string()
}

/// Scaling formulas: pods per gateway, throughput derived from all experiments.
pub fn scaling_formulas_sql() -> String {
    r#"
        SELECT
            experiment_id,
            scenario,
            gateway_replicas as gw,
            webhook_replicas as wh,
            pods_requested as pods,
            elapsed_ms as duration_ms,
            CAST(pods_requested AS DOUBLE) / NULLIF(CAST(gateway_replicas AS DOUBLE), 0) as pods_per_gw,
            CAST(pods_requested AS DOUBLE) / NULLIF(CAST(elapsed_ms AS DOUBLE) / 1000.0, 0) as pods_per_sec,
            CAST(gateway_replicas AS DOUBLE) / NULLIF(CAST(webhook_replicas AS DOUBLE), 0) as gw_wh_ratio
        FROM events
        WHERE signal_type = 'experiment_result'
            AND injection_rate >= 99.0
        ORDER BY pods_requested, gw
    "#.to_string()
}
