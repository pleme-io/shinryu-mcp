//! Stateless HTTP query endpoint for Shinryu.
//!
//! POST /query with {"sql": "SELECT ..."} returns JSON results.
//! Runs alongside the MCP streaming endpoint on the same axum router.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};

use crate::metrics::Metrics;
use crate::session::ManagedSession;

#[derive(Clone)]
pub struct AppState {
    pub session: Arc<ManagedSession>,
    pub metrics: Arc<Metrics>,
}

#[derive(Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
    pub row_count: usize,
    pub elapsed_ms: u64,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

async fn query_handler(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let start = Instant::now();
    state
        .metrics
        .queries_executed
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let ctx = state.session.get();
    match crate::query::execute_sql(&ctx, &req.sql).await {
        Ok(result) => {
            let elapsed = start.elapsed().as_millis() as u64;
            state.metrics.record_query_latency(elapsed as f64);

            // Parse the tab-separated result into structured JSON
            let lines: Vec<&str> = result.lines().collect();
            if lines.is_empty() {
                return Json(serde_json::json!(QueryResponse {
                    columns: vec![],
                    rows: vec![],
                    row_count: 0,
                    elapsed_ms: elapsed,
                }))
                .into_response();
            }

            // First line is column headers (tab-separated from DataFusion)
            let columns: Vec<String> = lines[0]
                .split('\t')
                .map(|s| s.trim().to_string())
                .collect();
            let mut rows = Vec::new();

            for line in &lines[1..] {
                if line.trim().is_empty() {
                    continue;
                }
                let values: Vec<&str> = line.split('\t').collect();
                let mut row = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val = values.get(i).unwrap_or(&"");
                    // Try to parse as number, fall back to string
                    if let Ok(n) = val.parse::<f64>() {
                        row.insert(col.clone(), serde_json::json!(n));
                    } else {
                        row.insert(col.clone(), serde_json::json!(val));
                    }
                }
                rows.push(serde_json::Value::Object(row));
            }

            let row_count = rows.len();
            Json(serde_json::json!(QueryResponse {
                columns,
                rows,
                row_count,
                elapsed_ms: elapsed,
            }))
            .into_response()
        }
        Err(e) => {
            state
                .metrics
                .query_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!(ErrorResponse {
                    error: e.to_string(),
                })),
            )
                .into_response()
        }
    }
}

async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.to_prometheus()
}

/// Build the HTTP router with /query, /health, and /metrics endpoints.
pub fn router(session: Arc<ManagedSession>, metrics: Arc<Metrics>) -> Router {
    let state = AppState { session, metrics };
    Router::new()
        .route("/query", post(query_handler))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_returns_ok() {
        let tmp = tempfile::TempDir::new().unwrap();
        for tier in ["bronze", "silver", "gold"] {
            std::fs::create_dir_all(tmp.path().join(tier)).unwrap();
        }
        let session = Arc::new(ManagedSession::new(tmp.path()).await.unwrap());
        let metrics = Arc::new(Metrics::new());
        let app = router(session, metrics);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }
}
