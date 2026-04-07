//! Scalar UDFs for time-series analysis.

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::*;
use std::sync::Arc;

/// `tumbling_window(timestamp_ms, window_size_ms) → Int64`
///
/// Assigns each row to a non-overlapping time window.
pub fn tumbling_window_udf() -> ScalarUDF {
    create_udf(
        "tumbling_window",
        vec![DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let ts = match &args[0] {
                ColumnarValue::Array(a) => a.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
                ColumnarValue::Scalar(s) => {
                    let v = match s {
                        datafusion::scalar::ScalarValue::Int64(v) => *v,
                        _ => None,
                    };
                    Int64Array::from(vec![v])
                }
            };
            let ws = match &args[1] {
                ColumnarValue::Array(a) => a.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
                ColumnarValue::Scalar(s) => {
                    let v = match s {
                        datafusion::scalar::ScalarValue::Int64(v) => *v,
                        _ => None,
                    };
                    Int64Array::from(vec![v; ts.len()])
                }
            };

            let result: Int64Array = ts.iter().zip(ws.iter())
                .map(|(t, w)| match (t, w) {
                    (Some(t), Some(w)) if w > 0 => Some((t / w) * w),
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }),
    )
}

/// `asof_nearest(ts1, ts2, tolerance_ms) → Int64`
///
/// Returns signed distance (ts2 − ts1) if within tolerance, NULL otherwise.
pub fn asof_nearest_udf() -> ScalarUDF {
    create_udf(
        "asof_nearest",
        vec![DataType::Int64, DataType::Int64, DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            let extract_i64 = |cv: &ColumnarValue, len: usize| -> Int64Array {
                match cv {
                    ColumnarValue::Array(a) => a.as_any().downcast_ref::<Int64Array>().unwrap().clone(),
                    ColumnarValue::Scalar(s) => {
                        let v = match s {
                            datafusion::scalar::ScalarValue::Int64(v) => *v,
                            _ => None,
                        };
                        Int64Array::from(vec![v; len])
                    }
                }
            };

            let len = match &args[0] {
                ColumnarValue::Array(a) => a.len(),
                ColumnarValue::Scalar(_) => 1,
            };
            let ts1 = extract_i64(&args[0], len);
            let ts2 = extract_i64(&args[1], len);
            let tol = extract_i64(&args[2], len);

            let result: Int64Array = ts1.iter().zip(ts2.iter()).zip(tol.iter())
                .map(|((t1, t2), tol)| match (t1, t2, tol) {
                    (Some(a), Some(b), Some(t)) => {
                        let delta = b - a;
                        if delta.abs() <= t { Some(delta) } else { None }
                    }
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use datafusion::prelude::SessionContext;

    async fn ctx_with_udfs() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(tumbling_window_udf());
        ctx.register_udf(asof_nearest_udf());
        ctx
    }

    fn collect_i64(batches: &[arrow::record_batch::RecordBatch], col: usize) -> Vec<Option<i64>> {
        batches
            .iter()
            .flat_map(|b| {
                let arr = b.column(col).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..arr.len()).map(move |i| {
                    if arr.is_null(i) { None } else { Some(arr.value(i)) }
                })
            })
            .collect()
    }

    #[tokio::test]
    async fn tumbling_window_basic_bucketing() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT tumbling_window(ts, 5000) as w FROM (VALUES (1000), (4999), (5000), (9999), (10000)) AS t(ts)")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let vals = collect_i64(&batches, 0);
        assert_eq!(vals, vec![Some(0), Some(0), Some(5000), Some(5000), Some(10000)]);
    }

    #[tokio::test]
    async fn tumbling_window_zero_window_returns_null() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT tumbling_window(1000, 0) as w")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let vals = collect_i64(&batches, 0);
        assert_eq!(vals, vec![None]);
    }

    #[tokio::test]
    async fn tumbling_window_negative_window_returns_null() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT tumbling_window(1000, -5) as w")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let vals = collect_i64(&batches, 0);
        assert_eq!(vals, vec![None]);
    }

    #[tokio::test]
    async fn tumbling_window_null_inputs() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT tumbling_window(CAST(NULL AS BIGINT), 5000) as w")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let vals = collect_i64(&batches, 0);
        assert_eq!(vals, vec![None]);
    }

    #[tokio::test]
    async fn tumbling_window_negative_timestamp() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT tumbling_window(-7000, 5000) as w")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let vals = collect_i64(&batches, 0);
        assert_eq!(vals, vec![Some(-5000)]);
    }

    #[tokio::test]
    async fn asof_nearest_within_tolerance() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT asof_nearest(100, 105, 10) as d")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let vals = collect_i64(&batches, 0);
        assert_eq!(vals, vec![Some(5)]);
    }

    #[tokio::test]
    async fn asof_nearest_outside_tolerance() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT asof_nearest(100, 200, 10) as d")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let vals = collect_i64(&batches, 0);
        assert_eq!(vals, vec![None]);
    }

    #[tokio::test]
    async fn asof_nearest_exact_boundary() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT asof_nearest(100, 110, 10) as d")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let vals = collect_i64(&batches, 0);
        assert_eq!(vals, vec![Some(10)], "boundary: abs(delta) == tolerance should match");
    }

    #[tokio::test]
    async fn asof_nearest_just_beyond_boundary() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT asof_nearest(100, 111, 10) as d")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let vals = collect_i64(&batches, 0);
        assert_eq!(vals, vec![None], "abs(delta) > tolerance should be NULL");
    }

    #[tokio::test]
    async fn asof_nearest_negative_delta() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT asof_nearest(110, 100, 10) as d")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let vals = collect_i64(&batches, 0);
        assert_eq!(vals, vec![Some(-10)], "negative delta at boundary should match");
    }

    #[tokio::test]
    async fn asof_nearest_null_any_arg() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT asof_nearest(CAST(NULL AS BIGINT), 100, 10) as d")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let vals = collect_i64(&batches, 0);
        assert_eq!(vals, vec![None]);
    }

    #[tokio::test]
    async fn asof_nearest_zero_tolerance() {
        let ctx = ctx_with_udfs().await;
        let df = ctx
            .sql("SELECT asof_nearest(100, 100, 0) as d1, asof_nearest(100, 101, 0) as d2")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let d1 = collect_i64(&batches, 0);
        let d2 = collect_i64(&batches, 1);
        assert_eq!(d1, vec![Some(0)], "identical timestamps with zero tolerance should match");
        assert_eq!(d2, vec![None], "different timestamps with zero tolerance should not match");
    }
}
