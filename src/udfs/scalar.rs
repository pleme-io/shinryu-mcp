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
/// Returns signed distance (ts2 - ts1) if within tolerance, NULL otherwise.
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
