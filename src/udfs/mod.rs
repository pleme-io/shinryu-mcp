//! Custom DataFusion extensions for burst-forge analytics.
//!
//! Registers domain-specific scalar UDFs, window UDFs, and table functions
//! that make burst-forge analysis elegant and efficient.

pub mod scalar;
pub mod table_fns;

use datafusion::prelude::SessionContext;

/// Register all custom Shinryū functions with a DataFusion session.
pub fn register_all(ctx: &SessionContext) {
    // Scalar UDFs
    ctx.register_udf(scalar::tumbling_window_udf());
    ctx.register_udf(scalar::asof_nearest_udf());

    tracing::info!("Registered Shinryū custom UDFs: tumbling_window, asof_nearest");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_all_makes_udfs_callable() {
        let ctx = SessionContext::new();
        register_all(&ctx);

        let df = ctx.sql("SELECT tumbling_window(1000, 500) as tw, asof_nearest(100, 105, 10) as an").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn register_all_is_idempotent() {
        let ctx = SessionContext::new();
        register_all(&ctx);
        register_all(&ctx);
        let df = ctx.sql("SELECT tumbling_window(1000, 500) as tw").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }
}
