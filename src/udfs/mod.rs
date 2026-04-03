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
