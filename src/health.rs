//! Minimal HTTP health endpoint for K8s liveness/readiness probes.

use std::net::SocketAddr;

use http_body_util::Full;
use hyper::{Request, Response, body::Bytes, service::service_fn};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

async fn handle(_req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    Ok(Response::new(Full::new(Bytes::from(r#"{"status":"healthy"}"#))))
}

/// Serve the health endpoint on the given port. Runs forever.
pub async fn serve(port: u16) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Health endpoint failed to bind to {addr}: {e}");
            return;
        }
    };
    tracing::info!("Health endpoint listening on {addr}");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!("Health accept error: {e}");
                continue;
            }
        };
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service_fn(handle))
                .await
            {
                tracing::debug!("Health connection error: {e}");
            }
        });
    }
}
