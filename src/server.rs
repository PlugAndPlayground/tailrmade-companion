use crate::{ai_handler, bridge, frontend_handler, mcp, origin_policy, response_handler};
use axum::{
    extract::Request, middleware::Next, response::Response, routing::get, routing::post, Router,
};
use bridge::SharedBridge;
use frontend_handler::SharedFrontendPackage;
use serde::Serialize;
use std::{
    net::SocketAddr,
    sync::{OnceLock, RwLock},
    time::Instant,
};

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum Status {
    Starting,
    Running { port: u16 },
    Failed { port: u16, error: String },
}

static STATUS: OnceLock<RwLock<Status>> = OnceLock::new();

fn status_cell() -> &'static RwLock<Status> {
    STATUS.get_or_init(|| RwLock::new(Status::Starting))
}

pub fn status() -> Status {
    status_cell().read().unwrap().clone()
}

fn set_status(status: Status) {
    *status_cell().write().unwrap() = status;
}

pub fn router(frontend: SharedFrontendPackage, bridge: SharedBridge) -> Router {
    let mcp = Router::new()
        .route_service("/mcp", mcp::service(bridge.clone()))
        .route_layer(axum::middleware::from_fn_with_state(
            bridge.clone(),
            mcp::require_token,
        ));
    let tabs = Router::new()
        .route("/bridge", get(bridge::handle_upgrade))
        .with_state(bridge);

    Router::new()
        .route("/forward", post(response_handler::tm_request))
        .route("/ping", get(response_handler::ping))
        .route("/ai/request", post(ai_handler::request))
        .route("/ai/claude-stream", post(ai_handler::claude_stream))
        .fallback(get(frontend_handler::serve))
        .with_state(frontend)
        .merge(mcp)
        .merge(tabs)
        .layer(origin_policy::cors())
        .layer(axum::middleware::from_fn(origin_policy::check_origin))
        .layer(axum::middleware::from_fn(log_request))
}

/// Logs method, path and status only; query strings may carry user data.
async fn log_request(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let started = Instant::now();
    let response = next.run(request).await;
    tracing::debug!(
        %method,
        path,
        status = response.status().as_u16(),
        duration_ms = started.elapsed().as_millis() as u64,
        "HTTP request"
    );
    response
}

/// Binds to localhost and updates the shared status once the listener is ready.
pub async fn bind(port: u16) -> Result<tokio::net::TcpListener, String> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(error) => {
            let error = if error.kind() == std::io::ErrorKind::AddrInUse {
                format!("Port {port} is already in use. Is another companion running?")
            } else {
                format!("Could not listen on port {port}: {error}")
            };
            tracing::error!("{error}");
            set_status(Status::Failed {
                port,
                error: error.clone(),
            });
            return Err(error);
        }
    };
    tracing::info!("Listening on http://{addr}");
    set_status(Status::Running { port });
    Ok(listener)
}

/// Serves an already-bound listener until the process exits.
pub async fn serve(
    listener: tokio::net::TcpListener,
    port: u16,
    frontend: SharedFrontendPackage,
    bridge: SharedBridge,
) -> Result<(), String> {
    axum::serve(listener, router(frontend, bridge))
        .await
        .map_err(|error| {
            let error = error.to_string();
            set_status(Status::Failed {
                port,
                error: error.clone(),
            });
            error
        })
}

/// Binds to localhost and serves until the process exits.
/// Returns an error when the port cannot be bound (usually: already in use).
pub async fn run(
    port: u16,
    frontend: SharedFrontendPackage,
    bridge: SharedBridge,
) -> Result<(), String> {
    let listener = bind(port).await?;
    serve(listener, port, frontend, bridge).await
}
