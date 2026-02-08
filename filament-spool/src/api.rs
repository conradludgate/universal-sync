//! HTTP API and health check endpoints for the acceptor server.

use axum::Router;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use iroh::Endpoint;

use crate::metrics::SharedMetrics;
use crate::state_store::SharedFjallStateStore;

#[derive(Clone)]
pub struct ApiState {
    pub metrics: SharedMetrics,
    pub endpoint: Endpoint,
    pub state_store: SharedFjallStateStore,
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/api/v1/endpoint-addr", get(endpoint_addr))
        .route("/api/v1/weavers", get(list_weavers))
        .route("/api/v1/weavers/{weaver_id}/storage", get(weaver_storage))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn metrics(State(state): State<ApiState>) -> Response {
    let body = state.metrics.encode().await;
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

async fn endpoint_addr(State(state): State<ApiState>) -> impl IntoResponse {
    let addr = state.endpoint.addr();
    let addr_bytes = postcard::to_allocvec(&addr).expect("serialization should not fail");
    let addr_str = bs58::encode(addr_bytes).into_string();
    axum::Json(serde_json::json!({ "addr": addr_str }))
}

async fn list_weavers(State(state): State<ApiState>) -> impl IntoResponse {
    let groups = state.state_store.list_groups();
    let ids: Vec<String> = groups
        .iter()
        .map(|g| bs58::encode(g.as_bytes()).into_string())
        .collect();
    axum::Json(ids)
}

async fn weaver_storage(State(state): State<ApiState>, Path(weaver_id): Path<String>) -> Response {
    let Ok(bytes) = bs58::decode(&weaver_id).into_vec() else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "invalid weaver id: not valid base58",
        )
            .into_response();
    };

    if bytes.len() != 32 {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "invalid weaver id: must be 32 bytes",
        )
            .into_response();
    }

    let mut group_bytes = [0u8; 32];
    group_bytes.copy_from_slice(&bytes);
    let group_id = filament_core::GroupId::new(group_bytes);

    let sizes = state.state_store.group_storage_sizes(&group_id);
    axum::Json(sizes).into_response()
}
