use crate::apierror::ApiError;
use crate::docker::exec::ExecResult;
use crate::job::Job;
use crate::metrics::Metrics;
use crate::models::{JobIdResponse, JobMessage, JobResponse, RunRequest, RunResponse, RunStatus, ScaleRequest, ScaleResponse};
use crate::state::{WorkerState};
use crate::worker::pool::WorkerInfo;
use axum::error_handling::HandleError;
use axum::extract::Path;
use axum::extract::rejection::JsonRejection;
use axum::http::{StatusCode, header};
use axum::routing::post;
use axum::{Json, Router, extract::State, routing::get};
use chrono::Utc;
use prometheus::Encoder;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;


pub fn worker_router() -> Router<Arc<WorkerState>> {
    Router::new()
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .route("/workers", get(list_workers))
        .route("/workers/scale-up", post(scale_up))
        .route("/workers/scale-down", post(scale_down))
        .route("/workers/{worker_id}/restart", post(restart_worker))
}

async fn handle_err(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
// async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
//     let version = state.runner.docker().version().await.unwrap();
//
//     Json(json!({
//         "version": version.version,
//         "api": version.api_version
//     }))
// }
#[derive(Serialize)]
struct CreateResponse {
    id: String,
}



async fn metrics(State(state): State<Arc<WorkerState>>) -> impl axum::response::IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = state.metrics.registry.gather();

    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();
    let content_type = encoder.format_type().to_string();
    ([(header::CONTENT_TYPE, content_type)], buffer)
}

async fn health() -> impl axum::response::IntoResponse {
    StatusCode::OK
}

pub async fn list_workers(State(state): State<Arc<WorkerState>>) -> Json<Vec<WorkerInfo>> {
    Json(state.worker_pool.list_workers().await)
}

pub async fn scale_up(
    State(state): State<Arc<WorkerState>>,
    Json(req): Json<ScaleRequest>,
) -> Result<Json<ScaleResponse>,ApiError> {
    state.worker_pool.set_size(req.count).await;
    Ok(Json(ScaleResponse{
        msg: String::from("scale up"),
    }))
}

pub async fn scale_down(
    State(state): State<Arc<WorkerState>>,
    Json(req): Json<ScaleRequest>,
) -> Result<Json<ScaleResponse>,ApiError> {
    state.worker_pool.set_size(req.count).await;
    Ok(Json(ScaleResponse{
        msg: String::from("scale down"),
    }))
}

pub async fn restart_worker(
    State(state): State<Arc<WorkerState>>,
    Path(worker_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .worker_pool
        .restart_worker(&worker_id)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}
