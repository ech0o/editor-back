use crate::apierror::ApiError;
use crate::docker::exec::ExecResult;
use crate::job::Job;
use crate::metrics::Metrics;
use crate::models::{
    JobIdResponse, JobMessage, JobResponse, RunRequest, RunResponse, RunStatus, ScaleRequest,
};
use crate::state::{AppState, JobState, WorkerState};
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

pub fn router() -> Router<AppState> {
    Router::new()
        // .route("/health", get(health))
        // // .route("/container", post(create_container))
        // // .route("/exec", get(exec))
        // .route("/mount", get(workspace))
        .route("/run", post(run_handle))
        .route("/run/{job_id}", get(get_job))
}

pub fn worker_router() -> Router<Arc<WorkerState>> {
    Router::new()
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .route("/workers", get(list_workers))
        .route("/workers/scale-up", post(scale_up))
        .route("/workers/scale-down", post(scale_down))
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

async fn run_handle(
    State(state): State<AppState>,
    results: Result<Json<RunRequest>, JsonRejection>,
) -> Result<Json<JobIdResponse>, ApiError> {
    // let _permit = match state.semaphore.try_acquire(){
    //     Ok(permit) => permit,
    //     Err(_)=>{
    //         return Err(ApiError::TooManyRequests)
    //     }
    // };
    let Json(request) = results.map_err(|err| match err.status() {
        StatusCode::PAYLOAD_TOO_LARGE => {
            tracing::error!(
                error=%err,
                "Invalid JSON request"
            );
            ApiError::PayloadTooLarge
        }
        _ => {
            println!("{}", err.status());
            tracing::error!(
                error=%err,
                "Invalid JSON request"
            );
            ApiError::InvalidJson
        }
    })?;
    // tracing::debug!("received request: {:?}", request);
    // let response = state.runner.run_rust(&request.code).await?;
    // Ok(Json(response))
    let job = Job {
        id: Uuid::new_v4(),
        language: "rust".to_string(),
        code: request.code,
        status: RunStatus::Accepted,
        stdout: None,
        stderr: None,
        exit_code: None,
        created_at: None,
        worker_id: None,
        heartbeat_at: None,
    };
    let job_id = job.id;
    let job_msg = JobMessage { job_id };
    state.jobs.create(&job).await?;
    state.kafka.send_job(&job_msg).await?;
    Ok(Json(JobIdResponse { job_id }))
}

async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
) -> Result<Json<JobResponse>, ApiError> {
    let job = state.jobs.get(job_id).await?.ok_or(ApiError::JobNotFound)?;

    Ok(Json(job.into()))
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
) -> StatusCode {
    state.worker_pool.scale_up(req.count).await;
    StatusCode::NO_CONTENT
}

pub async fn scale_down(
    State(state): State<Arc<WorkerState>>,
    Json(req): Json<ScaleRequest>,
) -> StatusCode {
    state.worker_pool.scale_down(req.count).await;
    StatusCode::NO_CONTENT
}
