use crate::apierror::ApiError;
use crate::docker::exec::ExecResult;
use crate::docker::workspace::{create_workspace, write_source};
use crate::job::Job;
use crate::models::{JobIdResponse, JobMessage, JobResponse, RunRequest, RunResponse, RunStatus};
use crate::state::{AppState, JobState};
use axum::error_handling::HandleError;
use axum::extract::Path;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router, extract::State, routing::get};
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
        .route("/metrics",get(metrics))
        .route("/run/{job_id}", get(get_job))
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

// #[derive(Debug, Deserialize,Serialize)]
// pub struct RunRequest {
//     pub code: String,
// }

// async fn create_container(
//     State(state): State<AppState>,
// ) -> Json<CreateResponse> {
//     let id = state
//         .runner
//         .create("rust:1.89")
//         .await
//         .unwrap();
//
//     Json(CreateResponse { id })
// }

// async fn exec(State(state): State<AppState>) -> Json<ExecResult> {
//     let id = state.runner.create("ubuntu:latest").await.unwrap();
//
//     state.runner.start(&id).await.unwrap();
//
//     let result = state.runner.exec(
//         &id,
//         &vec![
//             "echo".into(),
//             "Hello".into(),
//         ],
//     ).await.unwrap();
//
//     state.runner.remove(&id).await.unwrap();
//
//     Json(result)
// }

// async fn workspace(State(state): State<AppState>) -> Json<ExecResult> {
//     let workspace = create_workspace().unwrap();
//     let _ = write_source(&workspace, "hello.txt", "Hello Docker!");
//     let id = state
//         .runner
//         .create("ubuntu:latest", &workspace)
//         .await
//         .unwrap();
//     let _ = state.runner.start(&id).await;
//     let res = state
//         .runner
//         .exec(&id, vec!["cat".into(), "/workspace/hello.txt".into()])
//         .await;
//     Json(res.unwrap())
// }

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

async fn metrics()->impl axum::response::IntoResponse {
    ([(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
     crate::metrics::gather()
    )
}
