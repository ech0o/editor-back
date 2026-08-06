use axum::{
    extract::State,
    routing::get,
    Json,
    Router,
};
use axum::routing::post;
use serde::Serialize;
use serde_json::json;
use crate::docker::runner::ExecResult;

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/container", post(create_container))
        .route("/exec", get(exec))
}

async fn health(
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    let version = state
        .runner
        .docker()
        .version()
        .await
        .unwrap();

    Json(json!({
        "version": version.version,
        "api": version.api_version
    }))
}

#[derive(Serialize)]
struct CreateResponse {
    id: String,
}

async fn create_container(
    State(state): State<AppState>,
) -> Json<CreateResponse> {
    let id = state
        .runner
        .create("rust:1.89")
        .await
        .unwrap();

    Json(CreateResponse { id })
}

async fn exec(State(state): State<AppState>) -> Json<ExecResult> {
    let id = state.runner.create("ubuntu:latest").await.unwrap();

    state.runner.start(&id).await.unwrap();

    let result = state.runner.exec(
        &id,
        &vec![
            "echo".into(),
            "Hello".into(),
        ],
    ).await.unwrap();

    state.runner.remove(&id).await.unwrap();

    Json(result)
}