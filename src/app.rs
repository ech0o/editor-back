use crate::apierror::ErrorResponse;
use crate::db::Database;
use crate::docker::DockerRunner;
use crate::job::{Job, JobStore};
use crate::kafka::{KafkaConsumer, KafkaProducer};
use crate::models::{RunRequest, RunResponse, RunStatus};
use crate::state::JobState;
use crate::{routes, state::AppState};
use axum::Router;
use axum::http::{HeaderValue, Method};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use bollard::Docker;
use std::collections::HashMap;
use std::sync::Arc;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::RwLock;
use tower::ServiceExt;
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;

pub async fn create_app() -> anyhow::Result<Router> {
    let docker = Docker::connect_with_local_defaults()?;
    let runner = DockerRunner::new(Arc::new(docker));
    let db = PgPool::connect("postgres://postgres:example@localhost:5432/postgres").await?;
    let kafka = KafkaProducer::new("localhost:9092")?;
    let jobs = Arc::new(JobStore::new(db));
    let cors = CorsLayer::new()
        .allow_origin("http://localhost:3000".parse::<HeaderValue>()?)
        .allow_methods(vec![Method::GET, Method::POST, Method::PUT, Method::DELETE])
        .allow_headers(Any);
    let consumer = KafkaConsumer::new(
        "localhost:9092",
        "judge-worker",
        runner,
        jobs.clone(),
        kafka.producer.clone(),
    )?;
    consumer.subscribe()?;
    tokio::spawn(async move {
        if let Err(error) = consumer.run().await {
            eprintln!("Error consuming Kafka messages: {}", error);
        }
    });
    Ok(Router::new()
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        .layer(cors)
        .merge(routes::router())
        .with_state(AppState::new(jobs, kafka)))
}

// async fn worker(
//     mut rx: tokio::sync::mpsc::Receiver<Job>,
//     runner: DockerRunner,
//     jobs: Arc<JobStore>,
// ) {
//     while let Some(job) = rx.recv().await {
//         jobs.set_running(job.id).await;
//         println!("received job {:?}", job.id);
//         let result = runner.run_rust(job.id.clone(), &job.code).await;
//         match result {
//             Ok(run_res) => {
//                 jobs.finish(job.id, RunStatus::Success, Some(run_res)).await;
//             }
//             Err(_) => {
//                 jobs.finish(job.id, RunStatus::RuntimeError, None).await;
//             }
//         };
//         // println!("job {} finished with result {:?}", job.id, result);
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{RunResponse, RunStatus};
    use axum::http::Method;
    //
    // #[tokio::test]
    // async fn post_run_returns_accepted() {
    //     let app = create_app().await.unwrap();
    //     let request = Request::builder()
    //         .method(Method::POST)
    //         .uri("/run")
    //         .header("content-type", "application/json")
    //         .body(Body::from(
    //             r#"{
    //                 {"code":"// Type your code here\nfn main(){\n    println!(\"hello\");\n}"}
    //             }"#,
    //         ))
    //         .unwrap();
    //     let response = app.oneshot(request).await.unwrap();
    //     assert_eq!(response.status(), StatusCode::OK);
    //
    //     let body = axum::body::to_bytes(response.into_body(), usize::MAX)
    //         .await
    //         .unwrap();
    //     let res: RunResponse = serde_json::from_slice(&body).expect("invalid RunResponse");
    //     assert!(matches!(res.status, RunStatus::Accepted));
    //     assert_eq!(res.exit_code, 0);
    //     assert!(res.stdout.contains("hello"));
    // }
}

// #[tokio::test]
// async fn post_run_compile_error() {
//     let app = create_app().await.unwrap();
//     let request = Request::builder()
//         .method(Method::POST)
//         .uri("/run")
//         .header("content-type", "application/json")
//         .body(Body::from(
//             r#"{
//                 "code": "fn main() { let x: i32 = \"hello\"; }"
//             }"#,
//         ))
//         .unwrap();
//
//     let response = app.oneshot(request).await.unwrap();
//     assert_eq!(response.status(), StatusCode::OK);
//     let body = axum::body::to_bytes(response.into_body(), usize::MAX)
//         .await
//         .unwrap();
//     let res: RunResponse = serde_json::from_slice(&body).expect("invalid RunResponse");
//     assert!(matches!(res.status, RunStatus::CompileError));
//     assert_ne!(res.exit_code, 0);
// }

#[tokio::test]
async fn post_run_rejects_invalid_json() {
    let app = create_app().await.unwrap();
    let request = Request::builder()
        .method(Method::POST)
        .uri("/run")
        .header("content-type", "application/json")
        .body(Body::from(r#"this is not json"#))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    let error: ErrorResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(error.error, "invalid_json");
}

#[tokio::test]
async fn post_run_rejects_missing_code() {
    let app = create_app().await.unwrap();

    let request = Request::builder()
        .method("POST")
        .uri("/run")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"foo":"bar"}"#))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn post_run_rejects_large_body() {
    let app = create_app().await.unwrap();
    let code = "a".repeat(2 * 1024 * 1024);
    let body = serde_json::to_string(&RunRequest { code }).unwrap();
    let request = Request::builder()
        .method(Method::POST)
        .uri("/run")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    // let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    // println!("{:?}", body);
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
