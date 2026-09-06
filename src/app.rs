use crate::apierror::ErrorResponse;
use crate::db::Database;
use crate::docker::DockerRunner;
use crate::job::{Job, JobStore};
use crate::joboutbox::OutboxPublisher;
use crate::kafka::{KafkaConfig, KafkaConsumer, KafkaProducer};
use crate::metrics::Metrics;
use crate::models::{RunRequest, RunResponse, RunStatus};
use crate::reaper::Reaper;
use crate::state::{JobState, WorkerState};
use crate::worker::WorkerContext;
use crate::worker::pool::WorkerPool;
use crate::workspace::Workspace;
use crate::{routes, state::AppState};
use axum::Router;
use axum::http::{HeaderValue, Method};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use bollard::Docker;
use metrics_exporter_prometheus::PrometheusBuilder;
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;

pub async fn create_app() -> anyhow::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();

    let db_addr = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:example@localhost:5432/postgres".to_string());
    println!("db_addr: {:?}", db_addr);
    let db = PgPool::connect(db_addr.as_str()).await?;
    let kafka_addr =
        std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    println!("kafka_addr: {:?}", kafka_addr);
    ensure_topic_exists(kafka_addr.as_str(), "judge.jobs").await?;
    let kafka = KafkaProducer::new(kafka_addr.as_str())?;
    let jobs = Arc::new(JobStore::new(db));
    let metrics = Arc::new(Metrics::new()?);

    if args.get(1).map(String::as_str) == Some("worker") {
        run_worker(jobs, kafka, kafka_addr, metrics).await?;
    } else {
        run_api(jobs.clone(), kafka.clone(), metrics.clone()).await?;
    }
    Ok(())
}

async fn run_worker(
    jobs: Arc<JobStore>,
    kafka: KafkaProducer,
    kafka_addr: String,
    metrics: Arc<Metrics>,
) -> anyhow::Result<()> {
    let worker_id = hostname::get()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    tracing::info!("initializing worker,{}", kafka_addr.as_str());
    let docker = Docker::connect_with_local_defaults()?;
    let docker_runner = DockerRunner::new(Arc::new(docker));
    let shutdown = CancellationToken::new();
    let kafka_config = KafkaConfig {
        broker: kafka_addr,
        group_id: String::from("judge-worker"),
    };
    let ctx = WorkerContext {
        producer: kafka.producer.clone(),
        runner: Arc::new(docker_runner),
        metrics: metrics.clone(),
        jobs: jobs.clone(),
    };

    // let consumer = KafkaConsumer::new(
    //     worker_id.as_str(),
    //     &kafka_config,
    //     ctx,
    //     shutdown.clone(),
    // )?;
    let pool = Arc::new(WorkerPool::new(kafka_config, ctx, shutdown.clone()));
    pool.start(3).await;
    metrics.worker_started_total.inc();
    let listener = TcpListener::bind("0.0.0.0:9091").await?;
    tracing::info!("Listening on http://0.0.0.0:9091");
    // tracing::info!(
    //     worker_id = worker_id,
    //     pid = std::process::id(),
    //     "worker started"
    // );
    // let pool = Arc::new(pool);
    let worker_state = WorkerState::new(metrics.clone(), Arc::clone(&pool));
    let router = Router::new()
        .merge(routes::worker_router())
        .with_state(Arc::new(worker_state));
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let producer = Arc::new(kafka);
    let reaper = Reaper::new(jobs.clone(), producer.clone());
    tokio::spawn(async move {
        // let mut interval=tokio::time::interval(tokio::time::Duration::from_secs(10));
        //  loop{
        //      interval.tick().await;
        //      match jobs.reap_stale_job(Duration::from_secs(30)).await{
        //          Ok(ids)=>{
        //              for id in ids {
        //                  tracing::warn!(job_id = %id, "reaped stale job");
        //              }
        //          }
        //          Err(e)=>{
        //              tracing::error!(err = %e, "failed to reaped stale job");
        //          }
        //      }
        //  }
        let _ = reaper.run().await;
    });

    let outbox_publisher = OutboxPublisher::new(jobs.clone(), producer);
    tokio::spawn(async move {
        if let Err(err) = outbox_publisher.run().await {
            tracing::error!(err=%err,"outbox_publisher stopped");
        }
    });

    // let consumer_task = tokio::spawn(async move {
    //     consumer.subscribe()?;
    //     consumer.run().await
    // });
    shutdown_signal().await;
    tracing::info!("shutdown signal received");
    shutdown.cancel();
    pool.shutdown().await;
    // consumer_task.await??;
    Ok(())
}
async fn run_api(
    jobs: Arc<JobStore>,
    kafka: KafkaProducer,
    metrics: Arc<Metrics>,
) -> anyhow::Result<()> {
    let cors = CorsLayer::new()
        .allow_origin("http://localhost:3000".parse::<HeaderValue>()?)
        .allow_methods(vec![Method::GET, Method::POST, Method::PUT, Method::DELETE])
        .allow_headers(Any);
    let router = Router::new()
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        .layer(cors)
        .merge(routes::router())
        .with_state(AppState::new(jobs, kafka, metrics));
    let listener = TcpListener::bind("0.0.0.0:4000").await?;
    tracing::info!("Listening on http://0.0.0.0:4000");

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

pub async fn ensure_topic_exists(brokers: &str, topic_name: &str) -> anyhow::Result<()> {
    let admin_client: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("AdminClient failed to create admin client");
    tracing::info!("Admin client:");
    let new_topic = NewTopic::new(topic_name, 3, TopicReplication::Fixed(1));

    // 尝试创建 Topic，如果已存在会返回错误，直接忽略即可
    let res = admin_client
        .create_topics(&[new_topic], &AdminOptions::new())
        .await;

    tracing::info!("Topic '{:?}' ready", res);
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C signal handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

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

// #[tokio::test]
// async fn post_run_rejects_invalid_json() {
//     let app = create_app().await.unwrap();
//     let request = Request::builder()
//         .method(Method::POST)
//         .uri("/run")
//         .header("content-type", "application/json")
//         .body(Body::from(r#"this is not json"#))
//         .unwrap();
//     let response = app.oneshot(request).await.unwrap();
//     assert_eq!(response.status(), StatusCode::BAD_REQUEST);
//     let body = axum::body::to_bytes(response.into_body(), usize::MAX)
//         .await
//         .unwrap();
//
//     let error: ErrorResponse = serde_json::from_slice(&body).unwrap();
//
//     assert_eq!(error.error, "invalid_json");
// }
//
// #[tokio::test]
// async fn post_run_rejects_missing_code() {
//     let app = create_app().await.unwrap();
//
//     let request = Request::builder()
//         .method("POST")
//         .uri("/run")
//         .header("content-type", "application/json")
//         .body(Body::from(r#"{"foo":"bar"}"#))
//         .unwrap();
//
//     let response = app.oneshot(request).await.unwrap();
//
//     assert_eq!(response.status(), StatusCode::BAD_REQUEST);
// }
//
// #[tokio::test]
// async fn post_run_rejects_large_body() {
//     let app = create_app().await.unwrap();
//     let code = "a".repeat(2 * 1024 * 1024);
//     let body = serde_json::to_string(&RunRequest { code }).unwrap();
//     let request = Request::builder()
//         .method(Method::POST)
//         .uri("/run")
//         .header("content-type", "application/json")
//         .body(Body::from(body))
//         .unwrap();
//     let response = app.oneshot(request).await.unwrap();
//     // let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
//     // println!("{:?}", body);
//     assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
// }
