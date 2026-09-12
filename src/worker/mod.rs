use crate::docker::DockerRunner;
use crate::job::JobStore;
use crate::kafka::{KafkaConfig, KafkaConsumer, KafkaProducer};
use crate::metrics::Metrics;
use crate::worker::workspace::WorkerWorkspace;
use anyhow::Error;
use rdkafka::producer::FutureProducer;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub mod pool;
pub mod workspace;

pub struct Worker {
    pub worker_id: String,
    pub workspace: Arc<WorkerWorkspace>,
    pub consumer: KafkaConsumer,
    pub shutdown: CancellationToken,
    crash_after: Option<Duration>,
}

#[derive(Clone)]
pub struct WorkerContext {
    pub producer: FutureProducer,
    pub runner: Arc<DockerRunner>,
    pub metrics: Arc<Metrics>,
    pub jobs: Arc<JobStore>,
}
impl Worker {
    pub fn new(
        worker_id: String,
        kafka_config: KafkaConfig,
        ctx: WorkerContext,
    ) -> anyhow::Result<Self> {
        let workspace = Arc::new(WorkerWorkspace::new(worker_id.as_str())?);

        let shutdown = CancellationToken::new();

        let consumer = KafkaConsumer::new(
            &worker_id,
            &kafka_config,
            ctx,
            shutdown.clone(),
            Arc::clone(&workspace),
        )?;

        let crash_after = std::env::var("WORKER_CRASH_AFTER")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs);
        Ok(Self {
            worker_id,
            workspace,
            consumer,
            shutdown,
            crash_after
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!(
            worker_id = %self.worker_id,
            pid = std::process::id(),
            "worker started"
        );
        // if let Some(duration) = self.crash_after{
        //     tokio::time::sleep(duration).await;
        //     tracing::error!(
        //         worker_id = %self.worker_id,
        //         ?duration,
        //         "injecting worker failure"
        //     );
        //     return Err(anyhow::anyhow!("injecting worker failure"))
        // }
        self.consumer.subscribe()?;
        self.consumer.run().await?;

        Ok(())
    }

    pub fn shutdown(&self) -> &CancellationToken {
        &self.shutdown
    }
}
