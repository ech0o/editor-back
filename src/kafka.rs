use crate::docker::DockerRunner;
use crate::job::{Job, JobStore};
use crate::models::{JobMessage, RunStatus};
use anyhow::{anyhow, bail};
use futures_util::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct KafkaProducer {
    producer: FutureProducer,
}

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    runner: DockerRunner,
    jobs: Arc<JobStore>,
}

impl KafkaProducer {
    pub fn new(broker: &str) -> anyhow::Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .create()?;
        Ok(Self { producer })
    }

    pub async fn send_job(&self, job: &JobMessage) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(job)?;
        self.producer
            .send(
                FutureRecord::to("judge.jobs")
                    .key(&job.job_id.to_string())
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| anyhow!(err))?;
        Ok(())
    }
}

impl KafkaConsumer {
    pub fn new(
        broker: &str,
        group_id: &str,
        runner: DockerRunner,
        jobs: Arc<JobStore>,
    ) -> anyhow::Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;
        Ok(Self {
            consumer,
            runner,
            jobs,
        })
    }

    pub fn subscribe(&self) -> anyhow::Result<()> {
        self.consumer.subscribe(&["judge.jobs"])?;
        Ok(())
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let mut stream = self.consumer.stream();
        while let Some(message) = stream.next().await {
            match message {
                Ok(message) => {
                    let payload = message.payload().ok_or_else(|| anyhow!("no payload"))?;
                    let job_msg: JobMessage = serde_json::from_slice(payload)?;
                    let job = self
                        .jobs
                        .get(job_msg.job_id)
                        .await?
                        .ok_or_else(|| anyhow!("job not found"))?;
                    self.jobs.update_status(job.id, RunStatus::Running).await?;
                    let result = match job.language.as_str() {
                        "rust" => self.runner.run_rust(job.id, &job.code).await,
                        _ => bail!("un supported language"),
                    };
                    match result {
                        Ok(res) => {
                            self.jobs
                                .finish(
                                    job.id,
                                    res.status,
                                    Some(res.stdout),
                                    Some(res.stderr),
                                    Some(res.exit_code),
                                )
                                .await?;
                        }
                        Err(err) => {
                            self.jobs
                                .finish(
                                    job.id,
                                    RunStatus::RuntimeError,
                                    None,
                                    Some(err.to_string()),
                                    None,
                                )
                                .await?;
                        }
                    }
                    self.consumer
                        .commit_message(&message, CommitMode::Async)
                        .map_err(|err| anyhow!("Kafka commit failed: {err}"))?;
                }
                Err(err) => {
                    tracing::debug!(error=%err,"error receiving message: {:?}", err);
                }
            }
        }
        Ok(())
    }
}
