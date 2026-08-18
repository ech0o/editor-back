use crate::docker::DockerRunner;
use crate::error::ProcessError;
use crate::job::{Job, JobStore};
use crate::models::{DlqMessage, JobMessage, RunStatus};
use anyhow::{anyhow, bail};
use futures_util::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct KafkaProducer {
    pub producer: FutureProducer,
}

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    producer: FutureProducer,
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
        producer: FutureProducer,
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
            producer,
        })
    }

    pub fn subscribe(&self) -> anyhow::Result<()> {
        self.consumer.subscribe(&["judge.jobs"])?;
        Ok(())
    }

    async fn handle_message(
        &self,
        message: &BorrowedMessage<'_>,
    ) -> anyhow::Result<(), ProcessError> {
        let payload = message
            .payload()
            .ok_or_else(|| ProcessError::Permanent(anyhow!("no payload")))?;
        let job_msg: JobMessage = serde_json::from_slice(payload)
            .map_err(|err| ProcessError::Permanent(anyhow!("invalid job message: {err}")))?;
        let job = self
            .jobs
            .get(job_msg.job_id)
            .await
            .map_err(ProcessError::Retryable)?
            .ok_or_else(|| ProcessError::Permanent(anyhow!("job {} not found", job_msg.job_id)))?;
        let started = self
            .jobs
            .try_start(job.id)
            .await
            .map_err(ProcessError::Retryable)?;
        if !started {
            tracing::error!("job already started");
            return Ok(());
        }
        tracing::info!(job=%job.id,"job started");
        self.jobs
            .update_status(job.id, RunStatus::Running)
            .await
            .map_err(ProcessError::Retryable)?;
        let result = match job.language.as_str() {
            "rust" => self.runner.run_rust(job.id, &job.code).await,
            _ => {
                self.jobs
                    .finish(
                        job.id,
                        RunStatus::RuntimeError,
                        None,
                        Some(format!("unsupported lang:{}", job.language)),
                        None,
                    )
                    .await
                    .map_err(ProcessError::Retryable)?;
                return Err(ProcessError::Permanent(anyhow!(
                    "unsupported lang:{}",
                    job.language
                )));
            }
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
                    .await
                    .map_err(ProcessError::Retryable)?;
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
                    .await
                    .map_err(ProcessError::Retryable)?;
            }
        }
        Ok(())
    }
    pub async fn run(&self) -> anyhow::Result<()> {
        let mut stream = self.consumer.stream();
        while let Some(message) = stream.next().await {
            match message {
                Ok(message) => match self.handle_message(&message).await {
                    Ok(()) => {
                        self.consumer
                            .commit_message(&message, CommitMode::Async)
                            .map_err(|err| anyhow!("Kafka commit failed: {err}"))?;
                    }
                    Err(err @ ProcessError::Permanent(_)) => {
                        tracing::error!(
                            error = %err,
                            topic = message.topic(),
                            partition = message.partition(),
                            offset = message.offset(),
                            "permanent Kafka message error");
                        self.send_to_dlq(&message,&err).await?;
                        self.consumer
                            .commit_message(&message, CommitMode::Async)
                            .map_err(|err| anyhow!("Kafka commit failed: {err}"))?;
                    }
                    Err(ProcessError::Retryable(err)) => {
                        tracing::error!(
                            error = %err,
                            "retryable Kafka message error"
                        );
                    }
                },
                Err(err) => {
                    tracing::debug!(error=%err,"error receiving message: {:?}", err);
                }
            }
        }
        Ok(())
    }

    async fn send_to_dlq(
        &self,
        message: &BorrowedMessage<'_>,
        error: &ProcessError,
    ) -> anyhow::Result<()> {
        let payload = message.payload().unwrap_or_default();
        let dlq_message = DlqMessage {
            original_payload: payload.to_vec(),
            error: error.to_string(),
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: message.offset(),
        };
        let payload = serde_json::to_vec(&dlq_message)?;
        self.producer
            .send(
                FutureRecord::to("jobs.dlq")
                    .payload(&payload)
                    .key(&message.offset().to_string()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| anyhow!("failed to send message to DLQ: {err}"))?;
        Ok(())
    }
}
