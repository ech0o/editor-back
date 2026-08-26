use crate::docker::DockerRunner;
use crate::error::{ProcessError, RunError};
use crate::job::{Job, JobStore};
use crate::metrics::{Metrics, RunningGuard, job_completed, job_duration, job_failed};
use crate::models::{DlqMessage, JobMessage, RunStatus};
use anyhow::{anyhow, bail};
use chrono::Utc;
use futures_util::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone)]
pub struct KafkaProducer {
    pub producer: FutureProducer,
}

pub struct KafkaConsumer {
    worker_id: String,
    consumer: StreamConsumer,
    producer: FutureProducer,
    runner: DockerRunner,
    jobs: Arc<JobStore>,
    metrics: Arc<Metrics>,
    shutdown: CancellationToken,
}

impl KafkaProducer {
    pub fn new(broker: &str) -> anyhow::Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .create()?;
        tracing::info!("Kafka producer created");
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
        worker_id: &str,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
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
            worker_id: String::from(worker_id),
            metrics,
            shutdown,
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
        tracing::info!(
            worker_id = self.worker_id,
            partition = message.partition(),
            offset = message.offset(),
            "received job"
        );
        let payload = message
            .payload()
            .ok_or_else(|| ProcessError::Permanent(anyhow!("no payload")))?;
        let job_msg: JobMessage = serde_json::from_slice(payload)
            .map_err(|err| ProcessError::Permanent(anyhow!("invalid job message: {err}")))?;
        let job = self
            .jobs
            .get_and_queued(job_msg.job_id)
            .await
            .map_err(ProcessError::Retryable)?
            .ok_or_else(|| ProcessError::Permanent(anyhow!("job {} not found", job_msg.job_id)))?;
        let queue_latency = (Utc::now() - job.created_at.unwrap())
            .to_std()
            .expect("metric time to_std error");
        self.metrics
            .job_queue_latency_seconds
            .observe(queue_latency.as_secs_f64());
        let lock_token = self
            .jobs
            .try_start(job.id)
            .await
            .map_err(ProcessError::Retryable)?;
        let Some(lock_token) = lock_token else {
            return Ok(());
        };
        // let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let cancel = tokio_util::sync::CancellationToken::new();
        let jobs = self.jobs.clone();
        let job_id = job.id;
        let heartbeat_cancel = cancel.clone();
        let heartbeat_err = Arc::new(Mutex::new(None));
        let heartbeat_error = Arc::clone(&heartbeat_err);
        let heartbeat = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        match jobs.heartbeat(job_id, lock_token).await {
                            Ok(true) => {
                                tracing::info!(job_id=%job_id,"job lease renewed");
                            }
                            Ok(false) => {
                                tracing::warn!(job_id=%job_id,"job lease lost");
                                // let _ = shutdown_tx.send(true);
                                heartbeat_cancel.cancel();
                                break;
                            }
                            Err(err) => {
                                tracing::error!(job_id=%job_id,"job heartbeat database error: {}", err);
                                *heartbeat_error.lock().await = Some(err.to_string());
                                heartbeat_cancel.cancel();
                                break;
                            }
                        }
                    }
                    _ = heartbeat_cancel.cancelled() => {
                        break;
                    }
                }
            }
        });
        tracing::info!(job=%job.id,"job started");
        self.jobs
            .update_status(job.id, RunStatus::Running)
            .await
            .map_err(ProcessError::Retryable)?;
        let _start = self
            .metrics
            .job_duration_seconds
            .with_label_values(&[&self.worker_id])
            .start_timer();
        let result = match job.language.as_str() {
            "rust" => {
                let _running = RunningGuard::new(self.metrics.jobs_running.clone());
                self.runner.run_rust(job.id, &job.code, cancel).await
            }
            _ => {
                heartbeat.abort();
                self.jobs
                    .finish(
                        job.id,
                        RunStatus::RuntimeError,
                        None,
                        Some(format!("unsupported lang:{}", job.language)),
                        None,
                        lock_token,
                    )
                    .await
                    .map_err(ProcessError::Retryable)?;
                return Err(ProcessError::Permanent(anyhow!(
                    "unsupported lang:{}",
                    job.language
                )));
            }
        };
        tracing::info!(result=?result,"run code result");
        let heartbeat_err = heartbeat_err.lock().await.take();
        if let Some(err) = heartbeat_err {
            return Err(ProcessError::Retryable(anyhow!(
                "heartbeat database error: {}",
                err
            )));
        }
        // start.observe_duration();
        heartbeat.abort();
        // let Some(result) = result else {
        //     tracing::warn!(job_id=%job.id,"job execution aborted because lease was lost");
        //     return Err(ProcessError::Retryable(anyhow!("job lease lost")));
        // };
        match result {
            Ok(res) => {
                self.metrics
                    .jobs_finished_total
                    .with_label_values(&[&self.worker_id])
                    .inc();
                match res.status {
                    RunStatus::Accepted => {
                        self.metrics
                            .jobs_total
                            .with_label_values(&["accepted"])
                            .inc();
                    }
                    RunStatus::CompileError => {
                        self.metrics
                            .jobs_total
                            .with_label_values(&["CompileError"])
                            .inc();
                    }
                    RunStatus::RuntimeError => {
                        self.metrics
                            .jobs_total
                            .with_label_values(&["RuntimeError"])
                            .inc();
                    }
                    RunStatus::TimeLimitExceeded => {
                        self.metrics
                            .jobs_total
                            .with_label_values(&["TimeLimitExceeded"])
                            .inc();
                    }
                    RunStatus::MemoryLimitExceeded => {
                        self.metrics
                            .jobs_total
                            .with_label_values(&["MemoryLimitExceeded"])
                            .inc();
                    }
                    RunStatus::Queued => {
                        self.metrics.jobs_total.with_label_values(&["Queued"]).inc();
                    }
                    RunStatus::Running => {
                        self.metrics
                            .jobs_total
                            .with_label_values(&["Running"])
                            .inc();
                    }
                    RunStatus::Success => {
                        self.metrics
                            .jobs_total
                            .with_label_values(&["Success"])
                            .inc();
                    }
                }
                self.jobs
                    .finish(
                        job.id,
                        res.status,
                        Some(res.stdout),
                        Some(res.stderr),
                        Some(res.exit_code),
                        lock_token,
                    )
                    .await
                    .map_err(ProcessError::Retryable)?;
            }
            Err(RunError::Cancelled) => {
                tracing::warn!(
                    job_id = %job.id,
                    "job execution cancelled because lease was lost"
                );
                return Err(ProcessError::Retryable(anyhow!("job lease lost")));
            }
            Err(RunError::Other(err)) => {
                self.metrics.jobs_failed_total.inc();
                self.jobs
                    .finish(
                        job.id,
                        RunStatus::RuntimeError,
                        None,
                        Some(err.to_string()),
                        None,
                        lock_token,
                    )
                    .await
                    .map_err(ProcessError::Retryable)?;
            }
        }
        Ok(())
    }
    pub async fn run(&self) -> anyhow::Result<()> {
        let mut stream = self.consumer.stream();
        loop {
            tokio::select! {
                biased;
                 _ = self.shutdown.cancelled() => {
                    // result?;
                    tracing::info!("shutdown signal received");
                    break;
                }
                message = stream.next() => {
                    let Some(message) = message else{
                        break;
                    };
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

            }
        }
        tracing::info!("consumer stopped");

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
