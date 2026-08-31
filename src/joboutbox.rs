use crate::job::JobStore;
use crate::kafka::KafkaProducer;
use crate::models::JobMessage;
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use rdkafka::producer::Producer;
use std::sync::Arc;
use uuid::Uuid;

pub struct JobOutbox {
    pub id: Uuid,
    pub job_id: Uuid,
    pub event_type: String,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
}

pub struct OutboxPublisher {
    jobs: Arc<JobStore>,
    producer: Arc<KafkaProducer>,
}

impl OutboxPublisher {
    pub fn new(jobs: Arc<JobStore>, producer: Arc<KafkaProducer>) -> Self {
        Self { jobs, producer }
    }

    pub async fn publish_once(&self) -> anyhow::Result<()> {
        let events = self.jobs.get_unpublished_outbox(100).await?;
        for event in events {
        tracing::debug!("event job_id: {:?}", event.job_id);
            let message = JobMessage {
                job_id: event.job_id,
            };

            match self.producer.send_job(&message).await {
                Ok(()) => {
                    self.jobs.mark_outbox_published(event.id).await?;
                    tracing::info!(outbox_id=%event.id,
                        job_id=%event.job_id,
                        "outbox event published");
                }
                Err(err) => {
                    tracing::error!(outbox_id=%event.id,
                        job_id=%event.job_id,
                        err=?err,
                        "failed to publish outbox event");
                }
            }
        }
        Ok(())
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop{
            interval.tick().await;
            self.publish_once().await?;
        }
    }
}
