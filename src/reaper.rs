use crate::job::JobStore;
use std::sync::Arc;
use std::time::Duration;
use futures_util::SinkExt;
use crate::kafka::KafkaProducer;
use crate::models::JobMessage;

pub struct Reaper {
    jobs: Arc<JobStore>,
    producer: Arc<KafkaProducer>,
}

impl Reaper {
    pub fn new(jobs: Arc<JobStore>,producer:Arc<KafkaProducer>) -> Reaper {
        Self { jobs, producer }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            let jobs = self.jobs.reap_stale_job(Duration::from_secs(30)).await?;
            for job_id in jobs {
                tracing::warn!("Reaper running job {}", job_id);
                // let job = self.jobs.get(job_id).await?.ok_or(tracing::error!("job {} not found", job_id));
                // let message = JobMessage{job_id};
                // self.producer.send_job(&message).await?;
            }
        }

    }
}
