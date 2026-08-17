use std::collections::HashMap;
use std::sync::Arc;

use crate::docker::DockerRunner;
use crate::job::{Job, JobStore};
use crate::kafka::KafkaProducer;
use crate::models::{RunResponse, RunStatus};
use bollard::Docker;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, Semaphore, mpsc};
use uuid::Uuid;
use crate::db::Database;

#[derive(Clone)]
pub struct AppState {
    // pub runner: DockerRunner,
    pub semaphore: Arc<Semaphore>,
    pub jobs: Arc<JobStore>,
    pub kafka: KafkaProducer,
    // pub db:Database
}

impl AppState {
    pub fn new(
        job_store: Arc<JobStore>,
        kafka_producer: KafkaProducer,
    ) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(4)),
            jobs: job_store,
            kafka: kafka_producer,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct JobState {
    pub status: RunStatus,
    pub result: Option<RunResponse>,
}
