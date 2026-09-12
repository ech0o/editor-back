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
use crate::metrics::Metrics;
use crate::worker::pool::WorkerPool;


#[derive(Clone, Debug, Serialize)]
pub struct JobState {
    pub status: RunStatus,
    pub result: Option<RunResponse>,
}


#[derive(Clone)]
pub struct WorkerState {
    pub worker_pool: Arc<WorkerPool>,
    pub metrics:Arc<Metrics>,
}

impl WorkerState {
    pub fn new(metrics: Arc<Metrics>,worker_pool:Arc<WorkerPool>)->Self{
        Self{
            metrics,
            worker_pool,
        }
    }
}