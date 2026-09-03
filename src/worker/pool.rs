use crate::docker::DockerRunner;
use crate::job::JobStore;
use crate::kafka::KafkaConfig;
use crate::metrics::Metrics;
use crate::worker::{Worker, WorkerContext};
use rdkafka::producer::FutureProducer;
use std::collections::HashMap;
use std::fmt::format;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) struct WorkerPool {
    workers: Arc<Mutex<HashMap<String, WorkerStatus>>>,
    next_worker_id: usize,

    kafka_config: KafkaConfig,
    ctx: WorkerContext,
}

#[derive(Debug, Clone, Copy)]
pub enum WorkerStatus {
    Starting,
    Running,
    Restarting,
}

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub worker_id: String,
}

impl WorkerPool {
    pub fn new(kafka_config: KafkaConfig, ctx: WorkerContext) -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            next_worker_id: 1,
            kafka_config,
            ctx,
        }
    }

    async fn set_status(
        workers: &Arc<Mutex<HashMap<String, WorkerStatus>>>,
        worker_id: &str,
        status: WorkerStatus,
    ) {
        let mut workers = workers.lock().await;
        workers.insert(worker_id.to_string(), status);
    }

    // pub fn spawn_worker(&mut self) -> anyhow::Result<()> {
    //     let worker_id = format!("worker-{}", self.next_worker_id);
    //     self.next_worker_id += 1;
    //     let worker = Worker::new(worker_id.clone())?;
    //     let workers = Arc::clone(&self.workers);
    //     tokio::spawn(async move {
    //         let mut workers = workers.lock().await;
    //         workers.push(WorkerInfo {
    //             worker_id: worker_id.clone(),
    //         });
    //         worker.run().await;
    //         tracing::warn!(worker_id=%worker_id, "Worker exited");
    //     });
    //     Ok(())
    // }

    pub async fn start_worker(&mut self) {
        let worker_id = format!("worker-{}", self.next_worker_id);
        self.next_worker_id += 1;
        let workers = Arc::clone(&self.workers);
        let kafka = self.kafka_config.clone();
        let ctx = self.ctx.clone();
        tokio::spawn(async move {
            loop {
                Self::set_status(&workers, &worker_id, WorkerStatus::Starting).await;
                let id = worker_id.clone();
                let worker = match Worker::new(id.clone(), kafka.clone(), ctx.clone()) {
                    Ok(worker) => worker,
                    Err(err) => {
                        tracing::error!(worker_id=%worker_id,error=%err,"failed to create worker");

                        Self::set_status(&workers, &worker_id, WorkerStatus::Restarting).await;

                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                        continue;
                    }
                };
                Self::set_status(&workers, &worker_id, WorkerStatus::Running).await;
                tracing::info!(worker_id=%id, "worker {} is started", id);
                let handle = tokio::spawn(async move { worker.run().await });
                match handle.await {
                    Ok(Ok(_)) => {
                        tracing::info!(worker_id=%id, "worker exited normally");
                    }
                    Ok(Err(err)) => {
                        tracing::error!(worker_id = %worker_id,
                            error = %err,
                            "worker exited with error");
                    }
                    Err(err) => {
                        tracing::error!(worker_id = %worker_id,error=%err,"worker panicked");
                    }
                }
                Self::set_status(&workers, &worker_id, WorkerStatus::Restarting).await;
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }

    pub async fn start(&mut self, size: usize) {
        for _ in 0..size {
            self.start_worker().await;
        }
    }

    // pub fn remove_finished_worker(&mut self) {
    //     let finished: Vec<String> = self
    //         .workers
    //         .iter()
    //         .filter_map(|(worker_id, handle)| {
    //             if handle.is_finished() {
    //                 Some(worker_id.clone())
    //             } else {
    //                 None
    //             }
    //         })
    //         .collect();
    //
    //     for worker_id in finished {
    //         if let Some(handle) = self.workers.remove(&worker_id) {
    //             tokio::spawn(async move {
    //                 if let Err(err) = handle.await {
    //                     tracing::error!(err=%err, "worker failed error");
    //                 }
    //             });
    //         }
    //     }
    // }
}
