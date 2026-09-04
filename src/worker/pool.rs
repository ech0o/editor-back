use crate::docker::DockerRunner;
use crate::job::JobStore;
use crate::kafka::KafkaConfig;
use crate::metrics::Metrics;
use crate::worker::{Worker, WorkerContext};
use rdkafka::producer::FutureProducer;
use std::collections::HashMap;
use std::fmt::format;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

pub(crate) struct WorkerPool {
    workers: Arc<Mutex<HashMap<String, WorkerStatus>>>,
    next_worker_id: usize,
    supervisors: Vec<JoinHandle<()>>,
    kafka_config: KafkaConfig,
    ctx: WorkerContext,
    shutdown: CancellationToken,
}

#[derive(Debug, Clone, Copy)]
pub enum WorkerStatus {
    Starting,
    Running,
    Restarting,
    Stopped,
}

impl WorkerStatus{
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkerStatus::Starting => "starting",
            WorkerStatus::Running => "running",
            WorkerStatus::Restarting => "restarting",
            WorkerStatus::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub worker_id: String,
}

impl WorkerPool {
    pub fn new(kafka_config: KafkaConfig, ctx: WorkerContext, shutdown: CancellationToken) -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            next_worker_id: 1,
            kafka_config,
            ctx,
            shutdown,
            supervisors: Vec::new(),
        }
    }

    async fn set_status(
        workers: &Arc<Mutex<HashMap<String, WorkerStatus>>>,
        worker_id: &str,
        status: WorkerStatus,
        metrics:&Metrics,
    ) {
        {
            let mut workers = workers.lock().await;
            workers.insert(worker_id.to_string(), status);
        }
        metrics.set_worker_status(worker_id,status);
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
        let shutdown = self.shutdown.clone();
        let handle = tokio::spawn(async move {
            Self::supervise_worker(worker_id, kafka, ctx, workers, shutdown).await;
        });
        self.supervisors.push(handle);
    }

    pub async fn start(&mut self, size: usize) {
        for _ in 0..size {
            self.start_worker().await;
        }
    }

    async fn supervise_worker(
        worker_id: String,
        kafka: KafkaConfig,
        ctx: WorkerContext,
        workers: Arc<Mutex<HashMap<String, WorkerStatus>>>,
        shutdown: CancellationToken,
    ) {
        let mut backoff = INITIAL_BACKOFF;
        let metrics = Arc::clone(&ctx.metrics);
        loop {
            Self::set_status(&workers, &worker_id, WorkerStatus::Starting,&metrics).await;
            let worker = match Worker::new(worker_id.clone(), kafka.clone(), ctx.clone()) {
                Ok(worker) => {
                    backoff = INITIAL_BACKOFF;
                    worker
                }
                Err(err) => {
                    tracing::error!(
                        worker_id=%worker_id,
                        error=%err,
                        ?backoff,
                        "failed to create worker"
                    );
                    Self::set_status(&workers, &worker_id, WorkerStatus::Restarting,&metrics).await;
                    tokio::select! {
                        _ = tokio::time::sleep(backoff)=>{}
                        _ = shutdown.cancelled() => {
                            Self::set_status(&workers, &worker_id, WorkerStatus::Stopped,&metrics).await;
                            return;
                        }
                    }
                    backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
                    continue;
                }
            };
            let start_at = tokio::time::Instant::now();
            Self::set_status(&workers, &worker_id, WorkerStatus::Running,&metrics).await;
            let worker_shutdown = worker.shutdown.clone();
            let mut handle = tokio::spawn(async move { worker.run().await });
            if start_at.elapsed() > Duration::from_secs(30){
                backoff = INITIAL_BACKOFF;
            }
            tokio::select! {
                result = &mut handle => {
                    match result {
                        Ok(Ok(_)) => {
                            tracing::warn!(
                            worker_id = %worker_id,
                            "worker exited normally"
                            );
                             metrics.worker_restarted(&worker_id);
                        }
                        Ok(Err(err)) => {
                            tracing::error!(
                                worker_id = %worker_id,
                                error = %err,
                                "worker exited with error"
                            );
                            metrics.worker_restarted(&worker_id);
                        }
                        Err(err) => {
                            tracing::error!(
                                worker_id = %worker_id,
                                error = %err,
                                "worker panicked"
                            );
                             metrics.worker_restarted(&worker_id);
                        }
                    }
                    Self::set_status(&workers, &worker_id, WorkerStatus::Restarting,&metrics).await;
                    // tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = shutdown.cancelled() => {
                            Self::set_status(&workers, &worker_id, WorkerStatus::Stopped,&metrics).await;
                            return;
                        }
                    }
                    backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
                }
                _ = shutdown.cancelled() => {
                    tracing::info!(worker_id=%worker_id,"supervisor shutting down worker");
                    worker_shutdown.cancel();
                    let _ = handle.await;
                    Self::set_status(&workers, &worker_id, WorkerStatus::Stopped,&metrics).await;
                    break;
                }
            }
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
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        for handle in self.supervisors {
            if let Err(e) = handle.await {
                tracing::error!(
                    error=%e,
                    "supervisor task failed during shutdown"
                );
            }
        }
        tracing::info!("worker pool stopped");
    }
}
