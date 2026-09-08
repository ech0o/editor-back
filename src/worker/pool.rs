use crate::docker::DockerRunner;
use crate::job::JobStore;
use crate::kafka::KafkaConfig;
use crate::metrics::Metrics;
use crate::worker::{Worker, WorkerContext};
use rdkafka::producer::FutureProducer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::format;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

pub struct WorkerPool {
    workers: Arc<Mutex<HashMap<String, WorkerStatus>>>,
    next_worker_id: AtomicUsize,
    desired_size: AtomicUsize,
    supervisors: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    controls: Arc<Mutex<HashMap<String, WorkerControl>>>,
    kafka_config: KafkaConfig,
    ctx: WorkerContext,
    reconciler: Mutex<Option<JoinHandle<()>>>,
    shutdown: CancellationToken,
    // scale_lock: Mutex<()>,
    metrics: Arc<Metrics>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerStatus {
    Starting,
    Running,
    Restarting,
    Stopping,
    Stopped,
}

impl WorkerStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkerStatus::Starting => "starting",
            WorkerStatus::Running => "running",
            WorkerStatus::Restarting => "restarting",
            WorkerStatus::Stopping => "stopping",
            WorkerStatus::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerInfo {
    pub worker_id: String,
    pub status: WorkerStatus,
}

#[derive(Clone)]
pub struct WorkerControl {
    pub shutdown: CancellationToken,
}

impl WorkerPool {
    pub fn new(
        kafka_config: KafkaConfig,
        ctx: WorkerContext,
        shutdown: CancellationToken,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            next_worker_id: AtomicUsize::new(1),
            desired_size: AtomicUsize::new(0),
            kafka_config,
            ctx,
            shutdown,
            reconciler: Mutex::new(None),
            controls: Arc::new(Mutex::new(HashMap::new())),
            supervisors: Arc::new(Mutex::new(HashMap::new())),
            // scale_lock: Mutex::new(()),
            metrics,
        }
    }

    async fn set_status(
        workers: &Arc<Mutex<HashMap<String, WorkerStatus>>>,
        worker_id: &str,
        status: WorkerStatus,
        metrics: &Metrics,
    ) {
        {
            let mut workers = workers.lock().await;
            workers.insert(worker_id.to_string(), status);
        }
        metrics.set_worker_status(worker_id, status);
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

    pub async fn start_worker(&self) ->anyhow::Result<()> {
        let id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);

        let worker_id = format!("worker-{id}");

        self.spawn_supervisor(worker_id).await;
        Ok(())
    }

    pub async fn spawn_supervisor(&self, worker_id: String) {
        let workers = Arc::clone(&self.workers);
        let kafka = self.kafka_config.clone();
        let ctx = self.ctx.clone();
        let shutdown = self.shutdown.clone();
        let controls = Arc::clone(&self.controls);
        let worker_id_clone = worker_id.clone();
        let supervisor_worker_id = worker_id.clone();
        let supervisors = Arc::clone(&self.supervisors);
        let handle = tokio::spawn(async move {
            Self::supervise_worker(worker_id_clone, kafka, ctx, workers, shutdown, controls).await;
            let mut supervisor = supervisors.lock().await;
            supervisor.remove(&supervisor_worker_id);
        });
        self.supervisors.lock().await.insert(worker_id, handle);
    }

    pub async fn start(&self, size: usize) {
        self.set_size(size).await;
    }

    async fn supervise_worker(
        worker_id: String,
        kafka: KafkaConfig,
        ctx: WorkerContext,
        workers: Arc<Mutex<HashMap<String, WorkerStatus>>>,
        shutdown: CancellationToken,
        controls: Arc<Mutex<HashMap<String, WorkerControl>>>,
    ) {
        let mut backoff = INITIAL_BACKOFF;
        let metrics = Arc::clone(&ctx.metrics);
        loop {
            Self::set_status(&workers, &worker_id, WorkerStatus::Starting, &metrics).await;
            let worker = match Worker::new(worker_id.clone(), kafka.clone(), ctx.clone()) {
                Ok(worker) => worker,
                Err(err) => {
                    tracing::error!(
                        worker_id=%worker_id,
                        error=%err,
                        ?backoff,
                        "failed to create worker"
                    );
                    Self::set_status(&workers, &worker_id, WorkerStatus::Restarting, &metrics)
                        .await;
                    if !Self::wait_or_shutdown(backoff, &shutdown).await {
                        Self::set_status(&workers, &worker_id, WorkerStatus::Stopped, &metrics)
                            .await;
                        return;
                    }
                    backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
                    continue;
                }
            };
            let worker_shutdown = worker.shutdown.clone();

            {
                let mut controls = controls.lock().await;
                controls.insert(
                    worker_id.clone(),
                    WorkerControl {
                        shutdown: worker_shutdown.clone(),
                    },
                );
            }

            Self::set_status(&workers, &worker_id, WorkerStatus::Running, &metrics).await;
            let worker_shutdown = worker.shutdown.clone();
            let start_at = tokio::time::Instant::now();
            let mut handle = tokio::spawn(async move { worker.run().await });

            tokio::select! {
                result = &mut handle => {
                    let uptime = start_at.elapsed();
                    if worker_shutdown.is_cancelled(){
                        {
                            let mut controls = controls.lock().await;
                            controls.remove(&worker_id);
                        }
                        Self::set_status(&workers,&worker_id,WorkerStatus::Stopped,&metrics).await;
                        return;
                    }
                    match result {
                        Ok(Ok(_)) => {
                            tracing::info!(
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
                   if !Self::wait_or_shutdown(backoff,&shutdown).await{
                        {
                            let mut controls = controls.lock().await;
                            controls.remove(&worker_id);
                        }
                        Self::set_status(&workers, &worker_id, WorkerStatus::Stopped, &metrics).await;
                        return;
                    }
                    if uptime >= Duration::from_secs(30) {
                        backoff = INITIAL_BACKOFF;
                    } else {
                        backoff = std::cmp::min(backoff * 2, MAX_BACKOFF);
                    }
                }
                _ = shutdown.cancelled() => {
                     // {
                     //        let mut controls = controls.lock().await;
                     //        controls.remove(&worker_id);
                     // }
                    tracing::info!(worker_id=%worker_id,"supervisor shutting down worker");
                    worker_shutdown.cancel();
                    let _ = handle.await;
                    Self::set_status(&workers, &worker_id, WorkerStatus::Stopped,&metrics).await;

                    break;
                }
            }
        }
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        if let Some(handle) = self.reconciler.lock().await.take() {
            if let Err(e) = handle.await {
                tracing::error!(
                    error=%e,
                    "reconciler failed during shutdown"
                )
            }
        }
        let handles = {
            let mut supervisors = self.supervisors.lock().await;
            supervisors
                .drain()
                .map(|(_, handle)| handle)
                .collect::<Vec<_>>()
        };
        for handle in handles {
            if let Err(e) = handle.await {
                tracing::error!(
                    error=%e,
                    "supervisor task failed during shutdown"
                );
            }
        }
        tracing::info!("worker pool stopped");
    }

    pub async fn list_workers(&self) -> Vec<WorkerInfo> {
        let workers = self.workers.lock().await;
        workers
            .iter()
            .map(|(worker_id, status)| WorkerInfo {
                worker_id: worker_id.clone(),
                status: *status,
            })
            .collect()
    }

    async fn wait_or_shutdown(duration: Duration, shutdown: &CancellationToken) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(duration) => true,
            _ = shutdown.cancelled() => false,
        }
    }

    pub async fn stop_worker(&self, worker_id: &str) {
        let control = {
            let controls = self.controls.lock().await;
            controls.get(worker_id).cloned()
        };
         let Some(control) = control  else {
            tracing::warn!(worker_id=%worker_id,"worker control not found");
            return;
        };
        Self::set_status(&self.workers,worker_id,WorkerStatus::Stopping,&self.metrics).await;
        control.shutdown.cancel();
        let handle = {
            let mut supervisors = self.supervisors.lock().await;
            supervisors.remove(worker_id)
        };
        if let Some(handle) = handle {
            if let Err(e) = handle.await {
                tracing::error!(
                    worker_id=%worker_id,
                    error=%e,
                    "worker supervisor failed while stopping"
                )
            }
        }
        // control.shutdown.cancel();
    }

    pub async fn set_size(&self, target: usize) {
        // let _guard = self.scale_lock.lock().await;
        self.desired_size.store(target, Ordering::Relaxed);
    }

    pub async fn managed_worker_count(&self) -> usize {
        let workers = self.workers.lock().await;
        workers.len()
    }

    pub async fn restart_worker(&self, worker_id: &str) -> anyhow::Result<()> {
        let exists = {
            let workers = self.workers.lock().await;
            workers.contains_key(worker_id)
        };
        if !exists {
            anyhow::bail!("worker {} not found", worker_id);
        }
        tracing::info!(
            worker_id = %worker_id,
            "restarting worker"
        );
        Self::set_status(
            &self.workers,
            worker_id,
            WorkerStatus::Restarting,
            &self.ctx.metrics,
        )
        .await;
        self.stop_worker(worker_id).await;

        if self.shutdown.is_cancelled() {
            return Ok(());
        }
        self.spawn_supervisor(worker_id.to_string()).await;
        Ok(())
    }

    pub async fn reconcile(&self) {
        let desired = self.desired_size.load(Ordering::Relaxed);

        let actual = {
            let supervisors = self.supervisors.lock().await;
            supervisors.len()
        };
        self.metrics.worker_pool_desired.set(desired as i64);
        self.metrics.worker_pool_current.set(actual as i64);
        if actual < desired {
            let count = desired - actual;
            for _ in 0..count {
                if let Err(e)=self.start_worker().await{
                    tracing::error!(
                        error=%e,
                        "failed to start worker"
                    );
                }
            }
        } else if actual > desired {
            let count = actual - desired;
            let worker_ids = {
                let workers = self.workers.lock().await;
                workers
                    .iter()
                    .filter_map(|(worker_id, status)| {
                        matches!(status, WorkerStatus::Running).then(|| worker_id.clone())
                    })
                    .take(count)
                    .collect::<Vec<_>>()
            };
            for worker_id in worker_ids {
                self.stop_worker(&worker_id).await;
            }
        }
    }

    pub async fn start_reconciler(self: Arc<Self>) {
        let pool = Arc::clone(&self);
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = pool.shutdown.cancelled() => {
                        tracing::info!("reconciler stopped");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        pool.reconcile().await;
                    }
                }
            }
        });
        *self.reconciler.lock().await = Some(handle);
    }
}
