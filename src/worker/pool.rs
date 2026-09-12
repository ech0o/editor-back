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

const MAX_SCALE_BATCH: usize = 5;

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
    scale_lock: Mutex<()>,
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

#[derive(Debug, PartialEq, Eq)]
pub enum ScaleAction {
    Up(usize),
    Down(usize),
    None,
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
            scale_lock: Mutex::new(()),
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

    pub async fn start_worker(&self) -> anyhow::Result<()> {
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

                        let mut workers = workers.lock().await;
                        workers.remove(&worker_id);
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
                       Self::cleanup_workers(
                            &worker_id,
                            &workers,
                            &controls,
                            &metrics
                        ).await;
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
                    {
                        let mut controls = controls.lock().await;
                        controls.remove(&worker_id);
                    }
                    backoff = next_backoff(backoff, uptime);
                    // tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                   if !Self::wait_or_shutdown(backoff, &shutdown).await{

                       Self::set_status(
                            &workers,
                            &worker_id,
                            WorkerStatus::Stopped,
                            &metrics
                        ).await;
                        return;
                    }

                }
                _ = shutdown.cancelled() => {
                    tracing::info!(worker_id=%worker_id,"supervisor shutting down worker");
                    worker_shutdown.cancel();
                    Self::cleanup_workers(
                        &worker_id,
                        &workers,
                        &controls,
                        &metrics
                    ).await;

                    return;
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
            _ = tokio::time::sleep(duration) => {
                tracing::info!("worker sleep {:?}",duration);
                true
            },
            _ = shutdown.cancelled() => false,
        }
    }

    pub async fn stop_worker(&self, worker_id: &str) {
        let control = {
            let controls = self.controls.lock().await;
            controls.get(worker_id).cloned()
        };
        let Some(control) = control else {
            tracing::warn!(worker_id=%worker_id,"worker control not found");
            return;
        };
        Self::set_status(
            &self.workers,
            worker_id,
            WorkerStatus::Stopping,
            &self.metrics,
        )
        .await;
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
        let _guard = self.scale_lock.lock().await;
        self.desired_size.store(target, Ordering::Relaxed);
        self.reconcile_inner().await
    }

    pub async fn managed_worker_count(&self) -> usize {
        let supervisors = self.supervisors.lock().await;
        supervisors.len()
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
        let _guard = self.scale_lock.lock().await;
        self.reconcile_inner().await;
    }
    pub async fn reconcile_inner(&self) {
        let desired = self.desired_size.load(Ordering::Relaxed);
        let actual = {
            let supervisors = self.supervisors.lock().await;
            supervisors.len()
        };
        let status_count = {
            let workers = self.workers.lock().await;

            let mut counts = HashMap::new();
            for status in workers.values() {
                let name = match status {
                    WorkerStatus::Stopping => "stopping",
                    WorkerStatus::Stopped => "stopped",
                    WorkerStatus::Starting => "starting",
                    WorkerStatus::Restarting => "restarting",
                    WorkerStatus::Running => "running",
                };
                *counts.entry(name).or_insert(0usize) += 1;
            }
            counts
        };

        for status in ["starting", "running", "restarting", "stopping", "stopped"] {
            let count = status_count.get(status).copied().unwrap_or(0);
            self.metrics
                .worker_pool_status
                .with_label_values(&[status])
                .set(count as i64);
        }
        self.metrics.worker_pool_desired.set(desired as i64);
        self.metrics.worker_pool_current.set(actual as i64);
        self.metrics.worker_pool_reconcile_total.inc();
        match calculate_scale(actual, desired) {
            ScaleAction::Up(count) => {
                for _ in 0..count {
                    if let Err(e) = self.start_worker().await {
                        tracing::error!(
                            error=%e,
                            "failed to start worker"
                        );
                    }
                }
            }
            ScaleAction::Down(count) => {
                tracing::debug!("reconciling {} workers,scale down", count);
                self.metrics
                    .worker_pool_scale_down_total
                    .inc_by(count as u64);

                let worker_ids = {
                    let mut workers = self.workers.lock().await;
                    let worker_ids = workers
                        .iter()
                        .filter_map(|(worker_id, status)| {
                            matches!(status, WorkerStatus::Running).then(|| worker_id.clone())
                        })
                        .take(count)
                        .collect::<Vec<_>>();
                    for worker_id in &worker_ids {
                        if let Some(status) = workers.get_mut(worker_id) {
                            *status = WorkerStatus::Stopping;
                        }
                    }
                    worker_ids
                };
                for worker_id in worker_ids {
                    self.stop_worker(&worker_id).await;
                }
            }
            ScaleAction::None => {}
        }
        // if actual < desired {
        //     let count = (desired - actual).min(MAX_SCALE_BATCH);
        //     tracing::debug!("reconciling {} workers,scale up", count);
        //     self.metrics.worker_pool_scale_up_total.inc_by(count as u64);
        //     for _ in 0..count {
        //         if let Err(e) = self.start_worker().await {
        //             tracing::error!(
        //                 error=%e,
        //                 "failed to start worker"
        //             );
        //         }
        //     }
        // } else if actual > desired {
        //     let count = (actual - desired).min(MAX_SCALE_BATCH);
        //
        //     tracing::debug!("reconciling {} workers,scale down", count);
        //     self.metrics
        //         .worker_pool_scale_down_total
        //         .inc_by(count as u64);
        //
        //     let worker_ids = {
        //         let workers = self.workers.lock().await;
        //         workers
        //             .iter()
        //             .filter_map(|(worker_id, status)| {
        //                 matches!(status, WorkerStatus::Running).then(|| worker_id.clone())
        //             })
        //             .take(count)
        //             .collect::<Vec<_>>()
        //     };
        //     for worker_id in worker_ids {
        //         self.stop_worker(&worker_id).await;
        //     }
        // }
        let actual = {
            let supervisors = self.supervisors.lock().await;
            supervisors.len()
        };
        self.metrics.worker_pool_current.set(actual as i64);
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

    async fn cleanup_workers(
        worker_id: &str,
        workers: &Arc<Mutex<HashMap<String, WorkerStatus>>>,
        controls: &Arc<Mutex<HashMap<String, WorkerControl>>>,
        metrics: &Metrics,
    ) {
        {
            let mut controls = controls.lock().await;
            controls.remove(worker_id);
        }
        Self::set_status(workers, worker_id, WorkerStatus::Stopped, metrics).await;

        let mut workers = workers.lock().await;
        workers.remove(worker_id);
    }
}
fn calculate_scale(actual: usize, desired: usize) -> ScaleAction {
    if actual < desired {
        ScaleAction::Up((desired - actual).min(MAX_SCALE_BATCH))
    } else if actual > desired {
        ScaleAction::Down((actual - desired).min(MAX_SCALE_BATCH))
    } else {
        ScaleAction::None
    }
}

fn next_backoff(current: Duration, uptime: Duration) -> Duration {
    if uptime >= Duration::from_secs(30) {
        INITIAL_BACKOFF
    } else {
        std::cmp::min(current * 2, MAX_BACKOFF)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scale_up() {
        assert_eq!(calculate_scale(3, 5), ScaleAction::Up(2));
    }

    #[test]
    fn test_scale_down() {
        assert_eq!(calculate_scale(5, 2), ScaleAction::Down(3));
    }

    #[test]
    fn test_no_scaling() {
        assert_eq!(calculate_scale(3, 3), ScaleAction::None);
    }

    #[test]
    fn test_scale_up_is_limited() {
        assert_eq!(calculate_scale(0, 100), ScaleAction::Up(5));
    }

    #[test]
    fn test_scale_down_is_limited() {
        assert_eq!(calculate_scale(100, 0), ScaleAction::Down(5));
    }

    #[test]
    fn test_backoff_resets_after_stable_run() {
        let backoff = next_backoff(
            Duration::from_secs(8),
            Duration::from_secs(31),
        );

        assert_eq!(
            backoff,
            INITIAL_BACKOFF
        );
    }

    #[test]
    fn test_backoff_doubles_after_quick_crash() {
        let backoff = next_backoff(
            Duration::from_secs(2),
            Duration::from_secs(5),
        );

        assert_eq!(
            backoff,
            Duration::from_secs(4)
        );
    }

    #[test]
    fn test_backoff_is_capped() {
        let backoff = next_backoff(
            MAX_BACKOFF,
            Duration::from_secs(1),
        );

        assert_eq!(
            backoff,
            MAX_BACKOFF
        );
    }
}
