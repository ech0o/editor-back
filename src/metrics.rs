use crate::worker::pool::WorkerStatus;
use prometheus::core::Collector;
use prometheus::{
    Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    Opts, Registry,
};

pub fn gather() -> String {
    let metric_families = prometheus::gather();
    let encoder = prometheus::TextEncoder::new();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

pub struct RunningGuard {
    gauge: Gauge,
}

pub struct Metrics {
    pub worker_started_total: IntCounter,
    pub jobs_created_total: IntCounter,
    pub jobs_finished_total: IntCounterVec,
    pub jobs_failed_total: IntCounter,
    pub jobs_running: Gauge,
    pub worker_status: GaugeVec,
    pub worker_restarts_total: IntCounterVec,

    pub job_duration_seconds: HistogramVec,
    pub job_queue_latency_seconds: Histogram,
    pub jobs_finished_by_status: IntCounterVec,
    pub registry: Registry,
}

impl RunningGuard {
    pub fn new(gauge: Gauge) -> Self {
        gauge.inc();
        Self { gauge }
    }
}

impl Metrics {
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new();
        let worker_started_total =
            IntCounter::new("worker_started_total", "Total number of workers started")?;
        let jobs_created_total =
            IntCounter::new("jobs_created_total", "Total number of jobs created")?;
        let jobs_finished_total = IntCounterVec::new(
            Opts::new("jobs_finished_total", "Total number of jobs finished"),
            &["worker_id"],
        )?;
        let jobs_failed_total =
            IntCounter::new("jobs_failed_total", "Total number of jobs failed")?;

        let jobs_running = Gauge::new("jobs_running", "Number of currently running jobs")?;

        let job_duration_seconds = HistogramVec::new(
            HistogramOpts::new("job_duration_seconds", "Duration of job execution"),
            &["worker_id"],
        )?;
        let job_queue_latency_seconds = Histogram::with_opts(HistogramOpts::new(
            "job_queue_latency_seconds",
            "Time a job waits before being processed by a worker",
        ))?;

        let jobs_finished_by_status = IntCounterVec::new(
            Opts::new("jobs_finished_by_status", "Total number of jobs processed"),
            &["status"],
        )?;

        let worker_status = GaugeVec::new(
            prometheus::Opts::new("worker_status", "Current worker status"),
            &["worker_id", "status"],
        )?;

        let worker_restarts_total = IntCounterVec::new(
            prometheus::Opts::new("worker_restarts_total", "Total number of worker restarts"),
            &["worker_id"],
        )?;

        registry.register(Box::new(worker_started_total.clone()))?;
        registry.register(Box::new(jobs_running.clone()))?;
        registry.register(Box::new(jobs_created_total.clone()))?;
        registry.register(Box::new(jobs_finished_total.clone()))?;
        registry.register(Box::new(jobs_failed_total.clone()))?;
        registry.register(Box::new(job_duration_seconds.clone()))?;
        registry.register(Box::new(job_queue_latency_seconds.clone()))?;
        registry.register(Box::new(jobs_finished_by_status.clone()))?;
        registry.register(Box::new(worker_status.clone()))?;
        registry.register(Box::new(worker_restarts_total.clone()))?;

        Ok(Self {
            worker_started_total,
            jobs_created_total,
            jobs_finished_total,
            jobs_failed_total,
            jobs_running,
            job_duration_seconds,
            job_queue_latency_seconds,
            jobs_finished_by_status,
            worker_status,
            worker_restarts_total,
            registry,
        })
    }

    pub fn set_worker_status(&self, worker_id: &str, status: WorkerStatus) {
        const STATUSES: [&str; 4] = ["starting", "running", "restarting", "stopped"];

        for name in STATUSES {
            self.worker_status
                .with_label_values(&[worker_id, name])
                .set(0f64);
        }
        self.worker_status
            .with_label_values(&[worker_id, status.as_str()])
            .set(1f64);
    }

    pub fn worker_restarted(&self, worker_id: &str) {
        self.worker_restarts_total
            .with_label_values(&[worker_id])
            .inc();
    }
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

pub fn job_completed() {
    metrics::counter!("edit_jobs_completed_total").increment(1);
}

pub fn job_failed() {
    metrics::counter!("edit_jobs_failed_total").increment(1);
}

pub fn job_duration(seconds: f64) {
    metrics::histogram!("editor_job_duration_seconds").record(seconds);
}
