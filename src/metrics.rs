use prometheus::{Encoder, Gauge, Histogram, HistogramOpts, IntCounter, Registry};

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
    pub jobs_finished_total: IntCounter,
    pub jobs_failed_total: IntCounter,
    pub jobs_running: Gauge,

    pub job_duration_seconds: Histogram,
    pub job_queue_latency_seconds: Histogram,
    pub registry: Registry,
}

impl RunningGuard {
    pub fn new(gauge: Gauge) -> Self {
        gauge.inc();
        Self { gauge }
    }
}

impl Metrics {
    pub fn new()->anyhow::Result<Self> {
        let registry = Registry::new();
        let worker_started_total=IntCounter::new(
            "worker_started_total",
            "Total number of workers started",
        )?;
        let jobs_created_total=IntCounter::new(
            "jobs_created_total",
            "Total number of jobs created",
        )?;
        let jobs_finished_total=IntCounter::new(
            "jobs_finished_total",
            "Total number of jobs finished",
        )?;
        let jobs_failed_total=IntCounter::new(
            "jobs_failed_total",
            "Total number of jobs failed",
        )?;

        let jobs_running=Gauge::new(
            "jobs_running",
            "Number of currently running jobs"
        )?;

        let job_duration_seconds=Histogram::with_opts(HistogramOpts::new(
            "job_duration_seconds",
            "Job execution duration in seconds"
        ))?;
        let job_queue_latency_seconds=Histogram::with_opts(HistogramOpts::new(
            "job_queue_latency_seconds",
            "Time a job waits before being processed by a worker"
        ))?;

        registry.register(Box::new(worker_started_total.clone()))?;
        registry.register(Box::new(jobs_created_total.clone()))?;
        registry.register(Box::new(jobs_finished_total.clone()))?;
        registry.register(Box::new(jobs_failed_total.clone()))?;
        registry.register(Box::new(job_duration_seconds.clone()))?;
        registry.register(Box::new(job_queue_latency_seconds.clone()))?;
        Ok(Self{
            worker_started_total,
            jobs_created_total,
            jobs_finished_total,
            jobs_failed_total,
            jobs_running,
            job_duration_seconds,
            job_queue_latency_seconds,
            registry,
        })

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
