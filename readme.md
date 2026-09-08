# Rust Code Execution Platform

A distributed code execution platform built with **Rust**, designed to safely execute untrusted source code in isolated Docker containers.

The project focuses on building a reliable asynchronous job execution system rather than simply implementing a code runner. It includes message-based job scheduling, multiple execution workers, persistent job state, task recovery, container isolation, and system observability.

## ✨ Features

* 🚀 **Asynchronous API** built with Rust, Axum and Tokio
* 📬 **Kafka-based job queue** for asynchronous task processing
* ⚙️ **Multiple Workers** for parallel job execution
* 🐳 **Docker sandbox** for isolated code execution
* ⏱️ **Execution timeouts** for compilation and runtime
* 💾 **PostgreSQL** for persistent job and worker state
* 🔒 **Job locking** to prevent multiple workers from processing the same job
* ❤️ **Worker heartbeat** for detecting unhealthy workers
* 🔄 **Job recovery** for jobs left behind by failed workers
* 📊 **Prometheus + Grafana** monitoring
* 🧹 **Workspace lifecycle management** with automatic cleanup
* 🛑 **Graceful cancellation and container cleanup**

## 🏗️ Architecture

```text
                         ┌─────────────────┐
                         │    HTTP Client  │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │   Axum API      │
                         │                 │
                         │ Create / Query  │
                         │      Jobs       │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │      Kafka      │
                         │    Job Queue    │
                         └────────┬────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    │             │             │
                    ▼             ▼             ▼
              ┌──────────┐  ┌──────────┐  ┌──────────┐
              │ Worker 1 │  │ Worker 2 │  │ Worker N │
              │          │  │          │  │          │
              └────┬─────┘  └────┬─────┘  └────┬─────┘
                   │             │             │
                   └─────────────┼─────────────┘
                                 │
                                 ▼
                         ┌─────────────────┐
                         │ Docker Runner   │
                         │                 │
                         │ Compile         │
                         │ Execute         │
                         │ Timeout         │
                         │ Cancellation    │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │    PostgreSQL   │
                         │                 │
                         │ Jobs            │
                         │ Worker State    │
                         │ Execution Info  │
                         └─────────────────┘

              ┌─────────────────────────────┐
              │ Prometheus → Grafana        │
              │ Metrics & Monitoring        │
              └─────────────────────────────┘
```

## 🔄 Job Lifecycle

A submitted job follows the following lifecycle:

```text
Queued
  │
  ▼
Running
  │
  ├──────────────► Completed
  │
  ├──────────────► Failed
  │
  ├──────────────► TimeLimitExceeded
  │
  └──────────────► Worker Failure
                         │
                         ▼
                    Job Recovery
                         │
                         ▼
                       Queued
```

Jobs are persisted in PostgreSQL so that the execution state is not lost when a Worker process exits unexpectedly.

## 🐳 Docker Sandbox

User code is never executed directly inside the Worker process.

Instead, each execution creates an isolated Docker container:

```text
Worker
  │
  ├── Create temporary workspace
  │
  ├── Write source code
  │
  ├── Create Docker container
  │
  ├── Mount workspace
  │
  ├── Compile source code
  │
  ├── Execute program
  │
  ├── Enforce timeout
  │
  ├── Collect result
  │
  └── Remove container
```

The execution layer is implemented using the Docker API through [`bollard`](https://github.com/fussybeaver/bollard).

### Supported execution flow

For Rust jobs:

```text
main.rs
   │
   ▼
rustc
   │
   ▼
main
   │
   ▼
program output
```

Compilation and execution have independent timeout handling.

If a timeout occurs, the running container is stopped and cleaned up.

## 📬 Kafka Job Queue

The API service does not execute user code synchronously.

Instead:

```text
POST /runs
     │
     ▼
Create Job
     │
     ▼
Persist to PostgreSQL
     │
     ▼
Publish JobMessage
     │
     ▼
Kafka
     │
     ▼
Worker
```

This decouples request handling from code execution and allows the execution layer to scale independently.

Multiple Workers can consume jobs concurrently.

## 🔒 Job Locking

A Worker must acquire a job lock before starting execution.

Conceptually:

```sql
UPDATE jobs
SET
    status = 'running',
    locked_at = NOW(),
    lock_token = $2
WHERE id = $1
  AND (
      status = 'queued'
      OR (
          status = 'running'
          AND locked_at < NOW() - INTERVAL '30 seconds'
      )
  );
```

The conditional update makes job acquisition atomic.

This prevents two Workers from normally executing the same job at the same time.

The lock also provides a recovery mechanism for jobs whose Worker disappeared while processing them.

## ❤️ Worker Heartbeat

Workers periodically update their heartbeat information in PostgreSQL.

```text
Worker
  │
  │ heartbeat
  ▼
PostgreSQL
  │
  │
  ▼
Supervisor / Recovery Logic
```

A stale heartbeat can be used to identify Workers that are no longer healthy.

This is also used together with job locking to recover jobs that were left in the `Running` state.

## ⚙️ Worker Pool

The execution service supports multiple independent Workers:

```text
                 ┌──────────────┐
                 │    Kafka     │
                 └──────┬───────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
      Worker 1      Worker 2      Worker 3
          │             │             │
          ▼             ▼             ▼
      Container      Container      Container
```

Workers are intentionally separated from the API service so that execution capacity can be scaled independently.

A Supervisor is used to monitor Worker lifecycle and restart failed Workers.

## 📊 Observability

The project exposes Prometheus metrics for monitoring the execution system.

Examples include:

* Worker startup count
* Job completion count
* Job failure count
* Running jobs
* Job execution duration
* Worker health/status

The monitoring stack is:

```text
Worker / API
     │
     ▼
Prometheus
     │
     ▼
Grafana
```

This makes it possible to monitor both system-level and job-level behavior.

## 🧹 Workspace Management

Each job receives a temporary workspace.

```text
/tmp/
└── code-execution/
    └── <job-id>/
        ├── main.rs
        └── ...
```

The workspace is managed through a Rust `Workspace` abstraction.

Cleanup is performed automatically when the workspace is dropped:

```rust
impl Drop for Workspace {
    fn drop(&mut self) {
        if let Err(err) = fs::remove_dir_all(&self.root_path) {
            tracing::warn!(
                error = %err,
                "failed to remove workspace"
            );
        }
    }
}
```

This provides RAII-style resource management for temporary execution files.

## 🧰 Tech Stack

| Component             | Technology     |
| --------------------- | -------------- |
| Language              | Rust           |
| HTTP Server           | Axum           |
| Async Runtime         | Tokio          |
| Message Queue         | Kafka          |
| Database              | PostgreSQL     |
| Database Access       | SQLx           |
| Container Runtime     | Docker         |
| Docker API            | Bollard        |
| Metrics               | Prometheus     |
| Visualization         | Grafana        |
| Service Orchestration | Docker Compose |
| Logging               | tracing        |

## 🚀 Running Locally

### Requirements

* Rust
* Docker
* Docker Compose
* PostgreSQL
* Kafka

### Start Infrastructure
clone server
[editor-server](https://github.com/ech0o/editor-server)
```bash
git clone https://github.com/ech0o/editor-server.git
```

create table jobs

```sql
CREATE TABLE jobs (
                      id UUID PRIMARY KEY,
                      language TEXT NOT NULL,
                      code TEXT NOT NULL,
                      status TEXT NOT NULL,
                      output TEXT,
                      error TEXT,
                      exit_code INTEGER,
                      worker_id TEXT,
                      locked_at TIMESTAMPTZ,
                      heartbeat_at TIMESTAMPTZ,
                      lock_token UUID,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      finished_at TIMESTAMPTZ
);
```

create table job_outbox
```sql
CREATE TABLE job_outbox (
                            id UUID PRIMARY KEY,
                            job_id UUID NOT NULL REFERENCES jobs(id),
                            event_type TEXT NOT NULL,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            published_at TIMESTAMPTZ
);

CREATE INDEX idx_job_outbox_unpublished
    ON job_outbox (created_at)
    WHERE published_at IS NULL;
```
then
```bash
docker compose up -d
```

This starts the required infrastructure services such as:

```text
PostgreSQL
Kafka
Prometheus
Grafana
Server
```



### Run a Worker

```bash
cargo run -- worker
```

Multiple Workers can be started independently:

```bash
cargo run -- worker
cargo run -- worker
cargo run -- worker
```

Each Worker receives its own `WORKER_ID`.

## 🧪 Example

Submit a Rust program:

```rust
fn main() {
    println!("Hello, Rust!");
}
```

The execution flow is:

```text
HTTP Request
     │
     ▼
Create Job
     │
     ▼
PostgreSQL
     │
     ▼
Kafka
     │
     ▼
Worker
     │
     ▼
Docker Container
     │
     ▼
rustc
     │
     ▼
Program Execution
     │
     ▼
Result
     │
     ▼
PostgreSQL
     │
     ▼
HTTP Response
```

## 🧠 Design Goals

This project is primarily intended as a systems-oriented Rust project.

The main goals are:

1. Learn practical asynchronous Rust with Tokio.
2. Build a real producer/consumer architecture with Kafka.
3. Understand distributed task execution and failure recovery.
4. Practice safe execution of untrusted code using containers.
5. Design persistent task state and concurrency control with PostgreSQL.
6. Build observability using Prometheus and Grafana.
7. Understand Worker lifecycle management and automatic recovery.

## 🗺️ Roadmap

* [x] Axum HTTP API
* [x] Async job execution
* [x] Docker-based code sandbox
* [x] Compilation timeout
* [x] Runtime timeout
* [x] Workspace lifecycle management
* [x] Kafka job queue
* [x] PostgreSQL persistence
* [x] Multiple Workers
* [x] Job locking
* [x] Worker heartbeat
* [x] Prometheus metrics
* [x] Grafana monitoring
* [ ] Worker Supervisor
* [ ] Automatic Worker replacement
* [ ] Failed Worker job recovery
* [ ] Graceful Worker shutdown
* [ ] Integration tests
* [ ] Load testing

## 📌 Why This Project?

Instead of implementing code execution as a single synchronous service, this project explores how a code execution system can be designed as a small distributed system.

The project covers several practical backend engineering problems:

* asynchronous task processing
* message queues
* concurrency control
* resource isolation
* failure detection
* task recovery
* worker lifecycle management
* persistent state
* observability

The architecture is intentionally kept relatively small so that each component can be understood and tested independently.
