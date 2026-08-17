use crate::models::{RunResponse, RunStatus};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub language: String,
    pub code: String,
    pub status: RunStatus,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub exit_code: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobState {
    pub status: RunStatus,
    pub result: Option<RunResponse>,
}

#[derive(Debug)]
pub struct JobStore {
    // pub jobs: RwLock<HashMap<Uuid, JobState>>,
    pub pool: SqlitePool,
}

impl JobStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            stdout TEXT,
            stderr TEXT,
            exit_code INTEGER
        )
    "#,
        )
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn create(&self, job: &Job) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO jobs (id, language, code,status,stdout,
            stderr,
            exit_code)
VALUES (?,?,?,?,?,?,?)
"#,
        )
            .bind(job.id.to_string())
            .bind("rust")
            .bind(&job.code)
            .bind(job.status.to_string())
            .bind(&job.stdout)
            .bind(&job.stderr)
            .bind(job.exit_code)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // pub async fn set_running(&self, id: Uuid) {
    //     if let Some(job) = self.jobs.write().await.get_mut(&id) {
    //         job.status = RunStatus::Running;
    //     }
    // }
    //
    // pub async fn finish(&self, id: Uuid, status: RunStatus, res: Option<RunResponse>) {
    //     if let Some(job) = self.jobs.write().await.get_mut(&id) {
    //         job.status = status;
    //         job.result = res;
    //     }
    // }
    //
    pub async fn get(&self, id: Uuid) -> anyhow::Result<Option<Job>> {
        let row = sqlx::query(
            r#"
        SELECT
            id,
            language,
            code,
            status,
            stdout,
            stderr,
            exit_code
        FROM jobs
        WHERE id = ?
        "#,
        )
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut job = Job {
            id: Uuid::try_parse(&row.try_get::<String,_>("id")?.as_str())?,
            language: row.try_get("language")?,
            code: row.try_get("code")?,
            status: RunStatus::Queued,
            stdout: row.try_get("stdout")?,
            stderr: row.try_get("stderr")?,
            exit_code:row.try_get("exit_code")?,
        };
        let status = match row.try_get("status")? {
            "Accepted" => RunStatus::Accepted,
            "Queued" => RunStatus::Queued,
            "Running" => RunStatus::Running,
            "Success" => RunStatus::Success,
            "CompileError" => RunStatus::CompileError,
            "RuntimeError" => RunStatus::RuntimeError,
            "TimeLimitExceeded" => RunStatus::TimeLimitExceeded,
            "MemoryLimitExceeded"=> RunStatus::MemoryLimitExceeded,
            status => anyhow::bail!("unknown run status: {status}"),
        };
        job.status = status;
        Ok(Some(job))
    }

    pub async fn update_status(&self, id: Uuid, status: RunStatus) -> anyhow::Result<()> {
        let result = sqlx::query(
            r#"
UPDATE jobs
SET status = ? WHERE id = ?"#
        ).bind(status.to_string())
            .bind(id.to_string())
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            anyhow::bail!("job not found: {id}");
        }
        Ok(())
    }

    pub async fn finish(
        &self,
        id: Uuid,
        status: RunStatus,
        stdout: Option<String>,
        stderr: Option<String>,
        exit_code: Option<i64>,
    ) -> anyhow::Result<()> {
        let result = sqlx::query(
            r#"
        UPDATE jobs
        SET
            status = ?,
            stdout = ?,
            stderr = ?,
            exit_code = ?
        WHERE id = ?
        "#,
        )
            .bind(status.to_string())
            .bind(stdout)
            .bind(stderr)
            .bind(exit_code)
            .bind(id.to_string())
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            anyhow::bail!("job not found: {id}");
        }

        Ok(())
    }
}
