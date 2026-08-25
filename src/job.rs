use crate::models::{RunResponse, RunStatus};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Pool, Postgres, Row};
use std::collections::HashMap;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use tokio::sync::{RwLock, mpsc};
use uuid::{ Uuid};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub language: String,
    pub code: String,
    pub status: RunStatus,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub exit_code: Option<i32>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobState {
    pub status: RunStatus,
    pub result: Option<RunResponse>,
}

#[derive(Debug)]
pub struct JobStore {
    // pub jobs: RwLock<HashMap<Uuid, JobState>>,
    pub pool: PgPool,
}

impl JobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create(&self, job: &Job) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO jobs (id, language, code,status,stdout,
            stderr,
            exit_code)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"#,
        )
        .bind(job.id)
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

    pub async fn try_start(&self, id: Uuid) -> anyhow::Result<Option<Uuid>> {
        let token = Uuid::new_v4();
        let res = sqlx::query(
            r#"
            UPDATE jobs SET
                status = 'Running',
                locked_at = NOW(),
                lock_token = $2,
                updated_at = NOW()
            WHERE id = $1
            AND (status = 'Queued'
                OR (
                    status = 'Running'
                    AND locked_at < NOW() - INTERVAL '30 seconds'
                ))
"#,
        )
        .bind(id)
        .bind(token)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 1 {
            Ok(Some(token))
        } else {
            Ok(None)
        }
    }
    pub async fn get_and_queued(&self, id: Uuid) -> anyhow::Result<Option<Job>> {
        let row = sqlx::query_as!(Job,
            r#"
        SELECT
            id,
            language,
            code,
            status,
            stdout,
            stderr,
            exit_code,
            created_at
        FROM jobs
        WHERE id = $1
        "#,id
        )
        .fetch_optional(&self.pool)
        .await?;
        let Some(job) = row else {
            return Ok(None);
        };
        // let mut job = Job {
        //     id: row.try_get("id")?,
        //     language: row.try_get("language")?,
        //     code: row.try_get("code")?,
        //     status: RunStatus::Queued,
        //     stdout: row.try_get("stdout")?,
        //     stderr: row.try_get("stderr")?,
        //     exit_code: row.try_get("exit_code")?,
        //     created_at:row.try_get("created_at")?,
        // };
        self.update_status(job.id, RunStatus::Queued).await?;
        // let status = match row.try_get("status")? {
        //     "Accepted" => RunStatus::Accepted,
        //     "Queued" => RunStatus::Queued,
        //     "Running" => RunStatus::Running,
        //     "Success" => RunStatus::Success,
        //     "CompileError" => RunStatus::CompileError,
        //     "RuntimeError" => RunStatus::RuntimeError,
        //     "TimeLimitExceeded" => RunStatus::TimeLimitExceeded,
        //     "MemoryLimitExceeded" => RunStatus::MemoryLimitExceeded,
        //     status => anyhow::bail!("unknown run status: {status}"),
        // };
        // job.status = status;
        Ok(Some(job))
    }

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
        WHERE id = $1
        "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };

        let status = match row.try_get("status")? {
            "Accepted" => RunStatus::Accepted,
            "Queued" => RunStatus::Queued,
            "Running" => RunStatus::Running,
            "Success" => RunStatus::Success,
            "CompileError" => RunStatus::CompileError,
            "RuntimeError" => RunStatus::RuntimeError,
            "TimeLimitExceeded" => RunStatus::TimeLimitExceeded,
            "MemoryLimitExceeded" => RunStatus::MemoryLimitExceeded,
            status => anyhow::bail!("unknown run status: {status}"),
        };
        let job = Job {
            id: row.try_get("id")?,
            language: row.try_get("language")?,
            code: row.try_get("code")?,
            status,
            stdout: row.try_get("stdout")?,
            stderr: row.try_get("stderr")?,
            exit_code: row.try_get("exit_code")?,
            created_at: None,
        };
        Ok(Some(job))
    }
    pub async fn update_status(&self, id: Uuid, status: RunStatus) -> anyhow::Result<()> {
        let result = sqlx::query(
            r#"
UPDATE jobs
SET status = $1 WHERE id = $2"#,
        )
        .bind(status.to_string())
        .bind(id)
        .execute(&self.pool)
        .await?;
        tracing::info!(result = ?result, "updating job status");
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
        exit_code: Option<i32>,
        lock_token:Uuid
    ) -> anyhow::Result<bool> {
        let result = sqlx::query(
            r#"
        UPDATE jobs
        SET
            status = $1,
            stdout = $2,
            stderr = $3,
            exit_code = $4,
            locked_at = NULL,
            lock_token = NULL,
            updated_at = NOW()
        WHERE id = $5
            AND status = 'Running'
            AND lock_token = $6
        "#,
        )
        .bind(status.to_string())
        .bind(stdout)
        .bind(stderr)
        .bind(exit_code)
        .bind(id)
            .bind(lock_token)
        .execute(&self.pool)
        .await?;
        tracing::info!(result = ?result,status=?status, "finish job status");
        if result.rows_affected() == 0 {
            anyhow::bail!("job lease lost: {id}");
        }

        Ok(true)
    }

    pub async fn heartbeat(&self, id: Uuid, lock_token: Uuid) -> anyhow::Result<bool> {
        let res = sqlx::query(
            r#"
            UPDATE jobs
            SET 
                locked_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
            AND  status = 'Running'
            AND lock_token = $2
        "#,
        )
        .bind(id)
        .bind(lock_token)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() == 1)
    }
}
