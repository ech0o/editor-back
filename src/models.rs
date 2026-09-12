use crate::job::Job;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use anyhow::bail;
use serde::de::Visitor;
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug)]
pub struct RunRequest {
    // pub language: String,
    pub code: String,
}

// impl <'de>Visitor<'de> for RunRequest{
//     type Value = RunRequest;
//     fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
//         formatter.write_str("a string containing code")
//     }
//     fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>{
//
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Accepted,
    CompileError,
    RuntimeError,
    TimeLimitExceeded,
    MemoryLimitExceeded,
    Queued,
    Running,
    Success,
}

impl From<String> for RunStatus {
    fn from(code: String) -> Self {
        match code.as_ref() {
            "Accepted" => RunStatus::Accepted,
            "CompileError" => RunStatus::CompileError,
            "RuntimeError" => RunStatus::RuntimeError,
            "TimeLimitExceeded" => RunStatus::TimeLimitExceeded,
            "MemoryLimitExceeded" => RunStatus::MemoryLimitExceeded,
            "Queued" => RunStatus::Queued,
            "Running" => RunStatus::Running,
            "Success" => RunStatus::Success,
            _=>panic!("Unknown status: {}", code),
        }
    }
}


impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Accepted => "Accepted",
            RunStatus::CompileError => "CompileError",
            RunStatus::RuntimeError => "RuntimeError",
            RunStatus::TimeLimitExceeded => "TimeLimitExceeded",
            RunStatus::MemoryLimitExceeded => "MemoryLimitExceeded",
            RunStatus::Queued => "Queued",
            RunStatus::Running => "Running",
            RunStatus::Success => "Success",
        }
    }
}
impl Display for RunStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RunStatus::Accepted => {
                write!(f, "Accepted")
            }
            RunStatus::CompileError => {
                write!(f, "CompileError")
            }
            RunStatus::RuntimeError => {
                write!(f, "RuntimeError")
            }
            RunStatus::TimeLimitExceeded => {
                write!(f, "TimeLimitExceeded")
            }
            RunStatus::MemoryLimitExceeded => {
                write!(f, "MemoryLimitExceeded")
            }
            RunStatus::Queued => {
                write!(f, "Queued")
            }
            RunStatus::Running => {
                write!(f, "Running")
            }
            RunStatus::Success => {
                write!(f, "Success")
            }
        }
    }
}
#[derive(Debug, Serialize, Clone)]
pub struct RunResponse {
    pub stdout: String,
    pub status: RunStatus,
    pub stderr: String,
    pub exit_code: i32,
    pub job_id: Uuid,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Serialize, Clone)]
pub struct JobIdResponse {
    pub job_id: Uuid,
}

#[derive(Serialize, Debug)]
pub struct JobResponse {
    pub id: Uuid,
    pub language: String,
    pub status: RunStatus,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub exit_code: Option<i32>,
}

impl From<Job> for JobResponse {
    fn from(job: Job) -> JobResponse {
        Self {
            id: job.id,
            language: job.language,
            status: job.status,
            stdout: job.stdout,
            stderr: job.stderr,
            exit_code: job.exit_code,
        }
    }
}

#[derive(Debug,Serialize,Deserialize)]
pub struct JobMessage{
    pub job_id: Uuid,
}

#[derive(Debug,Serialize,Deserialize)]
pub struct DlqMessage{
    pub original_payload:Vec<u8>,
    pub error:String,
    pub topic:String,
    pub partition:i32,
    pub offset:i64,
}

#[derive(Debug,Serialize,Deserialize)]
pub struct ScaleRequest{
    pub count: usize,
}
#[derive(Debug,Serialize,Deserialize)]
pub struct ScaleResponse{
    pub msg: String,
}

