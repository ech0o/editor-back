use serde::{Deserialize, Serialize};

#[derive(Deserialize,Serialize,Debug)]
pub struct RunRequest {
    // pub language: String,
    pub code: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Accepted,
    CompileError,
    RuntimeError,
    TimeLimitExceeded,
    MemoryLimitExceeded,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RunResponse {
    pub stdout: String,
    pub status: RunStatus,
    pub stderr: String,
    pub exit_code: i64,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i64>,
}
