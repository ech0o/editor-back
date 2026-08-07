use serde::{
    Deserialize,
    Serialize,
};

#[derive(Deserialize)]
pub struct RunRequest {
    pub language: String,
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct RunResponse {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i64,
}