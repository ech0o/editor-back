use anyhow::Result;
use bollard::container::LogOutput;
use bollard::{
    Docker,
    exec::{CreateExecOptions, StartExecOptions, StartExecResults},
};

use futures_util::StreamExt;
use serde::Serialize;

#[derive(Serialize)]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i64,
}

pub async fn exec(docker: &Docker, container_id: &str, cmd: Vec<String>) -> Result<ExecResult> {
    let exec = docker
        .create_exec(
            container_id,
            CreateExecOptions {
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                cmd: Some(cmd),
                ..Default::default()
            },
        )
        .await?;
    let res = docker
        .start_exec(&exec.id, None::<StartExecOptions>)
        .await?;
    let mut stdout = String::new();
    let mut stderr = String::new();
    match res {
        StartExecResults::Attached { mut output, .. } => {
            while let Some(item) = output.next().await {
                let item = item?;
                match item {
                    LogOutput::StdOut { message } => {
                        stdout.push_str(&String::from_utf8_lossy(&message));
                    }
                    LogOutput::StdErr { message } => {
                        stderr.push_str(&String::from_utf8_lossy(&message));
                    }
                    _ => {}
                }
            }
        }
        StartExecResults::Detached => {}
    }
    let inspect = docker.inspect_exec(&exec.id).await?;
    let exit_code = inspect.exit_code.unwrap_or(-1);
    Ok(ExecResult {
        stdout,
        stderr,
        exit_code,
    })
}
