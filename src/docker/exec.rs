use anyhow::Result;
use bollard::container::LogOutput;
use bollard::{
    Docker,
    exec::{CreateExecOptions, StartExecOptions, StartExecResults},
};

use futures_util::StreamExt;
use serde::Serialize;
pub(crate) use crate::models::ExecResult;
// #[derive(Serialize)]
// pub struct ExecResult {
//     pub stdout: String,
//     pub stderr: String,
//     pub exit_code: i64,
// }

pub async fn exec(docker: &Docker, container_id: &str, cmd: Vec<String>) -> Result<ExecResult> {
    let config = CreateExecOptions {
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        cmd: Some(cmd),
        ..Default::default()
    };
    let exec = docker
        .create_exec(
            container_id,
            config
        )
        .await?;
    let res = docker
        .start_exec(&exec.id, None::<StartExecOptions>)
        .await?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    match res {
        StartExecResults::Attached { mut output, .. } => {
            while let Some(item) = output.next().await {
                let item = item?;
                match item {
                    LogOutput::StdOut { message } => {
                        stdout.extend_from_slice(&message);
                    }
                    LogOutput::StdErr { message } => {
                        stderr.extend_from_slice(&message);
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
        stdout:String::from_utf8_lossy(&stdout).to_string(),
        stderr:String::from_utf8_lossy(&stderr).to_string(),
        exit_code:Some(exit_code),
    })
}
