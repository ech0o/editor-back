use std::collections::HashMap;
use std::fs::remove_dir_all;
use std::path::Path;
use std::sync::Arc;

use crate::docker::exec::ExecResult;
use crate::docker::image::pull_if_needed;
use crate::docker::workspace::{create_workspace, write_source};
use crate::models::{RunResponse, RunStatus};
use crate::workspace::Workspace;
use anyhow::Result;
use bollard::container::LogOutput;
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::models::HostConfig;
use bollard::query_parameters::{
    ListContainersOptionsBuilder, RemoveContainerOptionsBuilder, StartContainerOptions,
    StopContainerOptions,
};
use bollard::{
    Docker, models::ContainerCreateBody, query_parameters::CreateContainerOptionsBuilder,
};
use futures_util::StreamExt;
use serde::Serialize;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

#[derive(Clone)]
pub struct DockerRunner {
    docker: Arc<Docker>,
}

// #[derive(Serialize, Debug)]
// pub struct RunResponse {
//     stdout: String,
//     stderr: String,
//     exit_code: i64,
// }
impl DockerRunner {
    pub fn new(docker: Arc<Docker>) -> Self {
        Self { docker }
    }

    pub fn docker(&self) -> &Docker {
        self.docker.as_ref()
    }

    async fn pull_if_needed(&self, image: &str) -> Result<()> {
        pull_if_needed(self.docker(), image).await
    }

    pub async fn create(&self, image: &str, workspace: &Path) -> Result<String> {
        self.pull_if_needed(image).await?;

        let host_config = HostConfig {
            binds: Some(vec![format!("{}:/workspace", workspace.to_string_lossy())]),
            ..Default::default()
        };

        let config = ContainerCreateBody {
            image: Some(image.to_owned()),
            cmd: Some(vec!["sleep".into(), "3600".into()]),
            tty: Some(false),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            open_stdin: Some(false),
            host_config: Some(host_config),
            ..Default::default()
        };

        let options = CreateContainerOptionsBuilder::default()
            .name(&format!("runner-{}", Uuid::new_v4()))
            .build();

        let response = self.docker.create_container(Some(options), config).await?;

        Ok(response.id)
    }

    pub async fn start(&self, id: &str) -> Result<()> {
        self.docker
            .start_container(id, None::<StartContainerOptions>)
            .await?;
        Ok(())
    }

    pub async fn remove(&self, id: &str) -> Result<()> {
        let options = RemoveContainerOptionsBuilder::default().force(true).build();
        self.docker.remove_container(id, Some(options)).await?;
        Ok(())
    }

    pub async fn exec(&self, id: &str, cmd: Vec<String>) -> anyhow::Result<ExecResult> {
        crate::docker::exec::exec(self.docker(), id, cmd).await
    }

    pub async fn run_rust(&self, code: &str) -> anyhow::Result<RunResponse> {
        let workspace = Workspace::new()?;
        workspace.write("main.rs", code)?;
        self.with_container("rust:1.89", workspace.path(), |id| async move {
            let compile = self
                .exec(
                    &id,
                    vec![
                        "rustc".into(),
                        "/workspace/main.rs".into(),
                        "-o".into(),
                        "/workspace/main".into(),
                    ],
                )
                .await?;
            if compile.exit_code != 0 {
                return Ok(RunResponse {
                    status: RunStatus::CompileError,
                    stdout: String::new(),
                    stderr: compile.stderr,
                    exit_code: compile.exit_code,
                });
            }
            let res = timeout(
                Duration::from_secs(2),
                self.exec(&id, vec!["/workspace/main".into()]),
            )
            .await;
            let res = match res {
                Ok(result) => result?,
                Err(_) => {
                    if let Err(err) = self.stop(id.as_str()).await {
                        tracing::warn!(container_id = %id,
                        error = %err,
                        "failed to stop timed out container")
                    }
                    return Ok(RunResponse {
                        status: RunStatus::TimeLimitExceeded,
                        stdout: String::new(),
                        stderr: String::new(),
                        exit_code: -1,
                    });
                }
            };
            Ok(RunResponse {
                status: if res.exit_code == 0 {
                    RunStatus::Accepted
                } else {
                    RunStatus::RuntimeError
                },
                stdout: res.stdout,
                stderr: res.stderr,
                exit_code: res.exit_code,
            })
        })
        .await
    }

    async fn cleanup_container(&self, id: &str) -> Result<()> {
        self.remove(id).await?;
        Ok(())
    }

    pub async fn with_container<F, Fut, T>(&self, image: &str, workspace: &Path, f: F) -> Result<T>
    where
        F: FnOnce(String) -> Fut,
        Fut: Future<Output = anyhow::Result<T>>,
    {
        let id = self.create(image, workspace).await?;
        self.start(id.as_str()).await?;
        let result = f(id.clone()).await;
        if let Err(e) = self.cleanup_container(id.as_str()).await {
            tracing::warn!(container_id = %id,
                error = %e,
                "failed to remove container")
        }
        result
    }

    pub async fn stop(&self, id: &str) -> Result<()> {
        self.docker
            .stop_container(
                id,
                Some(StopContainerOptions {
                    signal: None,
                    t: Some(1),
                }),
            )
            .await?;
        Ok(())
    }
}
mod test {
    use super::*;
    #[tokio::test]
    async fn test_run() -> anyhow::Result<()> {
        let docker = Docker::connect_with_local_defaults()?;
        let runner = DockerRunner::new(Arc::new(docker));
        let code = r#"
        fn main() {
             panic!("oops");
        }
        "#;

        let result = runner.run_rust(code).await?;

        println!("{:#?}", result);
        Ok(())
    }
}
