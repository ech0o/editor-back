use std::collections::HashMap;
use std::fs::remove_dir_all;
use std::path::Path;
use std::sync::Arc;

use crate::docker::exec::ExecResult;
use crate::docker::image::pull_if_needed;
use crate::docker::workspace::{create_workspace, write_source};
use crate::workspace::Workspace;
use anyhow::Result;
use bollard::container::LogOutput;
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::models::HostConfig;
use bollard::query_parameters::{
    ListContainersOptionsBuilder, RemoveContainerOptionsBuilder, StartContainerOptions,
};
use bollard::{
    Docker, models::ContainerCreateBody, query_parameters::CreateContainerOptionsBuilder,
};
use futures_util::StreamExt;
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone)]
pub struct DockerRunner {
    docker: Arc<Docker>,
}

#[derive(Serialize, Debug)]
pub struct RunResponse {
    stdout: String,
    stderr: String,
    exit_code: i64,
}
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
        self.with_container("rust:1.89",workspace.path(),|id|async move{
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
            let result = if compile.exit_code != 0 {
                RunResponse {
                    stdout: String::new(),
                    stderr: compile.stderr,
                    exit_code: compile.exit_code,
                }
            } else {
                let res = self.exec(&id, vec!["/workspace/main".into()]).await?;
                RunResponse {
                    stdout: res.stdout,
                    stderr: res.stderr,
                    exit_code: res.exit_code,
                }
            };
            Ok(result)
        }).await
        // let id = self.create("rust:1.89", workspace.path()).await?;
        // self.start(id.as_str()).await?;
        // let compile = self
        //     .exec(
        //         &id,
        //         vec![
        //             "rustc".into(),
        //             "/workspace/main.rs".into(),
        //             "-o".into(),
        //             "/workspace/main".into(),
        //         ],
        //     )
        //     .await?;
        // let result = if compile.exit_code != 0 {
        //     RunResponse {
        //         stdout: String::new(),
        //         stderr: compile.stderr,
        //         exit_code: compile.exit_code,
        //     }
        // } else {
        //     let res = self.exec(&id, vec!["/workspace/main".into()]).await?;
        //     RunResponse {
        //         stdout: res.stdout,
        //         stderr: res.stderr,
        //         exit_code: res.exit_code,
        //     }
        // };
        // self.remove(&id).await?;
        // Ok(result)
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
        if let Err(e)=self.cleanup_container(id.as_str()).await{
            tracing::warn!(container_id = %id,
                error = %e,
                "failed to remove container")
        }
        result
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
            println!("Hello, Runner!");
        }
        "#;

        let result = runner.run_rust(code).await?;

        println!("{:#?}", result);
        Ok(())
    }
}
