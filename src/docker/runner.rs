use std::sync::Arc;

use anyhow::Result;
use bollard::{
    Docker,
    models::ContainerCreateBody,
    query_parameters::CreateContainerOptionsBuilder,
};
use bollard::container::LogOutput;
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::query_parameters::{RemoveContainerOptionsBuilder, StartContainerOptions};
use uuid::Uuid;
use crate::docker::image::pull_if_needed;
use futures_util::StreamExt;
use serde::Serialize;

#[derive(Clone)]
pub struct DockerRunner {
    docker: Arc<Docker>,
}

#[derive(Serialize)]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i64,
}
impl DockerRunner {
    pub fn new(docker: Arc<Docker>) -> Self {
        Self { docker }
    }

    pub fn docker(&self) -> &Docker {
        self.docker.as_ref()
    }

    async fn pull_if_needed(
        &self,
        image: &str,
    ) -> Result<()> {
        pull_if_needed(
            self.docker(),
            image,
        )
            .await
    }

    pub async fn create(
        &self,
        image: &str,
    ) -> Result<String> {
        self.pull_if_needed(image).await?;

        let config = ContainerCreateBody {
            image: Some(image.to_owned()),
            cmd: Some(vec![
                "sleep".into(),
                "3600".into(),
            ]),
            tty: Some(false),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            open_stdin: Some(false),
            ..Default::default()
        };

        let options = CreateContainerOptionsBuilder::default()
            .name(&format!("runner-{}", Uuid::new_v4()))
            .build();

        let response = self
            .docker
            .create_container(Some(options), config)
            .await?;

        Ok(response.id)
    }

    pub async fn start(&self,id:&str) -> Result<()> {
        self.docker.start_container(id,None::<StartContainerOptions>).await?;
        Ok(())
    }

    pub async fn remove(&self,id:&str) -> Result<()> {
        let options=RemoveContainerOptionsBuilder::default().force(true).build();
        self.docker.remove_container(id,Some(options)).await?;
        Ok(())
    }

    pub async fn exec(&self,id:&str,cmd:&Vec<String>)->anyhow::Result<ExecResult>{
        let mut ret = ExecResult{
            stdout: "".to_string(),
            stderr: "".to_string(),
            exit_code: 0,
        };
        let exec=self.docker.create_exec(
            id,
            CreateExecOptions{
                attach_stdout:Some(true),
                attach_stderr:Some(true),
                cmd:Some(cmd.clone()),
                ..Default::default()
            }
        ).await?;
        let output=self.docker.start_exec(&exec.id, None).await?;
        match output {
            StartExecResults::Attached {mut output ,.. } => {
                while let Some(msg) = output.next().await {
                    println!("{:?}",msg);
                    match msg? {
                        LogOutput::StdOut {message} => {
                            ret.stdout.push_str(&String::from_utf8_lossy(&message));
                        },
                        LogOutput::StdErr {message} => {
                            ret.stderr.push_str(&String::from_utf8_lossy(&message));
                        }
                        _=>{}
                    }
                }
            }
            StartExecResults::Detached => {}
        }

        let inspect=self.docker.inspect_exec(&exec.id).await?;
        Ok(ret)
    }
}