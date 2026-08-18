use std::collections::HashMap;
use std::fs::remove_dir_all;
use std::path::Path;
use std::sync::Arc;

use crate::docker::image::pull_if_needed;
use crate::docker::workspace::{create_workspace, write_source};
use crate::models::{ExecResult, RunResponse, RunStatus};
use crate::workspace::Workspace;
use anyhow::{Result, anyhow};
use bollard::config::MountType;
use bollard::container::LogOutput;
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::models::{ContainerInspectResponse, HostConfig, Mount};
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

const COMPILE_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_TIMEOUT: Duration = Duration::from_secs(2);

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
            memory: Some(128 * 1024 * 1024),
            memory_swap: Some(128 * 1024 * 1024),
            nano_cpus: Some(500_000_000),
            network_mode: Some(String::from("none")),
            pids_limit: Some(64),
            cap_drop: Some(vec!["ALL".to_string()]),
            readonly_rootfs: Some(true),
            mounts: Some(vec![Mount {
                target: Some("/tmp".to_string()),
                typ: Some(MountType::TMPFS),
                ..Default::default()
            }]),
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

    pub async fn run_rust(&self,job_id:Uuid, code: &str) -> anyhow::Result<RunResponse> {
        let workspace = Workspace::new()?;
        workspace.write("main.rs", code)?;
        self.with_container("rust:1.89", workspace.path(), |id| async move {
            let compile = tokio::time::timeout(
                COMPILE_TIMEOUT,
                self.exec(
                    &id,
                    vec![
                        "rustc".into(),
                        "/workspace/main.rs".into(),
                        "-o".into(),
                        "/workspace/main".into(),
                    ],
                ),
            )
            .await;
            let compile = match compile {
                Ok(result) => result?,
                Err(_) => {
                    self.stop(&id).await?;
                    return Ok(RunResponse {
                        job_id,
                        stdout: String::new(),
                        status: RunStatus::TimeLimitExceeded,
                        stderr: "compilation timed out".to_string(),
                        exit_code: -1,
                    });
                }
            };
            let container = self.inspect_container(id.as_str()).await?;

            let state = container
                .state
                .ok_or_else(|| anyhow!("container state is missing"))?;

            if state.oom_killed.unwrap_or(false) {
                return Ok(RunResponse {
                    job_id,
                    status: RunStatus::MemoryLimitExceeded,
                    stdout: compile.stdout,
                    stderr: compile.stderr,
                    exit_code: compile.exit_code.unwrap_or(-1),
                });
            }
            if compile.exit_code.unwrap() != 0 {
                return Ok(RunResponse {
                    job_id,
                    status: RunStatus::CompileError,
                    stdout: String::new(),
                    stderr: compile.stderr,
                    exit_code: compile.exit_code.unwrap_or(-1),
                });
            }
            let res = timeout(RUN_TIMEOUT, self.exec(&id, vec!["/workspace/main".into()])).await;
            match res {
                Ok(result) => {
                    let result = result?;
                    let container = self.inspect_container(id.as_str()).await?;
                    let state = container
                        .state
                        .ok_or_else(|| anyhow::anyhow!("container state is missing"))?;
                    if state.oom_killed.unwrap_or(false) {
                        return Ok(RunResponse {
                            job_id,
                            stdout: result.stdout,
                            status: RunStatus::MemoryLimitExceeded,
                            stderr: result.stderr,
                            exit_code: result.exit_code.unwrap_or(-1),
                        });
                    }
                    let status = if result.exit_code.unwrap() == 0 {
                        RunStatus::Success
                    } else {
                        RunStatus::RuntimeError
                    };
                    Ok(RunResponse {
                        job_id,
                        stdout: result.stdout,
                        status,
                        stderr: result.stderr,
                        exit_code: result.exit_code.unwrap_or(-1),
                    })
                }
                Err(_) => {
                    if let Err(err) = self.stop(id.as_str()).await {
                        tracing::warn!(container_id = %id,
                        error = %err,
                        "failed to stop timed out container")
                    }
                    Ok(RunResponse {
                        job_id,
                        status: RunStatus::TimeLimitExceeded,
                        stdout: String::new(),
                        stderr: String::new(),
                        exit_code: -1,
                    })
                }
            }
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
        let result = match self.start(id.as_str()).await {
            Ok(()) => f(id.clone()).await,
            Err(err) => Err(err),
        };
        // let result = f(id.clone()).await;
        // if let Err(e) = self.cleanup_container(id.as_str()).await {
        //     tracing::warn!(container_id = %id,
        //         error = %e,
        //         "failed to remove container")
        // }
        self.clean_up(id.as_str()).await;
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

    pub async fn inspect_container(&self, container_id: &str) -> Result<ContainerInspectResponse> {
        Ok(self.docker.inspect_container(container_id, None).await?)
    }

    async fn clean_up(&self, id: &str) {
        if let Err(err) = self.stop(id).await {
            tracing::error!(container_id = %id,err=%err, "failed to stop container");
        }
        if let Err(err) = self.cleanup_container(id).await {
            tracing::error!(container_id = %id,err=%err, "failed to cleanup container");
        }
    }
}
// mod test {
//     use super::*;
// 
//     async fn runner() -> DockerRunner {
//         let docker = Docker::connect_with_local_defaults().unwrap();
//         DockerRunner::new(Arc::new(docker))
//     }
//     #[tokio::test]
//     async fn test_run() -> anyhow::Result<()> {
//         let runner = runner().await;
//         let code = r#"
//               use std::fs;
// 
// fn main() {
//     fs::write("/workspace/test.txt", "hello").unwrap();
//     println!("ok");
// }
//                 "#;
// 
//         let result = runner.run_rust(code).await?;
//         // assert!(matches!(result.status, RunStatus::Accepted));
//         // assert_eq!(result.exit_code, 0);
//         // assert_eq!(result.stdout.trim(), "hello");
//         println!("{:#?}", result);
//         Ok(())
//     }
// 
//     #[tokio::test]
//     async fn run_compile_error() {
//         let runner = runner().await;
// 
//         let result = runner
//             .run_rust(
//                 r#"
//             fn main() {
//                 let x: i32 = "not an integer";
//                 println!("{}", x);
//             }
//             "#,
//             )
//             .await
//             .expect("run_rust failed");
// 
//         assert!(matches!(result.status, RunStatus::CompileError));
// 
//         assert_ne!(result.exit_code, 0);
//         assert!(!result.stderr.is_empty());
//     }
// 
//     #[tokio::test]
//     async fn run_timeout() {
//         let runner = runner().await;
// 
//         let result = runner
//             .run_rust(
//                 r#"
//             fn main() {
//                 loop {}
//             }
//             "#,
//             )
//             .await
//             .expect("run_rust failed");
// 
//         assert!(matches!(result.status, RunStatus::TimeLimitExceeded));
//     }
// 
//     #[tokio::test]
//     async fn run_memory_limit() {
//         let runner = runner().await;
// 
//         let result = runner
//             .run_rust(
//                 r#"
//             fn main() {
//                 let mut data = Vec::new();
// 
//                 loop {
//                     data.push(vec![0u8; 1024 * 1024]);
//                 }
//             }
//             "#,
//             )
//             .await
//             .expect("run_rust failed");
//         println!("{:?}", result.status);
//         assert!(matches!(result.status, RunStatus::MemoryLimitExceeded));
//     }
//     #[tokio::test]
//     async fn capture_stdout_and_stderr() {
//         let runner = runner().await;
// 
//         let result = runner
//             .run_rust(
//                 r#"
//             use std::fs;
// 
// fn main() {
//     fs::write("/tmp/test.txt", "hello").unwrap();
//     println!("ok");
// }
//             "#,
//             )
//             .await
//             .expect("run_rust failed");
// 
//         assert!(matches!(result.status, RunStatus::Accepted));
// 
//         assert!(result.stdout.contains("stdout"));
//         assert!(result.stderr.contains("stderr"));
//     }
// 
//     #[tokio::test]
//     async fn parallel_test() {
//         let code_a = r#"
//         fn main() {
//     std::thread::sleep(std::time::Duration::from_secs(3));
//     println!("A");
//     }"#;
//         let code_b = r#"
//         fn main() {
//     std::thread::sleep(std::time::Duration::from_secs(3));
//     println!("B");
//     }"#;
//         let runner = runner().await;
//         let runner_a = runner.run_rust(code_a);
//         let runner_b = runner.run_rust(code_b);
//         let (a, b) = tokio::join!(runner_a, runner_b);
//         match (a, b) {
//             (Ok(a), Ok(b)) => {
//                 println!("a:{:?} b:{:?}", a, b);
//             }
//             (Err(a), Err(b)) => {
//                 println!("err:a:{:?} err:b:{:?}", a, b);
//             }
//             (Ok(_), Err(_)) | (Err(_), Ok(_)) => {
//                 println!("err");
//             }
//         }
//     }
// }
