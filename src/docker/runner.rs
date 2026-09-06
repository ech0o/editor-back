use std::collections::HashMap;
use std::fs;
use std::fs::remove_dir_all;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::docker::image::pull_if_needed;
use crate::error::RunError;
use crate::job::JobStore;
use crate::models::{ExecResult, RunResponse, RunStatus};
use crate::workspace::Workspace;
use anyhow::{Result, anyhow, bail};
use bollard::config::MountType;
use bollard::container::LogOutput;
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::models::{ContainerInspectResponse, HostConfig, Mount};
use bollard::query_parameters::{
    ListContainersOptions, ListContainersOptionsBuilder, RemoveContainerOptions,
    RemoveContainerOptionsBuilder, StartContainerOptions, StopContainerOptions,
};
use bollard::{
    Docker, models::ContainerCreateBody, query_parameters::CreateContainerOptionsBuilder,
};
use futures_util::StreamExt;
use serde::Serialize;
use std::time::Duration;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use crate::worker::workspace::WorkerWorkspace;

const COMPILE_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_TIMEOUT: Duration = Duration::from_secs(20);

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

    pub async fn create(&self,worker_id:&str, image: &str, workspace: &Path, job_id: Uuid,temp_path:String) -> Result<String> {
        self.pull_if_needed(image).await?;
        let main_rs = workspace.join("main.rs");
        tracing::info!(path=%workspace.display(), "creating path:");
        tracing::debug!(
            path = %main_rs.display(),
            exists = main_rs.exists(),
            "checking workspace"
        );
        // tracing::info!(path=%temp_path,"checking tmp:");
        let host_config = HostConfig {
            binds: Some(vec![format!("{}:/workspaces", workspace.to_string_lossy())]),
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

        let label = HashMap::from([
            ("app".to_string(), "code-runner".to_string()),
            ("managed-by".to_string(), "worker".to_string()),
            ("oj.worker_id".to_string(), worker_id.to_string()),
            ("oj.job_id".to_string(), job_id.to_string()),
            (
                "oj.workspace".to_string(),
                temp_path,
            ),
        ]);

        let config = ContainerCreateBody {
            image: Some(image.to_owned()),
            cmd: Some(vec!["sleep".into(), "3600".into()]),
            tty: Some(false),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            open_stdin: Some(false),
            host_config: Some(host_config),
            labels: Some(label),
            ..Default::default()
        };

        let options = CreateContainerOptionsBuilder::default()
            .name(&format!("runner-{}", Uuid::new_v4()))
            .build();

        let response = self.docker.create_container(Some(options), config).await?;
        let inspect = self.docker.inspect_container(&response.id, None).await?;

        tracing::info!(
            labels = ?inspect.config.and_then(|c| c.labels),
            "code container labels"
        );
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

    pub async fn run_rust(
        &self,
        worker_id: &str,
        job_id: Uuid,
        code: &str,
        cancel: CancellationToken,
        workspace: Workspace
    ) -> anyhow::Result<RunResponse, RunError> {
        // let workspace = Workspace::new(worker_id).map_err(RunError::Other)?;
        workspace.write("main.rs", code).map_err(RunError::Other)?;
        self.with_container(
            "rust:1.89",
            workspace.host_path(),
            cancel,
            job_id,
            worker_id,
            workspace.path().to_string_lossy().into_owned(),
            |id| async move {
                // let res =self.exec(&id,vec!["ls".into(),"/workspaces".into()]).await?;
                // tracing::info!(result=?res,"workspace result");
                let compile = tokio::time::timeout(
                    COMPILE_TIMEOUT,
                    self.exec(
                        &id,
                        vec![
                            "rustc".into(),
                            "/workspaces/main.rs".into(),
                            "-o".into(),
                            "/workspaces/main".into(),
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
                let res =
                    timeout(RUN_TIMEOUT, self.exec(&id, vec!["/workspaces/main".into()])).await;
                tracing::info!(job_id=?job_id, result=?res, "finished exec");
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
            },
        )
        .await
    }

    async fn cleanup_container(&self, id: &str) -> Result<()> {
        self.remove(id).await?;
        Ok(())
    }

    pub async fn with_container<F, Fut>(
        &self,
        image: &str,
        workspace: &Path,
        cancel: CancellationToken,
        job_id: Uuid,
        worker_id: &str,
        temp_path:String,
        f: F,
    ) -> Result<RunResponse, RunError>
    where
        F: FnOnce(String) -> Fut,
        Fut: Future<Output = anyhow::Result<RunResponse, RunError>>,
    {
        let id = self
            .create(worker_id, image, workspace, job_id, temp_path)
            .await
            .map_err(RunError::Other)?;
        self.start(id.as_str()).await.map_err(RunError::Other)?;
        tracing::info!(id=?id, "started container");
        let result = tokio::select! {
                res = f(id.clone())=>{
                    res
                }
                _= cancel.cancelled() => {
                    tracing::warn!(container_id = %id,"container execution cancelled");
                    if let Err(err)=self.stop(id.as_str()).await{
                    tracing::warn!(container_id = %id,err=%err,"failed to stop container");
                }
                    Err(RunError::Cancelled)
            }
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

    // pub async fn cleanup_orphans(&self) -> Result<()> {
    //     let filters = HashMap::from([(
    //         "label".to_string(),
    //         vec![
    //             "app=code-runner".to_string(),
    //             "managed-by=worker".to_string(),
    //             format!("worker_id-{}", self.worker_id),
    //         ],
    //     )]);
    //
    //     let containers = self
    //         .docker
    //         .list_containers(Some(ListContainersOptions {
    //             all: true,
    //             filters: Some(filters),
    //             ..Default::default()
    //         }))
    //         .await?;
    //
    //     for container in containers {
    //         if let Some(id) = container.id {
    //             tracing::warn!(container_id = %id,worker_id=%self.worker_id,"removing orphan container");
    //             let _ = self.remove(&id).await;
    //         }
    //     }
    //     Ok(())
    // }

    pub async fn list_code_containers(&self) -> Result<Vec<(String, Uuid)>> {
        let containers = self
            .docker
            .list_containers(Some(ListContainersOptions {
                all: true,
                filters: Some(HashMap::from([(
                    "label".to_string(),
                    vec!["managed-by=worker".to_string()],
                )])),
                ..Default::default()
            }))
            .await?;

        let mut result = Vec::new();
        for container in containers {
            let Some(id) = container.id else {
                continue;
            };
            let Some(labels) = container.labels else {
                continue;
            };

            let Some(job_id) = labels
                .get("oj.job_id")
                .and_then(|value| Uuid::parse_str(value).ok())
            else {
                tracing::warn!(
                    container_id = %id,
                    "code container has invalid job_id"
                );
                continue;
            };

            result.push((id, job_id));
        }
        Ok(result)
    }

    pub async fn clean_orphan_containers(&self, jobs: &JobStore) -> anyhow::Result<()> {
        let containers = self.list_code_containers().await?;

        for (container_id, job_id) in containers {
            let alive = jobs.is_job_execution_alive(job_id).await?;
            if alive {
                tracing::info!(
                    container_id = %container_id,
                    %job_id,
                    "code container is still active"
                );

                continue;
            }

            tracing::warn!(
                container_id = %container_id,
                %job_id,
                "removing orphan code container"
            );

            let workspace = match self.get_workspace_from_container(&container_id).await {
                Ok(path) => path,
                Err(error) => {
                    tracing::error!(
                        container_id=%container_id,
                        %job_id,
                        ?error,
                        "failed to get worksapce from container"
                    );
                    None
                }
            };
            let a = workspace.as_ref().unwrap();
            let path = a.as_path();
            tracing::info!(
                path = %path.display(),
                exists= path.exists(),
                "workspace before container removal"
            );

            if let Err(error) = self
                .docker
                .remove_container(
                    &container_id,
                    Some(RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    }),
                )
                .await
            {
                tracing::error!(
                    container_id = %container_id,
                    %job_id,
                    ?error,
                    "failed to remove orphan code container"
                );
            }

            tracing::info!(
                path = %path.display(),
                exists= path.exists(),
                "workspace after container removal"
            );

            if let Some(workspace) = workspace {
                // if let Err(err) = fs::remove_dir_all(&workspace) {
                //     tracing::error!(
                //         workspace=%workspace.display(),
                //         %job_id,
                //         ?err,
                //         "failed to remove orphan code workspace"
                //     )
                // } else {
                //     tracing::info!(
                //         workspace=%workspace.display(),
                //         %job_id,
                //         "removed orphan code workspace"
                //     );
                // }
                tracing::info!(
                    path = %workspace.display(),
                    "calling remove_workspace"
                );
                Self::remove_workspace(&workspace).await;
            }
        }

        Ok(())
    }

    async fn remove_workspace(path: &PathBuf) {
        for attempt in 0..5 {
            match fs::remove_dir_all(path) {
                Ok(()) => {
                    tracing::info!(
                        workspace = %path.display(),
                        "removed orphan workspace"
                    );
                    return;
                }
                Err(error) if error.raw_os_error() == Some(16) => {
                    tracing::debug!(
                        workspace = %path.display(),
                        attempt,
                        "workspace is still busy, retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(200)).await;
                }
                Err(error) => {
                    tracing::error!(
                        workspace = %path.display(),
                        ?error,
                        "failed to remove orphan workspace"
                    );
                    return;
                }
            }
        }
        tracing::error!(workspace = %path.display(),"workspace is still busy after retries");
    }

    async fn get_workspace_from_container(&self, container_id: &str) -> Result<Option<PathBuf>> {
        let container = self.docker.inspect_container(container_id, None).await?;

        let Some(config) = container.config else {
            return Ok(None);
        };
        let Some(labels) = config.labels else {
            tracing::warn!(container_id = %container_id,"orphan container does not have a label");
            return Ok(None);
        };
        let workspace = labels.get("oj.workspace").map(PathBuf::from);
        // if mount.destination.as_deref() != Some("/workspaces") {
        //     continue;
        // }
        //
        // let workspace_root = PathBuf::from(std::env::var("WORKSPACE_HOST_ROOT")?);
        //
        // let Some(source) = mount.source else { continue };
        // let source = PathBuf::from(source);
        // let workspace = source
        //     .parent()
        //     .ok_or_else(|| anyhow!("workspace has no parent directory"))?
        //     .to_path_buf();
        // if !workspace.starts_with(&workspace_root) {
        //     bail!(
        //         "workspace {} is outside workspace root {}",
        //         workspace.display(),
        //         workspace_root.display()
        //     )
        // }
        Ok(workspace)
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
