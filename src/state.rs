use std::sync::Arc;

use bollard::Docker;

use crate::docker::DockerRunner;

#[derive(Clone)]
pub struct AppState {
    pub runner: DockerRunner,
}

impl AppState {
    pub fn new(docker: Docker) -> Self {
        Self {
            runner: DockerRunner::new(Arc::new(docker)),
        }
    }
}