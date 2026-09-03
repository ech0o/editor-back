use std::{
    fs,
    path::PathBuf,
};
use tempfile::Builder;

pub struct WorkerWorkspace {
    root_path: PathBuf,
    // host_path: PathBuf,
}

impl WorkerWorkspace {
    pub fn new(worker_id: &str) -> anyhow::Result<Self> {
        let root_path = PathBuf::from("workspace").join(worker_id);

        fs::create_dir_all(&root_path)?;
        // let dir = Builder::new().tempdir_in(root_path)?;
        // let host_root = std::env::var("WORKSPACE_HOST_ROOT")?;
        // let host_path = PathBuf::from(host_root.clone()).join(worker_id).join(name);
        // let root_path = PathBuf::from("/workspaces").join(worker_id);

        Ok(Self { root_path })
    }

    pub fn path(&self) -> &std::path::Path {
        &self.root_path
    }

    // pub fn host_path(&self) -> &std::path::Path {
    //     &self.host_path
    // }
}

impl Drop for WorkerWorkspace {
    fn drop(&mut self) {
        tracing::info!(
            path = ?self.root_path,
            "cleaning up worker workspace"
        );

        if let Err(err) = fs::remove_dir_all(&self.root_path) {
            tracing::warn!(
                error = %err,
                path = ?self.root_path,
                "failed to remove worker workspace"
            );
        }
    }
}