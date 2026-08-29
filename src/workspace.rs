use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Result, anyhow};
use tempfile::{Builder, TempDir};
use uuid::Uuid;

pub struct Workspace {
    host_path: PathBuf,
}

impl Workspace {
    pub fn new(worker_id: &str) -> Result<Self> {
        // let dir = Builder::new()
        //     .prefix("workspace-")
        //     .tempdir_in("/workspaces")?;
        // let name = dir
        //     .path()
        //     .file_name()
        //     .ok_or_else(|| anyhow!("invalid workspace path"))?;
        let host_root = std::env::var("WORKSPACE_HOST_ROOT")?;
        // let host_path = PathBuf::from(host_root).join(name);
        let worker_dir = PathBuf::from(host_root).join(worker_id);
        fs::create_dir_all(&worker_dir)?;
        let path = worker_dir.join(Uuid::new_v4().to_string());
        fs::create_dir(&path)?;
        Ok(Self { host_path: path })
    }

    // pub fn path(&self) -> &Path {
    //     &self.dir.path()
    // }

    pub fn host_path(&self) -> &Path {
        &self.host_path
    }

    pub fn write(&self, filename: &str, code: &str) -> Result<()> {
        fs::write(self.host_path().join(filename), code)?;
        Ok(())
    }

    pub fn cleanup_orphan(worker_id: &str) -> anyhow::Result<()> {
        let host_root = std::env::var("WORKSPACE_HOST_ROOT")?;
        let working_dir = PathBuf::from(host_root).join(worker_id);
        if !working_dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(working_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                tracing::warn!(path = %path.display(), "removing orphan workspace");

                if let Err(e) = fs::remove_dir_all(&path) {
                    tracing::error!(path = %path.display(),
                        error = %e,
                        "failed to remove orphan workspace");
                }
            }
        }
        Ok(())
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        if let Err(err) = fs::remove_dir_all(&self.host_path()) {
            tracing::warn!(error = %err, "failed to remove workspace");
        };
    }
}
