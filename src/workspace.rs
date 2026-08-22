use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Result, anyhow};
use tempfile::{Builder, TempDir};
use uuid::Uuid;

pub struct Workspace {
    dir: TempDir,
    host_path: PathBuf,
}

impl Workspace {
    pub fn new() -> Result<Self> {
        // let path = std::env::temp_dir().join(format!("runner-{}", Uuid::new_v4()));
        // fs::create_dir_all(&path)?;
        let dir = Builder::new()
            .prefix("workspace-")
            .tempdir_in("/workspaces")?;
        let name = dir
            .path()
            .file_name()
            .ok_or_else(|| anyhow!("invalid workspace path"))?;
        let host_root = std::env::var("WORKSPACE_HOST_ROOT")?;
        let host_path = PathBuf::from(host_root).join(name);
        Ok(Self { dir, host_path })
    }

    pub fn path(&self) -> &Path {
        &self.dir.path()
    }

    pub fn host_path(&self) -> &Path {
        &self.host_path
    }

    pub fn write(&self, filename: &str, code: &str) -> Result<()> {
        fs::write(self.dir.path().join(filename), code)?;
        Ok(())
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir.path());
    }
}
