use anyhow::{Result, anyhow};
use std::fs::{DirBuilder, File};
use std::io::Read;
use std::{
    fs,
    path::{Path, PathBuf},
};
use tempfile::{Builder, TempDir};
use uuid::Uuid;

pub struct Workspace {
    // dir: TempDir,
    host_path: PathBuf,
    path: PathBuf,
}

impl Workspace {
    // pub fn new(worker_id: &str) -> Result<Self> {
    //     let path = format!("/workspaces/{}", worker_id);
    //     if !Path::new(&path).is_dir() {
    //         DirBuilder::new().recursive(true).create(&path)?;
    //     }
    //     let dir = Builder::new().tempdir_in(path)?;
    //     let name = dir
    //         .path()
    //         .file_name()
    //         .ok_or_else(|| anyhow!("invalid workspace path"))?;
    //     tracing::info!("workspace name: {:?}", name);
    //     let host_root = std::env::var("WORKSPACE_HOST_ROOT")?;
    //     let host_path = PathBuf::from(host_root.clone()).join(worker_id).join(name);
    //     let root_path = PathBuf::from("/workspaces").join(worker_id);
    //     Ok(Self {
    //         dir,
    //         host_path,
    //         root_path,
    //     })
    // }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn host_path(&self) -> &Path {
        &self.host_path
    }

    pub fn write(&self, filename: &str, code: &str) -> Result<()> {
        tracing::info!("Writing to {:?}", self.path.join(filename));
        fs::write(self.path.join(filename), code)?;
        // let dir = fs::read_dir(self.host_path())?;
        // let mut content = String::new();
        // let mut file = File::open(self.host_path().join(filename))?;
        // file.read_to_string(&mut content)?;
        // tracing::info!("code content {:?}", content);
        Ok(())
    }

    pub fn from(path: PathBuf) -> Result<Self> {
        tracing::info!("job Workspace path: {:?}", path);
        let host_root = std::env::var("WORKSPACE_HOST_ROOT")?;
        let host_path =
            PathBuf::from(host_root.clone()).join(path.clone().strip_prefix("/").unwrap_or(&path));
        tracing::info!("job Workspace host path: {:?}", host_path);
        Ok(Self { path, host_path })
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        tracing::info!("Cleaning up: {:?}", self.path);
        if let Err(err) = fs::remove_dir_all(&self.path) {
            tracing::warn!(error = %err, "failed to remove workspace");
        };
    }
}
