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
    dir: TempDir,
    host_path: PathBuf,
    root_path: PathBuf,
}

impl Workspace {
    pub fn new(worker_id: &str) -> Result<Self> {
        // let host_root = std::env::var("WORKSPACE_HOST_ROOT")?;
        // let worker_dir = PathBuf::from(host_root).join(worker_id);
        // fs::create_dir_all(&worker_dir)?;
        // let path = worker_dir.join(Uuid::new_v4().to_string());
        // fs::create_dir(&path)?;
        // tracing::info!("Workspace directory created at {:?}", path);
        // Ok(Self { host_path: path })
        let path = format!("/workspaces/{}", worker_id);
        if !Path::new(&path).is_dir() {
            DirBuilder::new()
                .recursive(true)
                .create(&path)?;
        }
        let dir = Builder::new().tempdir_in(path)?;
        let name = dir
            .path()
            .file_name()
            .ok_or_else(|| anyhow!("invalid workspace path"))?;
        tracing::info!("workspace name: {:?}", name);
        let host_root = std::env::var("WORKSPACE_HOST_ROOT")?;
        let host_path = PathBuf::from(host_root.clone()).join(worker_id).join(name);
        let root_path = PathBuf::from("/workspaces").join(worker_id);
        Ok(Self { dir, host_path, root_path })
    }

    pub fn path(&self) -> &Path {
        &self.dir.path()
    }

    pub fn host_path(&self) -> &Path {
        &self.host_path
    }

    pub fn write(&self, filename: &str, code: &str) -> Result<()> {
        tracing::info!("Writing to {:?}", self.path().join(filename));
        fs::write(self.dir.path().join(filename), code)?;
        // let dir = fs::read_dir(self.host_path())?;
        // let mut content = String::new();
        // let mut file = File::open(self.host_path().join(filename))?;
        // file.read_to_string(&mut content)?;
        // tracing::info!("code content {:?}", content);
        Ok(())
    }

    // pub fn cleanup_orphan(worker_id: &str) -> anyhow::Result<()> {
    //     let host_root = std::env::var("WORKSPACE_HOST_ROOT")?;
    //     let working_dir = PathBuf::from(host_root).join(worker_id);
    //     if !working_dir.exists() {
    //         return Ok(());
    //     }
    //     for entry in fs::read_dir(working_dir)? {
    //         let entry = entry?;
    //         let path = entry.path();
    //
    //         if path.is_dir() {
    //             tracing::warn!(path = %path.display(), "removing orphan workspace");
    //
    //             if let Err(e) = fs::remove_dir_all(&path) {
    //                 tracing::error!(path = %path.display(),
    //                     error = %e,
    //                     "failed to remove orphan workspace");
    //             }
    //         }
    //     }
    //     Ok(())
    // }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        tracing::info!("Cleaning up:{:?}",self.root_path);
        if let Err(err) = fs::remove_dir_all(&self.root_path) {
            tracing::warn!(error = %err, "failed to remove workspace");
        };
    }
}
