use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use uuid::Uuid;

pub struct Workspace {
    path: PathBuf,
}

impl Workspace {
    pub fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!("runner-{}", Uuid::new_v4()));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write(&self, filename: &str, code: &str) -> Result<()> {
        fs::write(self.path.join(filename), code)?;
        Ok(())
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
