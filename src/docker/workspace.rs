use anyhow::Result;
use std::{env, fs, path::PathBuf};
use std::path::Path;
use uuid::Uuid;

pub fn create_workspace()->Result<PathBuf>{
    let temp_dir = env::temp_dir().join(format!("runner-{}", Uuid::new_v4()));
    fs::create_dir_all(&temp_dir)?;
    Ok(temp_dir)
}

pub fn write_source(dir:&Path,filename:&str,code:&str)->Result<()>{
    fs::write(dir.join(filename), code)?;
    Ok(())
}