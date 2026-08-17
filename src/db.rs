use sqlx::SqlitePool;
use uuid::Uuid;
use crate::job::Job;

#[derive(Clone)]
pub struct Database {
    pub pool: SqlitePool,
}

impl Database {
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let pool = SqlitePool::connect(&url).await?;
        Ok(Self { pool })
    }

    

   
}
