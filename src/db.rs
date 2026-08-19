use sqlx::PgPool;
use uuid::Uuid;
use crate::job::Job;

#[derive(Clone)]
pub struct Database {
    pub pool: PgPool,
}

impl Database {
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(&url).await?;
        Ok(Self { pool })
    }


   
}
