use serde::Serialize;
use crate::error::ProcessError::{Permanent, Retryable};

pub struct ApiError(anyhow::Error);

#[derive(thiserror::Error, Debug)]
pub enum ProcessError {
    #[error("retryable error: {0}")]
    Retryable(anyhow::Error),
    #[error("permanent error: {0}")]
    Permanent(anyhow::Error),
}

// impl From<anyhow::Error> for ProcessError {
//     fn from(e: anyhow::Error) -> Self {
//         ProcessError::Permanent(e)
//     }
// }
