use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

pub enum ApiError {
    Internal(anyhow::Error),
    InvalidJson,
    PayloadTooLarge,
    TooManyRequests,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::Internal(err) => {
                tracing::error!(
                    error = %err,
                    "internal server error"
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: String::from("internal server error"),
                    }),
                )
                    .into_response()
            }
            ApiError::InvalidJson => (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: String::from("invalid_json"),
                }),
            )
                .into_response(),
            ApiError::PayloadTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorResponse {
                    error: String::from("payload_too_large"),
                }),
            )
                .into_response(),
            ApiError::TooManyRequests => (
                StatusCode::TOO_MANY_REQUESTS,
                Json(ErrorResponse {
                    error: String::from("too_many_requests"),
                }),
            )
                .into_response(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}
