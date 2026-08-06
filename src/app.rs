use axum::Router;
use bollard::Docker;

use crate::{routes, state::AppState};

pub async fn create_app() -> anyhow::Result<Router> {
    let docker = Docker::connect_with_local_defaults()?;

    Ok(Router::new()
        .merge(routes::router())
        .with_state(AppState::new(docker)))
}