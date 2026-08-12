mod app;
mod docker;
mod routes;

mod models;
mod state;
mod workspace;
mod apierror;
mod error;

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let app = app::create_app().await?;

    let listener = TcpListener::bind("0.0.0.0:4000").await?;
    tracing::info!("Listening on http://0.0.0.0:4000");

    axum::serve(listener, app).await?;

    Ok(())
}