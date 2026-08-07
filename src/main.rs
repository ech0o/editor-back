mod app;
mod docker;
mod routes;

mod models;
mod state;
mod workspace;

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let app = app::create_app().await?;

    let listener = TcpListener::bind("0.0.0.0:4000").await?;

    axum::serve(listener, app).await?;

    Ok(())
}