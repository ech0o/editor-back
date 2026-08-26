mod app;
mod docker;
mod routes;

mod apierror;
mod db;
mod error;
mod job;
mod kafka;
mod models;
mod state;
mod workspace;
mod metrics;

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    app::create_app().await?;

    Ok(())
}

// async fn shutdown_signal() {
//     tokio::signal::ctrl_c()
//         .await
//         .expect("failed to install CTRL+C signal handler");
// }
