mod app;
mod config;
mod domain;
mod error;
mod exchange;
mod service;
mod store;
mod web;

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), error::AppError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    app::run().await
}
