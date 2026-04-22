mod app;
mod config;
mod domain;
mod error;
mod exchange;
mod service;
mod store;
mod web;

use std::{fs, path::{Path, PathBuf}};

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

const LOG_DIR: &str = "logs";
const LOG_FILE_NAME: &str = "perp-data-moin.log";
const RETAIN_LOG_FILES: usize = 7;

#[tokio::main]
async fn main() -> Result<(), error::AppError> {
    let _log_guard = init_logging()?;

    app::run().await
}

fn init_logging() -> Result<WorkerGuard, std::io::Error> {
    let log_dir = Path::new(LOG_DIR);
    fs::create_dir_all(log_dir)?;
    prune_old_log_files(log_dir, LOG_FILE_NAME, RETAIN_LOG_FILES)?;

    let file_appender = tracing_appender::rolling::daily(log_dir, LOG_FILE_NAME);
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let console_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .compact();
    let file_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(false)
        .compact()
        .with_writer(file_writer);

    tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(file_layer)
        .init();

    Ok(guard)
}

fn prune_old_log_files(log_dir: &Path, file_name: &str, retain: usize) -> Result<(), std::io::Error> {
    if retain == 0 {
        return Ok(());
    }

    let prefix = format!("{file_name}.");
    let mut dated_logs: Vec<(String, PathBuf)> = Vec::new();

    for entry in fs::read_dir(log_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if let Some(date_suffix) = file_name.strip_prefix(&prefix) {
            if is_daily_suffix(date_suffix) {
                dated_logs.push((date_suffix.to_owned(), entry.path()));
            }
        }
    }

    dated_logs.sort_by(|left, right| left.0.cmp(&right.0));

    if dated_logs.len() > retain {
        let remove_count = dated_logs.len() - retain;
        for (_, path) in dated_logs.into_iter().take(remove_count) {
            fs::remove_file(path)?;
        }
    }

    Ok(())
}

fn is_daily_suffix(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 10 {
        return false;
    }

    bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}
