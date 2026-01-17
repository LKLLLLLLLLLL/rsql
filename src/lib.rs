use tracing_subscriber::prelude::*;
use std::fs;
use std::path;

mod config;
mod server;
mod execution;
mod common;
mod sql;
mod storage;
mod catalog;
mod transaction;
mod utils;

pub fn init_log() {
    let log_dir = path::Path::new(config::LOG_PATH).parent().unwrap();
    let log_filename = path::Path::new(config::LOG_PATH).file_name().unwrap().to_str().unwrap();
    fs::create_dir_all(log_dir).unwrap();

    let stdout_log = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .with_thread_names(true)
        .with_level(true);

    let file_appender = tracing_appender::rolling::daily(log_dir, log_filename);
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    Box::leak(Box::new(_guard));

    let file_log = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_thread_names(true)
        .with_level(true);

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(config::LOG_LEVEL))
        .with(stdout_log)
        .with(file_log)
        .init();
}

pub fn run() {
    init_log();
    server::daemon::daemon();
}
