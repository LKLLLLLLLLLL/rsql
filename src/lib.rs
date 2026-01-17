use tracing_subscriber::prelude::*;
use std::fs;
use std::path;
use tracing::info;
use std::env;

mod daemon;
mod config;
mod web_server;
mod db;

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
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(stdout_log)
        .with(file_log)
        .init();
}

pub fn run() {
    let args: Vec<String> = env::args().collect();
    
    init_log();
    
    if args.len() >= 2 {
        match args[1].as_str() {
            "web" => {
                info!("RSQL Web Server starting...");
                if let Err(e) = actix_web::rt::System::new().block_on(web_server::start_server()) {
                    tracing::error!("Web Server starting failed: {:?}", e);
                    std::process::exit(1);
                }
                info!("RSQL Web Server stopped.");
                return;
            }
            "sql" => {
                info!("RSQL SQL Server starting...");
                if let Err(e) = actix_web::rt::System::new().block_on(db::server::server::start_server()) {
                    tracing::error!("SQL Server starting failed: {:?}", e);
                    std::process::exit(1);
                }
                info!("RSQL SQL Server stopped.");
                return;
            }
            "daemon" => {
                info!("RSQL Daemon starting...");
                start_daemon_mode();
                return;
            }
            _ => {
                tracing::warn!("Unknown arguments '{}', daemon process will be started.", args[1]);
            }
        }
    }
    
    info!("RSQL Daemon process starting...(default)");
    start_daemon_mode();
}

fn start_daemon_mode() {
    info!("log file path: {}", config::LOG_PATH);
    
    if let Err(e) = actix_web::rt::System::new().block_on(daemon::start()) {
        tracing::error!("Daemon process starting failed: {:?}", e);
        std::process::exit(1);
    }
    info!("RSQL Daemon process stopped.");
}
