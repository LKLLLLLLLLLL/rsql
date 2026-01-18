use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::config::{MAX_RESTART_TIMES, RESTART_DELAY_SECS};

/// Daemon error types
#[derive(Debug)]
pub enum DaemonError {
    ProcessError(String),
    MaxRestartsReached(u32),
    PortConflict(u16),
}

impl std::fmt::Display for DaemonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DaemonError::ProcessError(msg) => write!(f, "Process error: {}", msg),
            DaemonError::MaxRestartsReached(max) => write!(f, "Maximum restart attempts reached: {}", max),
            DaemonError::PortConflict(port) => write!(f, "Port conflict: {}", port),
        }
    }
}

impl std::error::Error for DaemonError {}

/// Initialize tracing
fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    
    let filter = EnvFilter::from_default_env()
        .add_directive("info".parse().unwrap());

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(false)
        .with_line_number(false)
        .with_ansi(true)
        .compact()
        .try_init();
}

/// Check if port is available
fn check_port_available(port: u16) -> bool {
    match std::net::TcpListener::bind(("127.0.0.1", port)) {
        Ok(_) => true,
        Err(e) => {
            warn!("Port {} check failed: {}", port, e);
            false
        }
    }
}

/// Start SQL server process
fn start_sql_server(port: u16) -> Result<std::process::Child, DaemonError> {
    if !check_port_available(port) {
        return Err(DaemonError::PortConflict(port));
    }

    let exe_path = std::env::current_exe()
        .map_err(|e| DaemonError::ProcessError(format!("Failed to get executable path: {}", e)))?;

    info!("Starting SQL server process on port: {}", port);
    
    let child = Command::new(exe_path)
        .arg("sql")
        .env("PORT", port.to_string())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| DaemonError::ProcessError(format!("Failed to start process: {}", e)))?;

    Ok(child)
}

/// Global running flag
static GLOBAL_RUNNING: AtomicBool = AtomicBool::new(true);

/// Setup signal handler for Ctrl+C
fn setup_signal_handler() {
    ctrlc::set_handler(|| {
        info!("Received termination signal, stopping daemon...");
        GLOBAL_RUNNING.store(false, Ordering::SeqCst);
    })
    .expect("Failed to set Ctrl+C handler");
}

/// Main daemon function
pub fn run_daemon(port: u16) -> Result<(), DaemonError> {
    init_tracing();
    info!("RSQL Daemon starting, monitoring port: {}", port);
    
    setup_signal_handler();

    let mut restart_count = 0;
    let mut child_process: Option<std::process::Child> = None;

    while GLOBAL_RUNNING.load(Ordering::SeqCst) {
        let need_restart = if let Some(child) = &mut child_process {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if status.success() {
                        info!("SQL server process exited normally");
                        false
                    } else {
                        error!("SQL server process exited abnormally, status code: {:?}", status.code());
                        true
                    }
                }
                Ok(None) => false,
                Err(e) => {
                    error!("Failed to check process status: {}", e);
                    true
                }
            }
        } else {
            true
        };

        if need_restart {
            if let Some(mut child) = child_process.take() {
                let _ = child.kill();
                let _ = child.wait();
            }

            if restart_count >= MAX_RESTART_TIMES {
                return Err(DaemonError::MaxRestartsReached(MAX_RESTART_TIMES));
            }

            match start_sql_server(port) {
                Ok(child) => {
                    child_process = Some(child);
                    restart_count += 1;
                    
                    if restart_count > 1 {
                        info!("SQL server restarted {} times", restart_count - 1);
                    }
                    
                    thread::sleep(Duration::from_secs(2));
                }
                Err(e) => {
                    error!("Failed to start SQL server: {}", e);
                    
                    if matches!(e, DaemonError::PortConflict(_)) {
                        return Err(e);
                    }
                    
                    if GLOBAL_RUNNING.load(Ordering::SeqCst) {
                        info!("Waiting {} seconds before retry...", RESTART_DELAY_SECS);
                        thread::sleep(Duration::from_secs(RESTART_DELAY_SECS as u64));
                    }
                }
            }
        }

        if GLOBAL_RUNNING.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_secs(1));
        }
    }

    if let Some(mut child) = child_process.take() {
        info!("Stopping SQL server process...");
        thread::sleep(Duration::from_secs(1));
        let _ = child.kill();
        let _ = child.wait();
    }

    info!("Daemon stopped");
    Ok(())
}

/// SQL server entry function
fn run_sql_server() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    info!("RSQL SQL Server starting...");
    
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or_else(|| {
            info!("PORT environment variable not set, using default port");
            crate::config::PORT
        });
    
    info!("SQL Server listening on port: {}", port);
    
    if let Err(e) = actix_web::rt::System::new().block_on(crate::server::server::start_server()) {
        error!("SQL Server failed to start: {:?}", e);
        std::process::exit(1);
    }
    
    Ok(())
}

/// Main entry function
pub fn daemon() {
    let args: Vec<String> = std::env::args().collect();
    
    match args.get(1).map(|s| s.as_str()) {
        Some("sql") => {
            if let Err(e) = run_sql_server() {
                error!("SQL Server error: {}", e);
                std::process::exit(1);
            }
        }
        Some("daemon") | None => {
            let port = crate::config::PORT;
            
            if let Err(e) = run_daemon(port) {
                error!("Daemon process failed: {}", e);
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("Unknown argument: {}", args[1]);
            println!("Usage:");
            println!("  {} [daemon] - Start daemon process", args[0]);
            println!("  {} sql     - Start SQL server directly", args[0]);
            std::process::exit(1);
        }
    }
}