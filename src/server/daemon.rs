use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::{sleep, Duration};
use std::sync::Arc;
use std::process::Stdio;
use std::env;
use tracing::info;

use crate::config;
use super::server;
use crate::config::{MAX_RESTART_INTERVAL, MAX_RESTART_TIMES, MONITOR_TERM};

#[derive(Debug)]
pub enum DaemonError {
    MaxRestartsReached(String),
    ProcessError(String),
    PortConflict(String),
    InternalError(String),
}

impl std::fmt::Display for DaemonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DaemonError::MaxRestartsReached(msg) => write!(f, "[Daemon] {}", msg),
            DaemonError::ProcessError(msg) => write!(f, "[Daemon] {}", msg),
            DaemonError::PortConflict(msg) => write!(f, "[Daemon] Port conflict: {}", msg),
            DaemonError::InternalError(msg) => write!(f, "[Daemon] Internal error: {}", msg),
        }
    }
}

impl std::error::Error for DaemonError {}

struct ManagedProcess {
    name: String,
    command: String,
    args: Vec<String>,
    child: Arc<Mutex<Option<tokio::process::Child>>>,
    restart_count: Arc<std::sync::atomic::AtomicU32>,
    max_restarts: u32,
    stop_signal: Arc<std::sync::atomic::AtomicBool>,
    port: Option<u16>,
}

impl ManagedProcess {
    fn new(name: &str, args: Vec<&str>, stop_signal: Arc<std::sync::atomic::AtomicBool>, port: Option<u16>) -> Self {
        let exe = std::env::current_exe()
            .expect("[Daemon] Unable to get the current executable file path")
            .to_string_lossy()
            .to_string();

        Self {
            name: name.to_string(),
            command: exe,
            args: args.iter().map(|s| s.to_string()).collect(),
            child: Arc::new(Mutex::new(None)),
            restart_count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            max_restarts: MAX_RESTART_TIMES,
            stop_signal,
            port,
        }
    }

    fn check_port_available(&self) -> bool {
        if let Some(port) = self.port {
            use std::net::TcpListener;
            match TcpListener::bind(("127.0.0.1", port)) {
                Ok(_) => true, 
                Err(_) => false, 
            }
        } else {
            true 
        }
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut child_guard = self.child.lock().await;

        if self.stop_signal.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Box::new(DaemonError::ProcessError(
                format!("{} server stop signal received", self.name)
            )));
        }

        let current_restarts = self.restart_count.load(std::sync::atomic::Ordering::Relaxed);
        if current_restarts >= self.max_restarts {
            let error_msg = format!("{} server has reached the max restart time ({})", 
                     self.name, self.max_restarts);
            tracing::error!("{}", error_msg);
            return Err(Box::new(DaemonError::MaxRestartsReached(error_msg)));
        }

        if !self.check_port_available() {
            let error_msg = format!("Port conflict for {} server", self.name);
            tracing::error!("{}", error_msg);
            return Err(Box::new(DaemonError::PortConflict(error_msg)));
        }

        if let Some(child) = child_guard.as_mut() {
            if let Ok(None) = child.try_wait() {
                tracing::info!("{} server is already running", self.name);
                return Ok(());
            }
        }

        tracing::info!("Starting {} server...", self.name);
        
        let mut cmd = Command::new(&self.command);
        cmd.args(&self.args)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);

        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                if e.to_string().contains("address already in use") {
                    let error_msg = format!("Port conflict for {} server: {}", self.name, e);
                    return Err(Box::new(DaemonError::PortConflict(error_msg)));
                }
                return Err(Box::new(DaemonError::InternalError(e.to_string())));
            }
        };

        let name_std = self.name.clone();
        if let Some(stdout) = child.stdout.take() {
            tokio::spawn(async move {
                let mut reader = BufReader::new(stdout);
                let mut line = String::new();
                while reader.read_line(&mut line).await.is_ok() {
                    if !line.is_empty() {
                        print!("[{}] {}", name_std, line);
                        line.clear();
                    }
                }
            });
        }

        let name_err = self.name.clone();
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr);
                let mut line = String::new();
                while reader.read_line(&mut line).await.is_ok() {
                    if !line.is_empty() {
                        eprint!("[{} ERROR] {}", name_err, line);
                        line.clear();
                    }
                }
            });
        }

        *child_guard = Some(child);
        tracing::info!("{} server started successfully", self.name);
        
        self.restart_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        Ok(())
    }

    async fn check_and_restart(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.stop_signal.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }

        let mut child_guard = self.child.lock().await;
        
        match child_guard.as_mut() {
            Some(child) => {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        if !self.stop_signal.load(std::sync::atomic::Ordering::Relaxed) {
                            tracing::info!("{} server has stopped, status: {}. Restarting...", 
                                         self.name, status);
                            *child_guard = None;
                            
                            drop(child_guard);
                            
                            sleep(Duration::from_secs(MAX_RESTART_INTERVAL)).await;
                            
                            self.start().await?;
                        }
                    }
                    Ok(None) => {

                    }
                    Err(e) => {
                        tracing::error!("Error checking {} server status: {}", self.name, e);
                    }
                }
            }
            None => {
                if !self.stop_signal.load(std::sync::atomic::Ordering::Relaxed) {
                    drop(child_guard);
                    self.start().await?;
                }
            }
        }
        
        Ok(())
    }

    fn get_restart_count(&self) -> u32 {
        self.restart_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn stop(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut child_guard = self.child.lock().await;
        if let Some(child) = child_guard.as_mut() {
            if let Err(e) = child.start_kill() {
                tracing::warn!("Failed to send kill signal to {}: {}", self.name, e);
            }
            
            let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
            
            *child_guard = None;
        }
        Ok(())
    }
}

pub struct ProcessManager {
    sql_process: ManagedProcess,
    web_process: ManagedProcess,
    running: Arc<std::sync::atomic::AtomicBool>,
    stop_signal: Arc<std::sync::atomic::AtomicBool>,
}

impl ProcessManager {
    pub fn new(sql_port: Option<u16>, web_port: Option<u16>) -> Self {
        let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        Self {
            sql_process: ManagedProcess::new("SQL", vec!["sql"], stop_signal.clone(), sql_port),
            web_process: ManagedProcess::new("WEB", vec!["web"], stop_signal.clone(), web_port),
            running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            stop_signal,
        }
    }

    pub async fn start_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Starting all servers...");
        
        self.sql_process.start().await?;
        
        sleep(Duration::from_secs(2)).await;
        
        self.web_process.start().await?;
        
        Ok(())
    }

    pub async fn monitor(&self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Starting monitor loop...");
        
        while self.running.load(std::sync::atomic::Ordering::Relaxed) {
            if let Err(e) = self.sql_process.check_and_restart().await {
                tracing::error!("Monitor SQL server error: {}", e);
                
                if self.sql_process.get_restart_count() >= MAX_RESTART_TIMES {
                    let error_msg = format!("SQL server reached maximum restart limit ({})", MAX_RESTART_TIMES);
                    self.stop_signal.store(true, std::sync::atomic::Ordering::Relaxed);
                    return Err(Box::new(DaemonError::MaxRestartsReached(error_msg)));
                }
            }
            
            if let Err(e) = self.web_process.check_and_restart().await {
                tracing::error!("Monitor WEB server error: {}", e);
                
                if self.web_process.get_restart_count() >= MAX_RESTART_TIMES {
                    let error_msg = format!("WEB server reached maximum restart limit ({})", MAX_RESTART_TIMES);
                    self.stop_signal.store(true, std::sync::atomic::Ordering::Relaxed);
                    return Err(Box::new(DaemonError::MaxRestartsReached(error_msg)));
                }
            }
            
            sleep(Duration::from_secs(MONITOR_TERM)).await;
        }
        
        Ok(())
    }
    
    pub fn stop(&self) {
        self.running.store(false, std::sync::atomic::Ordering::Relaxed);
        self.stop_signal.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub async fn stop_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Stopping all servers...");
        
        if let Err(e) = self.sql_process.stop().await {
            tracing::error!("Error stopping SQL server: {}", e);
        }
        
        if let Err(e) = self.web_process.stop().await {
            tracing::error!("Error stopping WEB server: {}", e);
        }
        
        sleep(Duration::from_secs(1)).await;
        
        Ok(())
    }
}

pub async fn start_daemon() -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("RSQL Daemon starting...");
    
    let sql_port = config::PORT;
    let web_port = config::WEB_PORT;
    
    let manager = ProcessManager::new(Some(sql_port), Some(web_port));
    
    let manager_arc = Arc::new(tokio::sync::Mutex::new(manager));
    let manager_clone = manager_arc.clone();
    
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            tracing::info!("Received Ctrl+C signal, stopping daemon...");
            let manager = manager_clone.lock().await;
            manager.stop();
            if let Err(e) = manager.stop_all().await {
                tracing::error!("Error stopping servers: {}", e);
            }
            std::process::exit(0);
        }
    });
    
    manager_arc.lock().await.start_all().await?;
    
    let result = manager_arc.lock().await.monitor().await;
    
    if let Err(ref e) = result {
        tracing::error!("Daemon monitoring error: {}", e);
    }
    
    tracing::info!("RSQL Daemon stopped.");
    
    result
}

pub fn daemon(){
    let args: Vec<String> = env::args().collect();
    
    match args.get(1).map(|s| s.as_str()) {
        Some("web") => {
            info!("RSQL Web Server starting...");
            if let Err(e) = actix_web::rt::System::new().block_on(web_server::start_server()) {
                tracing::error!("Web Server starting failed: {:?}", e);
                std::process::exit(1);
            }
            info!("RSQL Web Server stopped.");
        }
        Some("sql") => {
            info!("RSQL SQL Server starting...");
            if let Err(e) = actix_web::rt::System::new().block_on(server::start_server()) {
                tracing::error!("SQL Server starting failed: {:?}", e);
                std::process::exit(1);
            }
            info!("RSQL SQL Server stopped.");
        }
        Some("daemon") | None => {
            info!("RSQL Daemon starting...");
            info!("Log file path: {}", config::LOG_PATH);
            
            if let Err(e) = actix_web::rt::System::new().block_on(start_daemon()) {
                tracing::error!("Daemon process starting failed: {:?}", e);
                std::process::exit(1);
            }
            info!("RSQL service stopped.");
        }
        _ => {
            tracing::warn!("Unknown argument '{}', starting daemon process.", args[1]);
            info!("RSQL Daemon starting...(default)");
            info!("Log file path: {}", config::LOG_PATH);
            
            if let Err(e) = actix_web::rt::System::new().block_on(start_daemon()) {
                tracing::error!("Daemon process starting failed: {:?}", e);
                std::process::exit(1);
            }
            info!("RSQL service stopped.");
        }
    }
}