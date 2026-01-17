use std::process::Stdio;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::{sleep, Duration};

#[derive(Debug)]
pub enum DaemonError {
    MaxRestartsReached(String),
    ProcessError(String),
}

impl std::fmt::Display for DaemonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DaemonError::MaxRestartsReached(msg) => write!(f, "{}", msg),
            DaemonError::ProcessError(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for DaemonError {}

#[derive(Clone)]
struct ManagedProcess {
    name: String,
    command: String,
    args: Vec<String>,
    child: Arc<Mutex<Option<tokio::process::Child>>>,
    restart_count: Arc<std::sync::atomic::AtomicU32>,
    max_restarts: u32,
    stop_signal: Arc<std::sync::atomic::AtomicBool>,
}

impl ManagedProcess {
    fn new(name: &str, args: Vec<&str>, stop_signal: Arc<std::sync::atomic::AtomicBool>) -> Self {
        let exe = std::env::current_exe()
            .expect("Unable to get the current executable file path")
            .to_string_lossy()
            .to_string();

        Self {
            name: name.to_string(),
            command: exe,
            args: args.iter().map(|s| s.to_string()).collect(),
            child: Arc::new(Mutex::new(None)),
            restart_count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            max_restarts: 5,
            stop_signal,
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

        if let Some(child) = child_guard.as_mut() {
            match child.try_wait() {
                Ok(None) => {
                    tracing::info!("{} server is running", self.name);
                    return Ok(());
                }
                _ => {}
            }
        }

        tracing::info!("starting {} server...", self.name);
        
        let mut cmd = Command::new(&self.command);
        cmd.args(&self.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn()?;

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
        tracing::info!("{} server starting success", self.name);
        
        self.restart_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        Ok(())
    }

    async fn check_and_restart(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.stop_signal.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }

        let mut child_guard = self.child.lock().await;
        
        if let Some(child) = child_guard.as_mut() {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if self.stop_signal.load(std::sync::atomic::Ordering::Relaxed) {
                        return Ok(());
                    }
                    
                    tracing::info!("{} server has stopped, status: {}. Restarting...", self.name, status);
                    *child_guard = None;
                    
                    drop(child_guard);
                    
                    sleep(Duration::from_secs(2)).await;
                    
                    self.start().await?;
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!("Checking {} server status receives error {}", self.name, e);
                }
            }
        } else {
            if self.stop_signal.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(());
            }
            
            tracing::info!("{} server is not running, starting...", self.name);
            drop(child_guard);
            self.start().await?;
        }
        
        Ok(())
    }

    fn get_restart_count(&self) -> u32 {
        self.restart_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn kill_child(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut child_guard = self.child.lock().await;
        if let Some(child) = child_guard.as_mut() {
            child.kill().await?;
            *child_guard = None;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ProcessManager {
    processes: Vec<ManagedProcess>,
    running: Arc<std::sync::atomic::AtomicBool>,
    stop_signal: Arc<std::sync::atomic::AtomicBool>,
}

impl ProcessManager {
    pub fn new() -> Self {
        let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        Self {
            processes: Vec::new(),
            running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            stop_signal: stop_signal.clone(),
        }
    }

    pub fn add_server(&mut self, name: &str, args: Vec<&str>) {
        self.processes.push(ManagedProcess::new(
            name, 
            args, 
            self.stop_signal.clone()
        ));
    }

    pub async fn start_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        for process in &self.processes {
            process.start().await?;
            sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    pub async fn monitor_loop(&self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Monitoring processes every 10 seconds...");
        
        while self.running.load(std::sync::atomic::Ordering::Relaxed) {
            sleep(Duration::from_secs(10)).await;
            
            for process in &self.processes {
                if process.get_restart_count() >= 5 {
                    let error_msg = format!("{} server reached maximum restart limit (5)", process.name);
                    tracing::error!("{}", error_msg);
                    self.stop_signal.store(true, std::sync::atomic::Ordering::Relaxed);
                    return Err(Box::new(DaemonError::MaxRestartsReached(error_msg)));
                }
                
                if let Err(e) = process.check_and_restart().await {
                    tracing::error!("Monitoring {} server, received error {}", process.name, e);
                    if let Some(daemon_error) = e.downcast_ref::<DaemonError>() {
                        match daemon_error {
                            DaemonError::MaxRestartsReached(_) => {
                                self.stop_signal.store(true, std::sync::atomic::Ordering::Relaxed);
                                return Err(Box::new(DaemonError::MaxRestartsReached(
                                    format!("{} server failed with max restarts", process.name)
                                )));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    pub fn stop(&self) {
        self.running.store(false, std::sync::atomic::Ordering::Relaxed);
        self.stop_signal.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub async fn stop_all_servers(&self) -> Result<(), Box<dyn std::error::Error>> {
        for process in &self.processes {
            if let Err(e) = process.kill_child().await {
                tracing::error!("Error killing {} server: {}", process.name, e);
            }
        }
        Ok(())
    }
}

pub async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let lockfile_path = get_lockfile_path();
    if !acquire_lock(&lockfile_path) {
        tracing::error!("Daemon is already running.");
        return Ok(());
    }
    
    tracing::info!("RSQL Daemon is starting.");
    
    let mut process_manager = ProcessManager::new();
    process_manager.add_server("WEB", vec!["web"]);
    process_manager.add_server("SQL", vec!["sql"]);
    
    let manager = Arc::new(process_manager);
    let manager_clone = manager.clone();
    let lockfile_path_clone = lockfile_path.clone();
    
    ctrlc::set_handler(move || {
        tracing::info!("Received stop signal, stopping...");
        manager_clone.stop();
        let _ = std::fs::remove_file(&lockfile_path_clone);
    })?;
    
    if let Err(e) = manager.start_all().await {
        tracing::error!("Failed to start servers: {}", e);
        let _ = std::fs::remove_file(&lockfile_path);
        return Err(e);
    }
    
    let monitor_result = manager.monitor_loop().await;
    
    if let Err(ref e) = monitor_result {
        if let Some(daemon_error) = e.downcast_ref::<DaemonError>() {
            match daemon_error {
                DaemonError::MaxRestartsReached(msg) => {
                    tracing::error!("CRITICAL: {}", msg);
                    tracing::error!("Daemon is exiting due to critical error.");
                    
                    if let Err(kill_err) = manager.stop_all_servers().await {
                        tracing::error!("Error stopping servers: {}", kill_err);
                    }
                    
                    let _ = std::fs::remove_file(&lockfile_path);
                    
                    return Err(Box::new(DaemonError::MaxRestartsReached(
                        format!("Daemon stopped due to: {}", msg)
                    )));
                }
                _ => {}
            }
        }
    }
    
    if let Err(e) = manager.stop_all_servers().await {
        tracing::error!("Error stopping servers: {}", e);
    }
    
    let _ = std::fs::remove_file(&lockfile_path);
    
    tracing::info!("Daemon process is stopped.");
    
    monitor_result
}

fn acquire_lock(lockfile_path: &std::path::Path) -> bool {
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(lockfile_path)
    {
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if let Ok(content) = std::fs::read_to_string(lockfile_path) {
                if let Ok(pid) = content.trim().parse::<u32>() {
                    if is_process_running(pid) {
                        return false;
                    }
                }
            }
            
            let _ = std::fs::write(lockfile_path, std::process::id().to_string());
            true
        }
        Err(_) => false,
    }
}

fn is_process_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        
        match kill(Pid::from_raw(pid as i32), None) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    
    #[cfg(windows)]
    {
        use std::process::Command;
        
        let output = Command::new("tasklist")
            .arg("/FI")
            .arg(format!("PID eq {}", pid))
            .output();
        
        match output {
            Ok(output) => {
                let output_str = String::from_utf8_lossy(&output.stdout);
                output_str.contains(&pid.to_string())
            }
            Err(_) => false,
        }
    }
}

fn get_lockfile_path() -> std::path::PathBuf {
    #[cfg(unix)]
    {
        std::path::PathBuf::from("/tmp/rsql-daemon.lock")
    }
    
    #[cfg(windows)]
    {
        std::env::temp_dir().join("rsql-daemon.lock")
    }
}