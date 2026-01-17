use tracing::info;

use crate::config::WEB_PORT;

pub async fn start_server() -> std::io::Result<()>{
    info!("Starting Web Server on port {}", WEB_PORT);
    Ok(())
}