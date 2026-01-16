use super::errors::RsqlResult;
use std::thread;
use tracing::info;

pub fn execute(sql: &str, connection_id: u64) -> RsqlResult<()> {
    info!("Executing SQL: {}", sql);
    thread::sleep(std::time::Duration::from_secs(1));
    info!("Parsing SQL...");
    info!("Openning transaction...");
    info!("Executing query plan...");
    if sql.to_lowercase().contains("commit") {
        info!("Committing transaction...");
    } else {
        info!("Rolling back transaction...");
    }
    info!("SQL executed successfully.");
    Ok(())
}