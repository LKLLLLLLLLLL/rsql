use thiserror::Error;

#[derive(Error, Debug)]
pub enum RsqlError {
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parser Error: {0}")]
    ParserError(String),

    #[error("Execution Error: {0}")]
    ExecutionError(String),

    #[error("WAL Error: {0}")]
    WalError(String),

    #[error("Storage Error: {0}")]
    StorageError(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Failed to acquire lock: {0}")]
    LockError(String),

    #[error("Unknown Error: {0}")]
    Unknown(String),
}
