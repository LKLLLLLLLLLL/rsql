//! This module defines custom error types for rsql database.
use thiserror::Error;

pub type RsqlResult<T> = std::result::Result<T, RsqlError>;

#[derive(Error, Debug)]
pub enum RsqlError {
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parser Error: {0}")]
    ParserError(String),

    #[error("WAL Error: {0}")]
    WalError(String),

    #[error("Unknown Error: {0}")]
    Unknown(String),
}
