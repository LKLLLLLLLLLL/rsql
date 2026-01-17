use super::error::RsqlError;

pub type RsqlResult<T> = std::result::Result<T, RsqlError>;