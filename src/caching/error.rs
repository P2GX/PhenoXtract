use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Could not read from cache: {reason}")]
    ReadError { reason: String },
    #[error("Could not write to cache: {reason}")]
    WriteError { reason: String },
}
