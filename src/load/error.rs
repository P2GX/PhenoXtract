use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),
    #[error("SerdeJson Error: {0}")]
    SerdeJson(serde_json::Error),
}
