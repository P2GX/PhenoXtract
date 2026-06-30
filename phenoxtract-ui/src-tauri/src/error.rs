use thiserror::Error;

#[derive(Debug, Error)]
pub enum PhenoxtractBackendError {
    #[error("Cant load App State: {0}.")]
    CantReadAppState(String),
    #[error("Cant save App State: {0}.")]
    CantWriteAppState(String),
    #[error("Unable to initialize app directories: {0}")]
    UnableToInitStateDirs(String),
}
