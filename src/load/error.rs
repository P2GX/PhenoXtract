#[derive(Debug)]
pub enum LoadError {
    #[allow(dead_code)]
    NotAllowedError(String),
    IOError(std::io::Error),
    SerdeJsonError(serde_json::Error),
}

impl From<std::io::Error> for LoadError {
    fn from(e: std::io::Error) -> Self {
        LoadError::IOError(e)
    }
}

impl From<serde_json::Error> for LoadError {
    fn from(e: serde_json::Error) -> Self {
        LoadError::SerdeJsonError(e)
    }
}
