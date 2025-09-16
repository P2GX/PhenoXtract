#[derive(Debug)]
pub enum LoadError {
    #[allow(dead_code)]
    NotAllowed(String),
    #[allow(dead_code)]
    IO(std::io::Error),
    #[allow(dead_code)]
    SerdeJson(serde_json::Error),
}

impl From<std::io::Error> for LoadError {
    fn from(e: std::io::Error) -> Self {
        LoadError::IO(e)
    }
}

impl From<serde_json::Error> for LoadError {
    fn from(e: serde_json::Error) -> Self {
        LoadError::SerdeJson(e)
    }
}
