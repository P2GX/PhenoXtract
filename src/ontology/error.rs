#[derive(Debug)]
pub enum RegistryError {
    #[allow(dead_code)]
    Io(std::io::Error),
    #[allow(dead_code)]
    Http(reqwest::Error),
    #[allow(dead_code)]
    EnvironmentVarNotSet(String),
    #[allow(dead_code)]
    NotRegistered(String),
    #[allow(dead_code)]
    UnableToResolveVersion(String),
}

impl From<std::io::Error> for RegistryError {
    fn from(err: std::io::Error) -> Self {
        RegistryError::Io(err)
    }
}

impl From<reqwest::Error> for RegistryError {
    fn from(err: reqwest::Error) -> Self {
        RegistryError::Http(err)
    }
}
