use redb::{CommitError, DatabaseError, StorageError};

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
    #[allow(dead_code)]
    Client(ClientError),
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

impl From<ClientError> for RegistryError {
    fn from(err: ClientError) -> Self {
        RegistryError::Client(err)
    }
}
#[derive(Debug)]
pub enum ClientError {
    #[allow(dead_code)]
    CacheCommit(CommitError),
    #[allow(dead_code)]
    CacheStorage(StorageError),
    #[allow(dead_code)]
    Cache(DatabaseError),
    #[allow(dead_code)]
    Request(reqwest::Error),
}

impl From<CommitError> for ClientError {
    fn from(err: CommitError) -> Self {
        ClientError::CacheCommit(err)
    }
}

impl From<StorageError> for ClientError {
    fn from(err: StorageError) -> Self {
        ClientError::CacheStorage(err)
    }
}
impl From<DatabaseError> for ClientError {
    fn from(err: DatabaseError) -> Self {
        ClientError::Cache(err)
    }
}

impl From<reqwest::Error> for ClientError {
    fn from(err: reqwest::Error) -> Self {
        ClientError::Request(err)
    }
}
