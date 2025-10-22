use crate::ontology::enums::OntologyRef;
use redb::{CommitError, DatabaseError, StorageError, TableError, TransactionError};
use std::fmt::{Debug, Display, Formatter};
#[derive(Debug)]
pub enum RegistryError {
    #[allow(dead_code)]
    Io(std::io::Error),
    #[allow(dead_code)]
    Http(reqwest::Error),
    #[allow(dead_code)]
    CantEstablishRegistryDir,
    #[allow(dead_code)]
    NotRegistered(String),
    #[allow(dead_code)]
    UnableToResolveVersion(String, Option<String>),
    #[allow(dead_code)]
    Client(ClientError),
}

impl Display for RegistryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::Io(e) => write!(f, "IO error: {}", e),
            RegistryError::Http(e) => write!(f, "HTTP error: {}", e),
            RegistryError::CantEstablishRegistryDir => {
                write!(f, "Cannot establish registry directory")
            }
            RegistryError::NotRegistered(name) => {
                write!(f, "Not registered: {}", name)
            }
            RegistryError::UnableToResolveVersion(ver, file) => {
                write!(f, "Unable to resolve version: {} for file {:?}", ver, file)
            }
            RegistryError::Client(e) => write!(f, "Client error: {}", e),
        }
    }
}

impl std::error::Error for RegistryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RegistryError::Io(e) => Some(e),
            RegistryError::Http(e) => Some(e),
            RegistryError::Client(e) => Some(e),
            _ => None,
        }
    }
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
    CacheTransaction(TransactionError),
    #[allow(dead_code)]
    CacheDatabase(DatabaseError),
    #[allow(dead_code)]
    CacheTable(TableError),
    #[allow(dead_code)]
    Request(reqwest::Error),
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::CacheCommit(e) => write!(f, "Cache commit error: {}", e),
            ClientError::CacheStorage(e) => write!(f, "Cache storage error: {}", e),
            ClientError::CacheTransaction(e) => write!(f, "Cache transaction error: {}", e),
            ClientError::CacheDatabase(e) => write!(f, "Cache database error: {}", e),
            ClientError::CacheTable(e) => write!(f, "Cache table error: {}", e),
            ClientError::Request(e) => write!(f, "Request error: {}", e),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::CacheCommit(e) => Some(e),
            ClientError::CacheStorage(e) => Some(e),
            ClientError::CacheTransaction(e) => Some(e),
            ClientError::CacheDatabase(e) => Some(e),
            ClientError::CacheTable(e) => Some(e),
            ClientError::Request(e) => Some(e),
        }
    }
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
impl From<TransactionError> for ClientError {
    fn from(err: TransactionError) -> Self {
        ClientError::CacheTransaction(err)
    }
}
impl From<DatabaseError> for ClientError {
    fn from(err: DatabaseError) -> Self {
        ClientError::CacheDatabase(err)
    }
}

impl From<TableError> for ClientError {
    fn from(err: TableError) -> Self {
        ClientError::CacheTable(err)
    }
}
impl From<reqwest::Error> for ClientError {
    fn from(err: reqwest::Error) -> Self {
        ClientError::Request(err)
    }
}

pub enum OntologyFactoryError {
    CantBuild(anyhow::Error, OntologyRef),
}

impl Display for OntologyFactoryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OntologyFactoryError::CantBuild(err, ontology) => {
                write!(
                    f,
                    "Failed to build ontology '{}':\n  Caused by: {} \n",
                    ontology, err
                )
            }
        }
    }
}
impl Debug for OntologyFactoryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}
