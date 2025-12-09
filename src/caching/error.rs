use redb::{CommitError, DatabaseError, StorageError, TableError, TransactionError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacherError {
    #[error("Could not create a default cache directory. Problem: {0}")]
    CreateDefaultCache(String),
    #[error(transparent)]
    CacheDatabase(#[from] DatabaseError),
    #[error(transparent)]
    CacheTransaction(#[from] TransactionError),
    #[error(transparent)]
    CacheCommit(#[from] CommitError),
    #[error(transparent)]
    CacheTable(#[from] TableError),
    #[error(transparent)]
    CacheStorage(#[from] StorageError),
}
