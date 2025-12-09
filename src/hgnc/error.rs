use crate::caching::error::CacherError;
use redb::{CommitError, DatabaseError, StorageError, TableError, TransactionError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HGNCError {
    #[error(
        "Found '{n_found}' documents for '{identifier}' on HGNC, when '{n_expected}' were expected."
    )]
    UnexpectedNumberOfDocuments {
        identifier: String,
        n_found: usize,
        n_expected: usize,
    },
    #[error("No {desired_element} found in GeneDoc.")]
    MissingElementInDocument { desired_element: String },
    #[error("Cant establish caching dir {0}")]
    CannotEstablishCacheDir(String),
    #[error(transparent)]
    CacherError(#[from] CacherError),
    #[error(transparent)]
    CacheCommit(#[from] CommitError),
    #[error(transparent)]
    CacheStorage(#[from] StorageError),
    #[error(transparent)]
    CacheTransaction(#[from] TransactionError),
    #[error(transparent)]
    CacheDatabase(#[from] DatabaseError),
    #[error(transparent)]
    CacheTable(#[from] TableError),
    #[error(transparent)]
    Request(#[from] reqwest::Error),
}
