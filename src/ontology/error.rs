use crate::caching::error::CacheError;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Cant setup directory for registry.")]
    CantEstablishRegistryDir,
    #[error("Not Registered: {0}")]
    NotRegistered(String),
    #[error("Ontology {0} does not offer a json version")]
    JsonFileMissing(String),
    #[error("Cant resolve version: {0} for file {1:?}")]
    UnableToResolveVersion(String, Option<String>),
    #[error("Client error: {0}")]
    Client(#[from] ClientError),
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Request error: {0}")]
    Request(#[from] reqwest::Error),
}

#[derive(Debug, Error)]
pub enum FactoryError {
    #[error("Failed to build ontology '{reason}'")]
    CantBuild { reason: String },
    #[error(transparent)]
    CacheError(#[from] CacheError),
}

#[derive(Debug, Error)]
pub enum BiDictError {
    #[error("Could not find entry for {0}")]
    NotFound(String),
    #[error("Request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("Cache error: {reason}")]
    Caching { reason: String },
}
