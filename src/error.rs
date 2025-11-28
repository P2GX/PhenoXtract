use crate::extract::error::ExtractionError;
use crate::ontology::error::{OntologyFactoryError, RegistryError};
use crate::transform::error::TransformError;
use config::ConfigError;
use std::path::PathBuf;
use thiserror::Error;

use crate::load::error::LoadError;
use validator::ValidationErrors;

#[derive(Debug, Error)]
pub enum ConstructionError {
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Ontolius(#[from] anyhow::Error),
    #[error(transparent)]
    OntologyFactoryError(#[from] OntologyFactoryError),
    #[error("No Pipeline Config found.")]
    NoPipelineConfigFound,
    #[error("Could not find config file at '{0}'")]
    NoConfigFileFound(PathBuf),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    ConfigError(#[from] ConfigError),
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error(transparent)]
    Extraction(#[from] ExtractionError),
    #[error(transparent)]
    Transform(#[from] TransformError),
    #[error(transparent)]
    Validation(#[from] ValidationErrors),
    #[error(transparent)]
    Load(#[from] LoadError),
}
