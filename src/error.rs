use crate::extract::error::ExtractionError;
use crate::ontology::error::{OntologyFactoryError, RegistryError};
use crate::transform::error::TransformError;
use std::path::PathBuf;
use thiserror::Error;

use crate::load::error::LoadError;
use validator::ValidationErrors;

#[allow(dead_code)]
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
}

#[derive(Debug, Error)]
pub enum PipelineError {
    #[allow(dead_code)]
    #[error(transparent)]
    Extraction(#[from] ExtractionError),
    #[allow(dead_code)]
    #[error(transparent)]
    Transform(#[from] TransformError),
    #[allow(dead_code)]
    #[error(transparent)]
    Validation(#[from] ValidationErrors),
    #[allow(dead_code)]
    #[error(transparent)]
    Load(#[from] LoadError),
}
