use crate::extract::error::ExtractionError;
use crate::ontology::error::{FactoryError, RegistryError};
use crate::transform::error::TransformError;
use config::ConfigError;
use pivot::hgnc::HGNCError;
use pivot::hgvs::HGVSError;
use polars::prelude::PolarsError;
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
    OntologyFactoryError(#[from] FactoryError),
    #[error("No Pipeline Config found.")]
    NoPipelineConfigFound,
    #[error("Could not load the aliases at {path} as a DataFrame. {err}")]
    LoadingAliases { path: PathBuf, err: PolarsError },
    #[error("Could not find config file at '{0}'")]
    NoConfigFileFound(PathBuf),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    ConfigError(#[from] ConfigError),
    #[error(transparent)]
    HgncError(#[from] HGNCError),
    #[error(transparent)]
    HgvsError(#[from] HGVSError),
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
