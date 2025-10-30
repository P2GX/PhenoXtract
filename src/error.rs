use crate::extract::error::ExtractionError;
use crate::ontology::error::RegistryError;
use crate::transform::error::TransformError;
use thiserror::Error;

use crate::load::error::LoadError;
use validator::ValidationErrors;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum ConstructionError {
    #[error("Registry error: {0}")]
    Registry(#[from] RegistryError),
    #[error("Ontolius error: {0}")]
    Ontolius(#[from] anyhow::Error),
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
