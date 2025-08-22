use crate::extract::error::ExtractionError;
use crate::transform::error::TransformError;

#[allow(dead_code)]
pub enum ConstructionError {
    NotFound(String),
}

pub enum PipelineError {
    ExtractionError(ExtractionError),
    TransformError(TransformError),
}

impl From<ExtractionError> for PipelineError {
    fn from(err: ExtractionError) -> PipelineError {
        PipelineError::ExtractionError(err)
    }
}

impl From<TransformError> for PipelineError {
    fn from(err: TransformError) -> PipelineError {
        PipelineError::TransformError(err)
    }
}
