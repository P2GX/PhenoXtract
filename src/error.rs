use crate::extract::error::ExtractionError;
use crate::transform::error::TransformError;
use validator::ValidationErrors;

#[allow(dead_code)]
pub enum ConstructionError {
    NotFound(String),
}

pub enum PipelineError {
    #[allow(dead_code)]
    Extraction(ExtractionError),
    #[allow(dead_code)]
    Transform(TransformError),
    #[allow(dead_code)]
    Validation(ValidationErrors),
}

impl From<ExtractionError> for PipelineError {
    fn from(err: ExtractionError) -> PipelineError {
        PipelineError::Extraction(err)
    }
}

impl From<TransformError> for PipelineError {
    fn from(err: TransformError) -> PipelineError {
        PipelineError::Transform(err)
    }
}

impl From<ValidationErrors> for PipelineError {
    fn from(err: ValidationErrors) -> PipelineError {
        PipelineError::Validation(err)
    }
}
