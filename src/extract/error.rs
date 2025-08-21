use polars::prelude::PolarsError;

#[derive(Debug)]
pub enum ExtractionError {
    #[allow(unused)]
    PolarsError(PolarsError),
}

impl From<PolarsError> for ExtractionError {
    fn from(err: PolarsError) -> Self {
        ExtractionError::PolarsError(err)
    }
}
