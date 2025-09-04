use calamine::XlsxError;
use polars::prelude::PolarsError;

#[derive(Debug)]
pub enum ExtractionError {
    #[allow(unused)]
    Polars(PolarsError),
    #[allow(dead_code)]
    Calamine(XlsxError),
    #[allow(dead_code)]
    ExcelIndexing(String),
    #[allow(dead_code)]
    VectorIndexing(String),
    #[allow(dead_code)]
    NoStringInHeader(String),
    #[allow(dead_code)]
    ContextError(ContextError),
}

impl From<PolarsError> for ExtractionError {
    fn from(err: PolarsError) -> Self {
        ExtractionError::Polars(err)
    }
}

impl From<XlsxError> for ExtractionError {
    fn from(err: XlsxError) -> Self {
        ExtractionError::Calamine(err)
    }
}

impl From<ContextError> for ExtractionError {
    fn from(err: ContextError) -> Self {
        ExtractionError::ContextError(err)
    }
}

#[derive(Debug)]
pub enum ContextError {
    /// The specified context identifier could not be found.
    NotFound(String),
    /// The specific ID to be replaced was not found within a MultiIdentifier.
    MultiIdNotFound(String),
    /// An error occurred while trying to set the new ID.
    SetIdFailed(String),
}
