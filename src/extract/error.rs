use calamine::XlsxError;
use polars::prelude::PolarsError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExtractionError {
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
    #[error("XlsxError error: {0}")]
    Calamine(#[from] XlsxError),
    #[error("Out of bounds index when loading vector {0} in {1}.")]
    ExcelIndexing(usize, String),
    #[error("Attempt to access vector at {0}. On Vector with len {1}")]
    VectorIndexing(usize, usize),
    #[error("Empty vector.")]
    EmptyVector,
    #[error("Table {0}  was empty.")]
    EmptyTable(String),
    #[error("Header was not a string.")]
    NoStringInHeader,
    #[error("Can't find table context with name {0}")]
    UnableToFindTableContext(String),
}
