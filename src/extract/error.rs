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
