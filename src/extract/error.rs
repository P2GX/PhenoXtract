use calamine::XlsxError;
use polars::prelude::PolarsError;

#[derive(Debug)]
pub enum ExtractionError {
    #[allow(unused)]
    Polars(PolarsError),
    #[allow(dead_code)]
    Calamine(XlsxError),
    ExcelIndexing,
    VectorIndexing,
    NoStringInHeader,
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
