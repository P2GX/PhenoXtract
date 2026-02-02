use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::error::ExtractionError;

/// A trait for types that can be extracted into one or more `ContextualizedDataFrame`s.
pub trait Extractable: std::fmt::Debug {
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, ExtractionError>;
}

pub trait HasSource {
    type Source;
    fn source(&self) -> &Self::Source;
    fn with_source(self, source: &Self::Source) -> Self;
}
