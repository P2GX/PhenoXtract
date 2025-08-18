use crate::extract::contextualized_data_frame::ContextualizedDataFrame;

/// A trait for types that can be extracted into one or more `ContextualizedDataFrame`s.
pub trait Extractable: std::fmt::Debug {
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, anyhow::Error>;
}

pub trait HasSource {
    type Source;
    #[allow(dead_code)]
    fn source(&self) -> &Self::Source;
    #[allow(dead_code)]
    fn with_source(self, source: &Self::Source) -> Self;
}
