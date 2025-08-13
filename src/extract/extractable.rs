use crate::extract::contextualized_data_frame::ContextualizedDataFrame;

/// A trait for types that can be extracted into one or more `ContextualizedDataFrame`s.
pub trait Extractable: std::fmt::Debug {
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, anyhow::Error>;
}
