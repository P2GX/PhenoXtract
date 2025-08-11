use crate::extract::contextualized_data_frame::ContextualizedDataFrame;

pub trait Extractable: std::fmt::Debug {
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, anyhow::Error>;
}
