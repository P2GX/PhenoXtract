use serde::{Deserialize, Serialize};

/// Represents the configuration for a data processing pipeline.
///
/// This struct holds the necessary information to define how data
/// should be loaded and transformed.
#[derive(Debug, Deserialize, Clone, Serialize, Default, PartialEq)]
pub struct PipelineConfig {
    #[allow(unused)]
    /// A list of strategies to transform the data. Each string identifies
    /// a specific transformation to be applied in order.
    transform_strategies: Vec<String>,

    #[allow(unused)]
    /// The loader responsible for fetching the initial data.
    loader: String,
}

impl PipelineConfig {
    #[allow(dead_code)]
    pub fn new(transform_strategies: Vec<String>, loader: String) -> Self {
        Self {
            transform_strategies,
            loader,
        }
    }
}
