use crate::config::meta_data::MetaData;
use crate::config::strategy_config::StrategyConfig;
use serde::{Deserialize, Serialize};

/// Represents the configuration for a data processing pipeline.
///
/// This struct holds the necessary information to define how data
/// should be loaded and transformed.
#[derive(Debug, Deserialize, Clone, Serialize, Default, PartialEq)]
pub struct PipelineConfig {
    /// Metadata the pipeline needs to configure itself. Like Ontology versions or resources.
    pub meta_data: MetaData,
    /// A list of strategies to transform the data. Each string identifies
    /// a specific transformation to be applied in order.
    pub transform_strategies: Vec<StrategyConfig>,

    #[allow(unused)]
    /// The loader responsible for fetching the initial data.
    pub loader: String,
}

impl PipelineConfig {
    #[allow(dead_code)]
    pub fn new(
        meta_data: MetaData,
        transform_strategies: Vec<StrategyConfig>,
        loader: String,
    ) -> Self {
        Self {
            meta_data,
            transform_strategies,
            loader,
        }
    }
}

// TODO: Add Try_From<PathBuf>
