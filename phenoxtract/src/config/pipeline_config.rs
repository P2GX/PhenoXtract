use crate::config::loader_config::LoaderConfig;
use crate::config::meta_data::MetaData;
use crate::config::strategy_config::StrategyConfig;
use crate::utils::default_cache_dir;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Represents the configuration for a data processing pipeline.
///
/// This struct holds the necessary information to define how data
/// should be loaded and transformed.
#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct PipelineConfig {
    /// Metadata the pipeline needs to configure itself. Like Ontology versions or resources.
    pub meta_data: MetaData,
    /// A list of strategies to transform the data. Each string identifies
    /// a specific transformation to be applied in order.
    pub transform_strategies: Vec<StrategyConfig>,
    /// The loader responsible for fetching the initial data.
    pub loader: LoaderConfig,
    #[serde(default = "config_cache_dir")]
    pub cache_dir: Option<PathBuf>,
}

impl PipelineConfig {
    pub fn new(
        meta_data: MetaData,
        transform_strategies: Vec<StrategyConfig>,
        loader: LoaderConfig,
        cache_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            meta_data,
            transform_strategies,
            loader,
            cache_dir,
        }
    }
}

fn config_cache_dir() -> Option<PathBuf> {
    let cache_dir = default_cache_dir();

    match cache_dir {
        Ok(dir) => Some(dir),
        Err(err) => {
            panic!(
                "Could not get cache directory: {}. To avoid this set the cache_dir variable in the config.",
                err
            )
        }
    }
}
