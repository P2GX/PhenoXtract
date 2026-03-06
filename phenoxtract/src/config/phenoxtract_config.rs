use crate::config::datasource_config::DataSourceConfig;
use crate::config::pipeline_config::PipelineConfig;
use crate::validation::phenoxtractor_config_validation::validate_unique_data_sources;
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Represents all necessary data to construct and run the table to phenopacket pipeline
#[derive(Debug, Deserialize, Serialize, Validate, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PhenoXtractConfig {
    #[validate(custom(function = "validate_unique_data_sources"))]
    pub data_sources: Vec<DataSourceConfig>,
    #[serde(rename = "pipeline")]
    pub pipeline_config: PipelineConfig,
}

impl PhenoXtractConfig {
    pub fn pipeline_config(&self) -> PipelineConfig {
        self.pipeline_config.clone()
    }
    pub fn data_sources(&self) -> Vec<DataSourceConfig> {
        self.data_sources.clone()
    }
}
