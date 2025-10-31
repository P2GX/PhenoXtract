use crate::config::pipeline_config::PipelineConfig;
use crate::extract::data_source::DataSource;
use crate::validation::phenoxtractor_config_validation::validate_unique_data_sources;
use config::{Config, ConfigError, File, FileFormat};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use validator::Validate;

/// Represents all necessary data to construct and run the table to phenopacket pipeline
#[derive(Debug, Deserialize, Validate, Serialize, Clone, PartialEq)]
pub struct PhenoXtractorConfig {
    #[validate(custom(function = "validate_unique_data_sources"))]
    #[allow(unused)]
    pub data_sources: Vec<DataSource>,
    #[allow(unused)]
    pub pipeline: PipelineConfig,
}

impl PhenoXtractorConfig {
    pub fn pipeline_config(&self) -> PipelineConfig {
        self.pipeline.clone()
    }
    #[allow(dead_code)]
    pub fn data_sources(&self) -> Vec<DataSource> {
        self.data_sources.clone()
    }
}
