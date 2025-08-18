use crate::config::meta_data::MetaData;
use crate::config::pipeline_config::PipelineConfig;
use crate::extract::data_source::DataSource;
use crate::validation::phenoxtractor_config_validation::validate_unique_data_sources;
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Represents all necessary data to construct and run the table to phenopacket pipeline
#[derive(Debug, Deserialize, Validate, Serialize)]
struct PhenoXtractorConfig {
    #[validate(custom(function = "validate_unique_data_sources"))]
    #[allow(unused)]
    data_sources: Vec<DataSource>,
    #[allow(unused)]
    meta_data: MetaData,
    #[allow(unused)]
    pipeline: Option<PipelineConfig>,
}

impl PhenoXtractorConfig {
    #[allow(dead_code)]
    pub fn get_pipeline_config(&self) -> Option<PipelineConfig> {
        self.pipeline.clone()
    }
    #[allow(dead_code)]
    pub fn get_data_sources(&self) -> Vec<DataSource> {
        self.data_sources.clone()
    }
    #[allow(dead_code)]
    pub fn get_meta_data(&self) -> MetaData {
        self.meta_data.clone()
    }
}
