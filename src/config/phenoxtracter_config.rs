use crate::config::meta_data::MetaData;
use crate::config::pipeline_config::PipelineConfig;
use crate::extract::data_source::DataSource;
use serde::Deserialize;

/// Represents all necessary data to construct and run the table to phenopacket pipeline
#[derive(Debug, Deserialize)]
struct PhenoXtractorConfig {
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
