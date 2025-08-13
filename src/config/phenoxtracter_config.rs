use crate::config::meta_data::MetaData;
use crate::config::pipeline_config::PipelineConfig;
use crate::extract::data_source::DataSource;
use serde::Deserialize;

/// Represents all necessary data to construct and run the table to phenopacket pipeline
#[derive(Debug, Deserialize)]
pub struct PhenoXtractorConfig {
    #[allow(unused)]
    pub data_sources: Vec<DataSource>,
    #[allow(unused)]
    pub meta_data: MetaData,
    #[allow(unused)]
    pub pipeline: Option<PipelineConfig>,
}
