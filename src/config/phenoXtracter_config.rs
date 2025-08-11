use crate::config::meta_data::MetaData;
use crate::config::pipeline_config::PipelineConfig;
use crate::extract::data_source::DataSource;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct PhenoXtractorConfig {
    data_sources: Vec<DataSource>,
    meta_data: MetaData,
    pipeline: PipelineConfig,
}
