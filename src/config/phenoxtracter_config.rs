use crate::config::meta_data::MetaData;
use crate::config::pipeline_config::PipelineConfig;
use crate::extract::data_source::DataSource;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct PhenoXtractorConfig {
    #[allow(unused)]
    data_sources: Vec<DataSource>,
    #[allow(unused)]
    meta_data: MetaData,
    #[allow(unused)]
    pipeline: PipelineConfig,
}
