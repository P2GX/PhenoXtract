use crate::Pipeline;
use crate::config::ConfigLoader;
use crate::config::pipeline_config::PipelineConfig;
use crate::error::{ConstructionError, PipelineError};
use crate::extract::data_source::DataSource;
use crate::validation::phenoxtractor_config_validation::validate_unique_data_sources;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use validator::Validate;

/// Represents all necessary data to construct and run the table to phenopacket pipeline
#[derive(Debug, Deserialize, Serialize, Validate, Clone, PartialEq)]
pub struct PhenoXtractConfig {
    #[validate(custom(function = "validate_unique_data_sources"))]
    pub data_sources: Vec<DataSource>,
    pub pipeline: PipelineConfig,
}

impl PhenoXtractConfig {
    pub fn run(&self) -> Result<(), PipelineError> {
        let mut pipeline = Pipeline::try_from(self.pipeline.clone())?;
        pipeline.run(self.data_sources.clone().as_mut_slice())?;
        Ok(())
    }
}

impl TryFrom<PathBuf> for PhenoXtractConfig {
    type Error = ConstructionError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        Ok(ConfigLoader::load(path)?)
    }
}
