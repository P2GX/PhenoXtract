use crate::Pipeline;
use crate::config::PhenoXtractConfig;
use crate::error::{ConstructionError, PipelineError};
use crate::extract::DataSource;

pub struct Phenoxtract {
    pipeline: Pipeline,
    data_sources: Vec<DataSource>,
}

impl Phenoxtract {
    pub fn run(&mut self) -> Result<(), PipelineError> {
        self.pipeline.run(self.data_sources.as_mut_slice())?;
        Ok(())
    }
}

impl TryFrom<PhenoXtractConfig> for Phenoxtract {
    type Error = ConstructionError;

    fn try_from(config: PhenoXtractConfig) -> Result<Self, Self::Error> {
        let pipeline = Pipeline::try_from(config.pipeline_config)?;
        let data_sources = config
            .data_sources
            .into_iter()
            .map(DataSource::try_from)
            .collect::<Result<Vec<DataSource>, ConstructionError>>()?;
        Ok(Self {
            pipeline,
            data_sources,
        })
    }
}
