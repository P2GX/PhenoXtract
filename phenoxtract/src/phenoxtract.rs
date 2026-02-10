use crate::Pipeline;
use crate::error::PipelineError;
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

/*impl TryFrom<PhenoXtractConfig> for Phenoxtract {
    type Error = ConstructionError;

    fn try_from(value: PhenoXtractConfig) -> Result<Self, Self::Error> {
        let pipeline = Pipeline::try_from(value.pipeline_config)?;
        let data_sources = value.data_sources;
        Ok(Self {
            pipeline,
            data_sources,
        })
    }
}*/
