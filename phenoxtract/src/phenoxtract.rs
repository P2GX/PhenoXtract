use crate::Pipeline;
use crate::error::PipelineError;
use crate::extract::DataSource;

#[derive(PartialEq, Debug)]
pub struct Phenoxtract {
    pub(crate) pipeline: Pipeline,
    pub(crate) data_sources: Vec<DataSource>,
}

impl Phenoxtract {
    pub fn run(&mut self) -> Result<(), PipelineError> {
        self.pipeline.run(self.data_sources.as_mut_slice())?;
        Ok(())
    }
}

impl Phenoxtract {
    pub fn new(pipeline: Pipeline, data_sources: Vec<DataSource>) -> Phenoxtract {
        Phenoxtract {
            pipeline,
            data_sources,
        }
    }
}
