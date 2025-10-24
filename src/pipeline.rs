use crate::config::pipeline_config::PipelineConfig;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::traits::Extractable;
use crate::load::file_system_loader::FileSystemLoader;
use crate::load::traits::Loadable;
use crate::transform::transform_module::TransformerModule;

use crate::error::{ConstructionError, PipelineError};
use crate::ontology::CachedOntologyFactory;

use crate::ontology::resource_references::OntologyRef;
use crate::transform::Collector;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use log::info;
use phenopackets::schema::v2::Phenopacket;
use std::path::PathBuf;
use validator::Validate;

#[allow(dead_code)]
#[derive(Debug)]
pub struct Pipeline {
    transformer_module: TransformerModule,
    loader_module: Box<dyn Loadable>,
}

impl Pipeline {
    #[allow(dead_code)]
    pub fn run(
        &mut self,
        extractables: &mut [impl Extractable + Validate],
    ) -> Result<(), PipelineError> {
        let data = self.extract(extractables)?;
        let phenopackets = self.transform(data)?;
        self.load(phenopackets.as_slice())?;
        Ok(())
    }

    pub fn extract(
        &self,
        extractables: &mut [impl Extractable + Validate],
    ) -> Result<Vec<ContextualizedDataFrame>, PipelineError> {
        info!("Starting extract");
        extractables.validate()?;
        let tables: Vec<ContextualizedDataFrame> = extractables
            .iter()
            .flat_map(|ex| ex.extract().unwrap())
            .collect();
        info!("Concluded extraction extracted {:?} tables", tables.len());
        Ok(tables)
    }

    pub fn transform(
        &mut self,
        data: Vec<ContextualizedDataFrame>,
    ) -> Result<Vec<Phenopacket>, PipelineError> {
        info!("Starting Transformation");
        data.iter().try_for_each(|t| t.validate())?;

        let phenopackets = self.transformer_module.run(data)?;
        info!(
            "Concluded Transformation. Found {:?} Phenopackets",
            phenopackets.len()
        );
        Ok(phenopackets)
    }

    pub fn load(&self, phenopackets: &[Phenopacket]) -> Result<(), PipelineError> {
        self.loader_module.load(phenopackets)?;

        info!("Concluded Loading");
        Ok(())
    }
    pub fn new(
        transformer_module: TransformerModule,
        loader_module: impl Loadable + 'static,
    ) -> Pipeline {
        Pipeline {
            transformer_module,
            loader_module: Box::new(loader_module),
        }
    }

    #[allow(unused)]
    #[allow(dead_code)]
    pub fn from_config(value: &PipelineConfig) -> Result<Self, ConstructionError> {
        // In progress
        // TOOD: Read hpo version from config later
        let mut factory = CachedOntologyFactory::default();
        let hpo_dict = factory.build_bidict(&OntologyRef::hp(None), None).unwrap();
        let builder = PhenopacketBuilder::new(hpo_dict);
        let tf_module =
            TransformerModule::new(vec![], Collector::new(builder, "replace_me".to_owned()));
        let loader_module = FileSystemLoader::new(PathBuf::from("some/dir/"));

        Ok(Pipeline::new(tf_module, loader_module))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_from_pipeline_config() {
        let config = PipelineConfig::default();
        let _ = Pipeline::from_config(&config);
    }
}
