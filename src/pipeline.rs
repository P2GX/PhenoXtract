use crate::config::pipeline_config::PipelineConfig;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::traits::Extractable;
use crate::load::file_system_loader::FileSystemLoader;
use crate::load::traits::Loadable;
use crate::ontology::traits::OntologyRegistry;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use crate::transform::transform_module::TransformerModule;

use crate::error::{ConstructionError, PipelineError};
use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
use crate::ontology::utils::init_ontolius;
use log::info;
use phenopackets::schema::v2::Phenopacket;
use std::path::PathBuf;
use validator::Validate;

#[allow(dead_code)]
struct Pipeline {
    transformer_module: TransformerModule,
    loader_module: Box<dyn Loadable>,
}

impl Pipeline {
    #[allow(dead_code)]
    pub fn run(
        &self,
        extractables: &mut [impl Extractable + Validate],
    ) -> Result<(), PipelineError> {
        let mut data = self.extract(extractables)?;
        let phenopackets = self.transform(data.as_mut_slice())?;
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
        &self,
        tables: &mut [ContextualizedDataFrame],
    ) -> Result<Vec<Phenopacket>, PipelineError> {
        info!("Starting Transformation");
        tables.iter().try_for_each(|t| t.validate())?;

        let phenopackets = self.transformer_module.run(tables)?;
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
        let hpo_registry = GithubOntologyRegistry::default_hpo_registry()?;
        // TOOD: Read hpo version from config later
        let registry_path = hpo_registry.register("latest")?;
        let hpo = init_ontolius(registry_path)?;
        let tf_module = TransformerModule::new(vec![], PhenopacketBuilder::new(hpo));
        let loader_module = FileSystemLoader {
            out_path: PathBuf::from("some/dir/"),
        };
        Ok(Pipeline::new(tf_module, loader_module))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skip_in_ci;
    use rstest::rstest;

    #[rstest]
    fn test_from_pipeline_config() {
        skip_in_ci!();
        let config = PipelineConfig::default();
        let _ = Pipeline::from_config(&config);
    }
}
