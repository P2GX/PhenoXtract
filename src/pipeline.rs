use crate::config::pipeline_config::PipelineConfig;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::traits::Extractable;
use crate::load::file_system_loader::FileSystemLoader;
use crate::load::traits::Loadable;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use crate::transform::transform_module::TransformerModule;

use crate::error::ConstructionError;
use phenopackets::schema::v2::Phenopacket;
use std::path::PathBuf;

#[allow(dead_code)]
struct Pipeline {
    transformer_module: TransformerModule,
    loader_module: Box<dyn Loadable>,
}

impl Pipeline {
    #[allow(dead_code)]
    pub fn run(&self, extractables: &mut [impl Extractable]) -> Result<(), anyhow::Error> {
        let mut data = self.extract(extractables)?;
        let phenopackets = self.transform(data.as_mut_slice())?;
        self.load(phenopackets.as_slice())?;
        Ok(())
    }

    pub fn extract(
        &self,
        extractables: &mut [impl Extractable],
    ) -> Result<Vec<ContextualizedDataFrame>, anyhow::Error> {
        let tables: Vec<ContextualizedDataFrame> = extractables
            .iter()
            .flat_map(|ex| ex.extract().unwrap())
            .collect();

        Ok(tables)
    }

    pub fn transform(
        &self,
        tables: &mut [ContextualizedDataFrame],
    ) -> Result<Vec<Phenopacket>, anyhow::Error> {
        let phenopackets = self.transformer_module.run(tables)?;
        Ok(phenopackets)
    }

    pub fn load(&self, phenopackets: &[Phenopacket]) -> Result<(), anyhow::Error> {
        for phenopacket in phenopackets {
            if let Err(e) = self.loader_module.load(phenopacket) {
                // TODO: Replace print with logging later
                println!(
                    "Could not save Phenopacket for subject: {}. Error: {:?}",
                    phenopacket.clone().subject.unwrap().id.as_str(),
                    e
                )
            }
        }
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
        /// In progress
        let builder = PhenopacketBuilder::default();
        /// TODO: Write task for this
        let tf_module = TransformerModule::new(vec![], PhenopacketBuilder::default());
        let loader_module = FileSystemLoader {
            out_path: PathBuf::from("some/dir/"),
        };
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
