use crate::config::pipeline_config::PipelineConfig;
use crate::config::{ConfigLoader, PhenoXtractConfig};
use crate::error::{ConstructionError, PipelineError};
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::traits::Extractable;
use crate::load::traits::Loadable;

use crate::load::loader_factory::LoaderFactory;
use crate::ontology::CachedOntologyFactory;
use crate::ontology::loinc_client::LoincClient;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::traits::HasPrefixId;
use crate::transform::collecting::cdf_collector_broker::CdfCollectorBroker;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use crate::transform::strategies::strategy_factory::StrategyFactory;
use crate::transform::strategies::traits::Strategy;
use crate::transform::transform_module::TransformerModule;
use log::info;
use phenopackets::schema::v2::Phenopacket;
use pivot::hgnc::CachedHGNCClient;
use pivot::hgvs::CachedHGVSClient;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug)]
pub struct Pipeline {
    transformer_module: TransformerModule,
    loader_module: Box<dyn Loadable>,
}

impl Pipeline {
    pub fn new(
        transformer_module: TransformerModule,
        loader_module: Box<dyn Loadable>,
    ) -> Pipeline {
        Pipeline {
            transformer_module,
            loader_module,
        }
    }

    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy>) {
        self.transformer_module.add_strategy(strategy);
    }
    pub fn insert_strategy(&mut self, idx: usize, strategy: Box<dyn Strategy>) {
        self.transformer_module.insert_strategy(idx, strategy);
    }

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
            .map(|ex| ex.extract())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
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
}

impl TryFrom<PipelineConfig> for Pipeline {
    type Error = ConstructionError;

    fn try_from(config: PipelineConfig) -> Result<Self, Self::Error> {
        let mut ontology_factory = CachedOntologyFactory::default();

        let hpo_bidict = if let Some(hp_ref) = &config.meta_data.hp_ref {
            Some(ontology_factory.build_bidict(hp_ref, None)?)
        } else {
            None
        };

        let mut disease_bidicts: HashMap<String, Arc<OntologyBiDict>> = HashMap::new();
        let mut unit_ontology_bidicts: HashMap<String, Arc<OntologyBiDict>> = HashMap::new();

        for disease_ref in &config.meta_data.disease_refs {
            let disease_dict = ontology_factory.build_bidict(disease_ref, None)?;
            disease_bidicts.insert(disease_dict.ontology.prefix_id().to_string(), disease_dict);
        }

        for unit_ontology_ref in &config.meta_data.unit_ontology_refs {
            let unit_ontology_dict = ontology_factory.build_bidict(unit_ontology_ref, None)?;
            unit_ontology_bidicts.insert(
                unit_ontology_dict.ontology.prefix_id().to_string(),
                unit_ontology_dict,
            );
        }

        let mut strategy_factory = StrategyFactory::new(ontology_factory);
        let phenopacket_builder = PhenopacketBuilder::new(
            Box::new(CachedHGNCClient::default()),
            Box::new(CachedHGVSClient::default()),
            hpo_bidict,
            disease_bidicts,
            unit_ontology_bidicts,
            config.credentials.loinc.map(LoincClient::new),
        );

        let strategies: Vec<Box<dyn Strategy>> = config
            .transform_strategies
            .iter()
            .map(|strat| strategy_factory.try_from_config(strat))
            .collect::<Result<Vec<_>, _>>()?;

        let tf_module = TransformerModule::new(
            strategies,
            CdfCollectorBroker::with_default_collectors(
                phenopacket_builder,
                config.meta_data.cohort_name.clone(),
            ),
        );
        let loader_module = LoaderFactory::try_from_config(config.loader)?;

        Ok(Pipeline::new(tf_module, loader_module))
    }
}

impl PartialEq for Pipeline {
    fn eq(&self, other: &Self) -> bool {
        self.transformer_module == other.transformer_module
            && format!("{:?}", self.loader_module) == format!("{:?}", other.loader_module)
    }
}

impl TryFrom<PhenoXtractConfig> for Pipeline {
    type Error = ConstructionError;

    fn try_from(config: PhenoXtractConfig) -> Result<Self, Self::Error> {
        Pipeline::try_from(config.pipeline)
    }
}

impl TryFrom<PathBuf> for Pipeline {
    type Error = ConstructionError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        if !path.exists() {
            return Err(ConstructionError::NoConfigFileFound(path));
        }
        let config: PhenoXtractConfig = ConfigLoader::load(path.clone()).unwrap();

        Pipeline::try_from(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigLoader;
    use crate::test_suite::config::get_full_config_bytes;
    use rstest::{fixture, rstest};
    use std::fs::File as StdFile;
    use std::io::Write;
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_try_from_pipeline_config(temp_dir: TempDir) {
        let file_path = temp_dir.path().join("config.yaml");
        let mut file = StdFile::create(&file_path).unwrap();
        file.write_all(get_full_config_bytes().as_slice()).unwrap();
        let config: PhenoXtractConfig = ConfigLoader::load(file_path.clone()).unwrap();

        let configs_from_sources = [
            Pipeline::try_from(config.clone()).unwrap(),
            Pipeline::try_from(config.pipeline.clone()).unwrap(),
            Pipeline::try_from(file_path).unwrap(),
        ];

        let expected_config = configs_from_sources.first().unwrap();

        for config in configs_from_sources.iter() {
            assert_eq!(config, expected_config);
        }
    }
}
