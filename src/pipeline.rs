use crate::config::pipeline_config::PipelineConfig;
use crate::config::{ConfigLoader, PhenoXtractConfig};
use crate::error::{ConstructionError, PipelineError};
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::traits::Extractable;
use crate::load::traits::Loadable;
use std::fs;

use crate::load::loader_factory::LoaderFactory;
use crate::ontology::CachedOntologyFactory;
use crate::ontology::loinc_client::LoincClient;
use crate::transform::bidict_library::BiDictLibrary;
use crate::transform::collecting::cdf_collector_broker::CdfCollectorBroker;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use crate::transform::strategies::strategy_factory::StrategyFactory;
use crate::transform::strategies::traits::Strategy;
use crate::transform::transform_module::TransformerModule;
use crate::utils::get_cache_dir;
use log::info;
use ontology_registry::blocking::bio_registry_metadata_provider::BioRegistryMetadataProvider;
use ontology_registry::blocking::file_system_ontology_registry::FileSystemOntologyRegistry;
use ontology_registry::blocking::obolib_ontology_provider::OboLibraryProvider;
use phenopackets::schema::v2::Phenopacket;
use pivot::hgnc::CachedHGNCClient;
use pivot::hgvs::CachedHGVSClient;
use std::path::PathBuf;
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
        let ontology_registry_dir = get_cache_dir()?.join("ontology_registry");

        if !ontology_registry_dir.exists() {
            fs::create_dir_all(&ontology_registry_dir)?;
        }

        let mut ontology_factory =
            CachedOntologyFactory::new(Box::new(FileSystemOntologyRegistry::new(
                ontology_registry_dir,
                BioRegistryMetadataProvider::default(),
                OboLibraryProvider::default(),
            )));

        let mut hpo_bidict_library = BiDictLibrary::empty_with_name("HPO");
        let mut disease_bidict_library = BiDictLibrary::empty_with_name("DISEASE");
        let mut unit_bidict_library = BiDictLibrary::empty_with_name("UNIT");

        if let Some(hp_ref) = &config.meta_data.hp_ref {
            let hpo_bidict = ontology_factory.build_bidict(hp_ref, None)?;
            hpo_bidict_library.add_bidict(hpo_bidict);
        };

        for disease_ref in &config.meta_data.disease_refs {
            let disease_bidict = ontology_factory.build_bidict(disease_ref, None)?;
            disease_bidict_library.add_bidict(disease_bidict);
        }

        for unit_ontology_ref in &config.meta_data.unit_ontology_refs {
            let unit_bidict = ontology_factory.build_bidict(unit_ontology_ref, None)?;
            unit_bidict_library.add_bidict(unit_bidict);
        }

        let mut strategy_factory = StrategyFactory::new(ontology_factory);
        let phenopacket_builder = PhenopacketBuilder::new(
            Box::new(CachedHGNCClient::default()),
            Box::new(CachedHGVSClient::default()),
            hpo_bidict_library,
            disease_bidict_library,
            unit_bidict_library,
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
        let config: PhenoXtractConfig = ConfigLoader::load(path.clone())?;

        Pipeline::try_from(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigLoader;
    use crate::test_suite::config::get_full_config_bytes;
    use dotenvy::dotenv;
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
        dotenv().ok();
        let file_path = temp_dir.path().join("config.yaml");
        let mut file = StdFile::create(&file_path).expect("Failed to create config file");
        file.write_all(get_full_config_bytes().as_slice())
            .expect("Failed to write config file");
        let config: PhenoXtractConfig =
            ConfigLoader::load(file_path.clone()).expect("Failed to load config loader");

        let configs_from_sources = [
            Pipeline::try_from(config.clone()).expect("Failed to convert config from config"),
            Pipeline::try_from(config.pipeline.clone())
                .expect("Failed to convert config from pipeline"),
            Pipeline::try_from(file_path).expect("Failed to convert config from path"),
        ];

        let expected_config = configs_from_sources.first().unwrap();

        for config in configs_from_sources.iter() {
            assert_eq!(config, expected_config);
        }
    }
}
