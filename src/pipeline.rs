use crate::config::pipeline_config::PipelineConfig;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::traits::Extractable;
use crate::load::file_system_loader::FileSystemLoader;
use crate::load::traits::Loadable;
use crate::transform::transform_module::TransformerModule;
use std::collections::HashMap;

use crate::error::{ConstructionError, PipelineError};
use crate::ontology::CachedOntologyFactory;

use crate::config::{ConfigLoader, PhenoXtractorConfig};
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::traits::HasPrefixId;
use crate::transform::Collector;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use crate::transform::strategies::strategy_factory::StrategyFactory;
use log::info;
use phenopackets::schema::v2::Phenopacket;
use std::path::PathBuf;
use std::sync::Arc;
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
}

impl TryFrom<PipelineConfig> for Pipeline {
    type Error = ConstructionError;

    fn try_from(config: PipelineConfig) -> Result<Self, Self::Error> {
        let mut ontology_factory = CachedOntologyFactory::default();
        let hp_dict = ontology_factory.build_bidict(&config.meta_data.hp_ref, None)?;
        let mondo_dict = ontology_factory.build_bidict(&config.meta_data.mondo_ref, None)?;
        let geno_dict = ontology_factory.build_bidict(&config.meta_data.geno_ref, None)?;
        let bi_dicts: HashMap<String, Arc<OntologyBiDict>> = HashMap::from_iter([
            (hp_dict.ontology.prefix_id().to_string(), hp_dict),
            (mondo_dict.ontology.prefix_id().to_string(), mondo_dict),
            (geno_dict.ontology.prefix_id().to_string(), geno_dict),
        ]);
        let mut strat_factory = StrategyFactory::new(ontology_factory);

        let phenopacket_builder = PhenopacketBuilder::new(bi_dicts);

        let strategies: Result<Vec<_>, _> = config
            .transform_strategies
            .iter()
            .map(|strat| strat_factory.try_from_config(strat))
            .collect();
        let tf_module = TransformerModule::new(
            strategies?,
            Collector::new(phenopacket_builder, config.meta_data.cohort_name.clone()),
        );
        let loader_module = FileSystemLoader::new(PathBuf::from(config.loader));

        Ok(Pipeline::new(tf_module, loader_module))
    }
}

impl TryFrom<PhenoXtractorConfig> for Pipeline {
    type Error = ConstructionError;

    fn try_from(config: PhenoXtractorConfig) -> Result<Self, Self::Error> {
        Pipeline::try_from(config.pipeline)
    }
}

impl TryFrom<PathBuf> for Pipeline {
    type Error = ConstructionError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        if !path.exists() {
            return Err(ConstructionError::NoConfigFileFound(path));
        }
        let pheno_config: Result<PhenoXtractorConfig, _> = ConfigLoader::load(path.clone());
        if let Ok(pheno_config) = pheno_config {
            return Pipeline::try_from(pheno_config);
        };

        let pipeline_config: Result<PipelineConfig, _> = ConfigLoader::load(path);
        if let Ok(pipeline_config) = pipeline_config {
            return Pipeline::try_from(pipeline_config);
        };

        Err(ConstructionError::NoPipelineConfigFound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_try_from_pipeline_config() {
        let config = PipelineConfig::default();
        let _ = Pipeline::try(&config);
    }
}
