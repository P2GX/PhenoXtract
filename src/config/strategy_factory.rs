use crate::config::strategy_config::StrategyConfig;
use crate::error::ConstructionError;
use crate::ontology::CachedOntologyFactory;
use crate::transform::strategies::mapping::DefaultMappings;
use crate::transform::strategies::{
    AliasMapStrategy, MappingStrategy, MultiHPOColExpansionStrategy, OntologyNormaliserStrategy,
};
use crate::transform::traits::Strategy;

struct StrategyFactory {
    ontology_factory: CachedOntologyFactory,
}

impl StrategyFactory {
    #[allow(dead_code)]
    fn try_from_config(
        &mut self,
        config: StrategyConfig,
    ) -> Result<Box<dyn Strategy>, ConstructionError>
    where
        Self: Sized,
    {
        match config {
            StrategyConfig::AliasMapping => Ok(Box::new(AliasMapStrategy)),
            StrategyConfig::DefaultMappings(mapping_type) => match mapping_type {
                DefaultMappings::SexMapping => {
                    Ok(Box::new(MappingStrategy::default_sex_mapping_strategy()))
                }
            },
            StrategyConfig::MultiHPOColumnExpansion => Ok(Box::new(MultiHPOColExpansionStrategy)),
            StrategyConfig::OntologyNormalizer {
                ontology_prefix,
                data_context,
            } => {
                let ontology_bi_dict =
                    self.ontology_factory.build_bidict(&ontology_prefix, None)?;
                Ok(Box::new(OntologyNormaliserStrategy::new(
                    ontology_bi_dict,
                    data_context,
                )))
            }
        }
    }
}
