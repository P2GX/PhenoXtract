use crate::config::strategy_config::StrategyConfig;
use crate::error::ConstructionError;
use crate::ontology::CachedOntologyFactory;
use crate::transform::strategies::mapping::DefaultMappings;
use crate::transform::strategies::{
    AliasMapStrategy, MappingStrategy, MultiHPOColExpansionStrategy, OntologyNormaliserStrategy,
    StringCorrectionStrategy,
};
use crate::transform::traits::Strategy;

pub struct StrategyFactory {
    ontology_factory: CachedOntologyFactory,
}

impl StrategyFactory {
    pub fn new(ontology_factory: CachedOntologyFactory) -> Self {
        StrategyFactory { ontology_factory }
    }
    #[allow(dead_code)]
    pub fn try_from_configs(
        &mut self,
        configs: &[StrategyConfig],
    ) -> Result<Vec<Box<dyn Strategy>>, ConstructionError>
    where
        Self: Sized,
    {
        configs
            .iter()
            .map(|config| self.try_from_config(config))
            .collect()
    }

    #[allow(dead_code)]
    pub fn try_from_config(
        &mut self,
        config: &StrategyConfig,
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
                let ontology_bi_dict = self.ontology_factory.build_bidict(ontology_prefix, None)?;

                Ok(Box::new(OntologyNormaliserStrategy::new(
                    ontology_bi_dict,
                    data_context.clone(),
                )))
            }
            StrategyConfig::StringCorrectionStrategy {
                header_context,
                data_context,
                chars_to_replace,
                new_chars,
            } => Ok(Box::new(StringCorrectionStrategy::new(
                header_context.clone(),
                data_context.clone(),
                chars_to_replace.clone(),
                new_chars.clone(),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::strategy_config::StrategyConfig;
    use crate::config::table_context::Context;
    use crate::ontology::OntologyRef;
    use crate::transform::strategies::mapping::DefaultMappings;
    use rstest::rstest;

    fn create_test_factory() -> StrategyFactory {
        StrategyFactory {
            ontology_factory: CachedOntologyFactory::default(),
        }
    }

    #[rstest]
    fn test_try_from_config_alias_mapping() {
        let mut factory = create_test_factory();
        let config = StrategyConfig::AliasMapping;

        let result = factory.try_from_config(&config);

        assert!(
            result.is_ok(),
            "Should successfully create AliasMapStrategy"
        );
    }

    #[rstest]
    fn test_try_from_config_default_sex_mapping() {
        let mut factory = create_test_factory();
        let config = StrategyConfig::DefaultMappings(DefaultMappings::SexMapping);

        let result = factory.try_from_config(&config);

        assert!(
            result.is_ok(),
            "Should successfully create SexMapping strategy"
        );
    }

    #[rstest]
    fn test_try_from_config_multi_hpo_expansion() {
        let mut factory = create_test_factory();
        let config = StrategyConfig::MultiHPOColumnExpansion;

        let result = factory.try_from_config(&config);

        assert!(
            result.is_ok(),
            "Should successfully create MultiHPOColExpansionStrategy"
        );
    }

    #[rstest]
    fn test_try_from_config_ontology_normalizer() {
        let mut factory = create_test_factory();
        let config = StrategyConfig::OntologyNormalizer {
            ontology_prefix: OntologyRef::geno().clone(),
            data_context: Context::GenoLabelOrId,
        };

        let result = factory.try_from_config(&config);

        assert!(result.is_ok(), "{:?}", result);
    }

    #[rstest]
    fn test_try_from_configs_empty() {
        let mut factory = create_test_factory();
        let configs: Vec<StrategyConfig> = vec![];

        let result = factory.try_from_configs(&configs);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().len(),
            0,
            "Empty config should return empty vec"
        );
    }

    #[rstest]
    fn test_try_from_configs_single() {
        let mut factory = create_test_factory();
        let configs = vec![StrategyConfig::AliasMapping];

        let result = factory.try_from_configs(&configs);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1, "Should create one strategy");
    }

    #[rstest]
    fn test_try_from_configs_multiple() {
        let mut factory = create_test_factory();
        let configs = vec![
            StrategyConfig::AliasMapping,
            StrategyConfig::MultiHPOColumnExpansion,
            StrategyConfig::DefaultMappings(DefaultMappings::SexMapping),
        ];

        let result = factory.try_from_configs(&configs);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 3, "Should create three strategies");
    }

    #[rstest]
    fn test_strategy_trait_object_creation() {
        let mut factory = create_test_factory();
        let config = StrategyConfig::AliasMapping;

        let strategy_result = factory.try_from_config(&config);

        assert!(strategy_result.is_ok());
        let strategy: Box<dyn Strategy> = strategy_result.unwrap();

        let _: &dyn Strategy = strategy.as_ref();
    }
}
