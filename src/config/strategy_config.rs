use crate::config::table_context::Context;
use crate::ontology::OntologyRef;
use crate::transform::strategies::mapping::DefaultMappings;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyConfig {
    AliasMapping,
    DefaultMappings(DefaultMappings),
    #[serde(rename = "multi_hpo_column_expansion")]
    MultiHPOColumnExpansion,
    OntologyNormalizer {
        ontology_prefix: OntologyRef,
        data_context: Context,
    },
    StringCorrectionStrategy {
        header_context: Context,
        data_context: Context,
        chars_to_replace: String,
        new_chars: String,
    },
}
