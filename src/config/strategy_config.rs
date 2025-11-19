use crate::config::context::Context;
use crate::ontology::OntologyRef;
use crate::transform::strategies::mapping::DefaultMapping;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyConfig {
    AliasMap,
    DefaultMapping(DefaultMapping),
    MultiHpoColExpansion,
    OntologyNormaliser {
        ontology_prefix: OntologyRef,
        data_context: Context,
    },
    AgeToIso8601,
}
