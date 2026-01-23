use crate::config::context::ContextKind;
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
        ontology: OntologyRef,
        data_context_kind: ContextKind,
    },
    AgeToIso8601,
    DateToAge,
}
