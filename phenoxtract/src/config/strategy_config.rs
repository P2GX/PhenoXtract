use crate::config::context::ContextKind;
use crate::ontology::resource_references::ResourceRef;
use crate::transform::strategies::mapping::DefaultMapping;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum StrategyConfig {
    AliasMap,
    DefaultMapping(DefaultMapping),
    MultiHpoColExpansion,
    OntologyNormaliser {
        ontology: ResourceRef,
        data_context_kind: ContextKind,
    },
    AgeToIso8601,
    DateToAge,
}
