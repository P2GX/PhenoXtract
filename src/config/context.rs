use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

/// Defines the semantic meaning or type of data in a column (either the header or the data itself).
///
/// This enum is used to tag data with a specific, machine-readable context,
/// such as identifying a column as containing HPO IDs or subject's sex.
#[derive(Debug, Clone, PartialEq, Deserialize, Default, Serialize, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Context {
    #[allow(unused)]
    HpoLabelOrId,
    #[allow(unused)]
    OmimLabelOrId,
    #[allow(unused)]
    OrphanetLabelOrId,
    #[allow(unused)]
    MondoLabelOrId,
    #[allow(unused)]
    HgncSymbolOrId,
    #[allow(unused)]
    GenoLabelOrId,
    #[allow(unused)]
    Hgvs,
    #[allow(unused)]
    OnsetDateTime,
    #[allow(unused)]
    OnsetAge,
    #[allow(unused)]
    SubjectId,
    #[allow(unused)]
    SubjectSex,
    #[allow(unused)]
    DateOfBirth,
    #[allow(unused)]
    VitalStatus,
    #[allow(unused)]
    AgeAtLastEncounter,
    #[allow(unused)]
    DateAtLastEncounter,
    #[allow(unused)]
    WeightInKg,
    #[allow(unused)]
    DateOfDeath,
    #[allow(unused)]
    AgeOfDeath,
    #[allow(unused)]
    CauseOfDeath,
    #[allow(unused)]
    SurvivalTimeDays,
    #[allow(unused)]
    SmokerBool,
    #[allow(unused)]
    ObservationStatus,
    #[allow(unused)]
    MultiHpoId,
    #[default]
    None,
    //...
}

impl Display for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

// context constants

pub const DISEASE_LABEL_OR_ID_CONTEXTS: [Context; 3] = [
    Context::MondoLabelOrId,
    Context::OmimLabelOrId,
    Context::OrphanetLabelOrId,
];

pub const DATE_CONTEXTS: [Context; 4] = [
    Context::DateOfBirth,
    Context::DateAtLastEncounter,
    Context::OnsetDateTime,
    Context::DateOfDeath,
];

pub const DATE_CONTEXTS_WITHOUT_DOB: [Context; 3] = [
    Context::DateAtLastEncounter,
    Context::OnsetDateTime,
    Context::DateOfDeath,
];

pub const AGE_CONTEXTS: [Context; 3] = [
    Context::AgeAtLastEncounter,
    Context::OnsetAge,
    Context::AgeOfDeath,
];

pub fn date_to_age_contexts_hash_map() -> HashMap<Context, Context> {
    DATE_CONTEXTS_WITHOUT_DOB
        .into_iter()
        .zip(AGE_CONTEXTS)
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::config::context::{Context, date_to_age_contexts_hash_map};
    use rstest::rstest;

    #[rstest]
    fn test_date_to_age_contexts_hash_map() {
        let hm = date_to_age_contexts_hash_map();
        assert_eq!(hm.len(), 3);
        assert_eq!(
            hm[&Context::DateAtLastEncounter],
            Context::AgeAtLastEncounter
        );
        assert_eq!(hm[&Context::OnsetDateTime], Context::OnsetAge);
        assert_eq!(hm[&Context::DateOfDeath], Context::AgeOfDeath);
    }
}
