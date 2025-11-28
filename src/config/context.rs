use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// Defines the semantic meaning or type of data in a column (either the header or the data itself).
///
/// This enum is used to tag data with a specific, machine-readable context,
/// such as identifying a column as containing HPO IDs or subject's sex.
#[derive(Debug, Clone, PartialEq, Deserialize, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Context {
    HpoLabelOrId,
    OmimLabelOrId,
    OrphanetLabelOrId,
    MondoLabelOrId,
    HgncSymbolOrId,
    GenoLabelOrId,
    Hgvs,
    OnsetDateTime,
    OnsetAge,
    SubjectId,
    SubjectSex,
    DateOfBirth,
    VitalStatus,
    SubjectAge,
    WeightInKg,
    AgeOfDeath,
    CauseOfDeath,
    SurvivalTimeDays,
    ObservationStatus,
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

pub const AGE_CONTEXTS: [Context; 3] =
    [Context::SubjectAge, Context::OnsetAge, Context::AgeOfDeath];
