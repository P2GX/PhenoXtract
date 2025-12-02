use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// Defines the semantic meaning or type of data in a column (either the header or the data itself).
///
/// This enum is used to tag data with a specific, machine-readable context,
/// such as identifying a column as containing HPO IDs or subject's sex.
#[derive(Debug, Clone, PartialEq, Deserialize, Default, Serialize, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Context {
    // individual
    SubjectId,
    SubjectSex,
    DateOfBirth,
    VitalStatus,
    DateAtLastEncounter,
    AgeAtLastEncounter,
    WeightInKg,
    DateOfDeath,
    AgeOfDeath,
    CauseOfDeath,
    SurvivalTimeDays,

    // ontologies and databases
    HpoLabelOrId,
    OmimLabelOrId,
    OrphanetLabelOrId,
    MondoLabelOrId,
    HgncSymbolOrId,
    GenoLabelOrId,

    // variants
    Hgvs,

    // measurements
    QuantitativeMeasurement {
        loinc_id: String,
        unit_ontology_id: String,
    },
    QualitativeMeasurement {
        loinc_id: String,
        unit_ontology_prefix: String,
    },

    // other
    ObservationStatus,
    MultiHpoId,
    OnsetDate,
    OnsetAge,
    #[default]
    None,
    //...
}

impl Display for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Identical to Context, except that the customisable fields from contexts are stripped
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Default)]
pub enum ContextType {
    // individual
    SubjectId,
    SubjectSex,
    DateOfBirth,
    VitalStatus,
    DateAtLastEncounter,
    AgeAtLastEncounter,
    WeightInKg,
    DateOfDeath,
    AgeOfDeath,
    CauseOfDeath,
    SurvivalTimeDays,

    // ontologies and databases
    HpoLabelOrId,
    OmimLabelOrId,
    OrphanetLabelOrId,
    MondoLabelOrId,
    HgncSymbolOrId,
    GenoLabelOrId,

    // variants
    Hgvs,

    // measurements
    QuantitativeMeasurement,
    QualitativeMeasurement,

    // other
    ObservationStatus,
    MultiHpoId,
    OnsetDate,
    OnsetAge,
    #[default]
    None,
    //...
}

impl From<&Context> for ContextType {
    fn from(context: &Context) -> Self {
        match context {
            // individual
            Context::SubjectId => ContextType::SubjectId,
            Context::SubjectSex => ContextType::SubjectSex,
            Context::DateOfBirth => ContextType::DateOfBirth,
            Context::VitalStatus => ContextType::VitalStatus,
            Context::DateAtLastEncounter => ContextType::DateAtLastEncounter,
            Context::AgeAtLastEncounter => ContextType::AgeAtLastEncounter,
            Context::WeightInKg => ContextType::WeightInKg,
            Context::DateOfDeath => ContextType::DateOfDeath,
            Context::AgeOfDeath => ContextType::AgeOfDeath,
            Context::CauseOfDeath => ContextType::CauseOfDeath,
            Context::SurvivalTimeDays => ContextType::SurvivalTimeDays,

            // ontologies and databases
            Context::HpoLabelOrId => ContextType::HpoLabelOrId,
            Context::OmimLabelOrId => ContextType::OmimLabelOrId,
            Context::OrphanetLabelOrId => ContextType::OrphanetLabelOrId,
            Context::MondoLabelOrId => ContextType::MondoLabelOrId,
            Context::HgncSymbolOrId => ContextType::HgncSymbolOrId,
            Context::GenoLabelOrId => ContextType::GenoLabelOrId,

            // variants
            Context::Hgvs => ContextType::Hgvs,

            // measurements
            Context::QuantitativeMeasurement { .. } => ContextType::QuantitativeMeasurement,
            Context::QualitativeMeasurement { .. } => ContextType::QualitativeMeasurement,

            // other
            Context::ObservationStatus => ContextType::ObservationStatus,
            Context::MultiHpoId => ContextType::MultiHpoId,
            Context::OnsetDate => ContextType::OnsetDate,
            Context::OnsetAge => ContextType::OnsetAge,
            Context::None => ContextType::None,
        }
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
    Context::OnsetDate,
    Context::DateOfDeath,
];

pub const AGE_CONTEXTS: [Context; 3] = [
    Context::AgeAtLastEncounter,
    Context::OnsetAge,
    Context::AgeOfDeath,
];
