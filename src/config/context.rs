#![allow(unused_assignments)]
use enum_try_as_inner::EnumTryAsInner;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use strum_macros::Display;
use strum_macros::EnumDiscriminants;

/// Defines the semantic meaning or type of data in a column (either the header or the data itself).
///
/// This enum is used to tag data with a specific, machine-readable context,
/// such as identifying a column as containing HPO IDs or subject's sex.
///
#[derive(
    Debug,
    Clone,
    PartialEq,
    Deserialize,
    Default,
    Serialize,
    Hash,
    Eq,
    EnumDiscriminants,
    EnumTryAsInner,
)]
#[derive_err(Debug)]
#[strum_discriminants(name(ContextKind))]
#[strum_discriminants(derive(Display, Deserialize, Serialize))]
#[strum_discriminants(
    doc = "ContextKind is the same as Context, but all variants have their fields stripped. This is useful if you want to consider e.g. the QuantitativeMeasurement variant as a whole as opposed to a specific instance of it."
)]
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
    DiseaseLabelOrId,
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
    ReferenceRangeLow,
    ReferenceRangeHigh,

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

// context constants

pub const DISEASE_LABEL_OR_ID_CONTEXTS: [Context; 3] = [
    Context::DiseaseLabelOrId,
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
