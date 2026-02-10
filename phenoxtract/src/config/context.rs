#![allow(unused_assignments)]
use enum_try_as_inner::EnumTryAsInner;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use strum_macros::Display;
use strum_macros::EnumDiscriminants;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Hash, Eq)]
enum TimeElementType {
    GestationalAge,
    Age,
    OntologyClass,
    Timestamp,
    TimeIntervalStart,
    TimeIntervalEnd,
    AgeRangeStart,
    AgeRangeEnd,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Hash, Eq)]
enum Boundary {
    Start,
    End,
}
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
    TimeAtLastEncounter(TimeElementType),
    TimeOfDeath(TimeElementType),
    CauseOfDeath,
    SurvivalTimeDays,

    // ontologies and databases
    HpoLabelOrId,
    DiseaseLabelOrId,
    HgncSymbolOrId,

    // variants
    Hgvs,

    // measurements
    QuantitativeMeasurement {
        assay_id: String,
        unit_ontology_id: String,
    },
    QualitativeMeasurement {
        assay_id: String,
    },
    ReferenceRange(Boundary),

    // other
    ObservationStatus,
    MultiHpoId,
    OnsetTime(TimeElementType),
    ReleaseTime(TimeElementType),
    #[default]
    None,
    //...
}

impl Display for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

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
