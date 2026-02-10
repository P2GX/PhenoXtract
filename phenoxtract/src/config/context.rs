#![allow(unused_assignments)]
use enum_try_as_inner::EnumTryAsInner;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use strum::IntoEnumIterator;
use strum_macros::EnumDiscriminants;
use strum_macros::{Display, EnumIter};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Hash, Eq, EnumIter)]
#[serde(rename_all = "snake_case")]
pub enum TimeElementType {
    Age,
    Date,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Hash, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Boundary {
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
#[strum_discriminants(derive(Display, Deserialize, Serialize, EnumIter))]
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
    LastEncounter(TimeElementType),
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

    // Medical Actions
    TreatmentTarget,
    TreatmentIntent,
    ResponseToTreatment,
    TreatmentTerminationReason,

    ProcedureLabelOrId,
    ProcedureBodySite,
    TimeOfProcedure(TimeElementType),

    // other
    ObservationStatus,
    MultiHpoId,
    Onset(TimeElementType),

    #[default]
    None,
    //...
}

macro_rules! time_element_contexts {
    ($context_variant:ident) => {{
        #[allow(dead_code, unused_variables)]
        fn assert_exhaustive(t: TimeElementType) {
            match t {
                TimeElementType::Age => {}
                TimeElementType::Date => {}
            }
        }

        &[
            Context::$context_variant(TimeElementType::Age),
            Context::$context_variant(TimeElementType::Date),
        ]
    }};
}

impl Context {
    pub const LAST_ENCOUNTER_VARIANTS: &'static [Context] = time_element_contexts!(LastEncounter);
    pub const TIME_OF_DEATH_VARIANTS: &'static [Context] = time_element_contexts!(TimeOfDeath);
    pub const TIME_OF_PROCEDURE_VARIANTS: &'static [Context] =
        time_element_contexts!(TimeOfProcedure);
    pub const ONSET_VARIANTS: &'static [Context] = time_element_contexts!(Onset);
    pub fn all_time_based(tt: TimeElementType) -> Vec<Context> {
        ContextKind::iter()
            .filter_map(|kind| match kind {
                ContextKind::LastEncounter => Some(Context::LastEncounter(tt.clone())),
                ContextKind::TimeOfDeath => Some(Context::TimeOfDeath(tt.clone())),
                ContextKind::TimeOfProcedure => Some(Context::TimeOfProcedure(tt.clone())),
                ContextKind::Onset => Some(Context::Onset(tt.clone())),

                // Ensures that we see a compile error, when we add another context using a TimeElementType
                ContextKind::SubjectId
                | ContextKind::SubjectSex
                | ContextKind::DateOfBirth
                | ContextKind::VitalStatus
                | ContextKind::CauseOfDeath
                | ContextKind::SurvivalTimeDays
                | ContextKind::HpoLabelOrId
                | ContextKind::DiseaseLabelOrId
                | ContextKind::HgncSymbolOrId
                | ContextKind::Hgvs
                | ContextKind::QuantitativeMeasurement
                | ContextKind::QualitativeMeasurement
                | ContextKind::ReferenceRange
                | ContextKind::TreatmentTarget
                | ContextKind::TreatmentIntent
                | ContextKind::ResponseToTreatment
                | ContextKind::TreatmentTerminationReason
                | ContextKind::ProcedureLabelOrId
                | ContextKind::ProcedureBodySite
                | ContextKind::ObservationStatus
                | ContextKind::MultiHpoId
                | ContextKind::None => None,
            })
            .collect()
    }
}

impl Display for Context {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
