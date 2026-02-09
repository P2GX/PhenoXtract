use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::transform::error::CollectorError;
use polars::datatypes::StringChunked;

pub(crate) struct MedicalActionData {
    pub(crate) treatment_target_col: Option<StringChunked>,
    pub(crate) treatment_intent_col: Option<StringChunked>,
    pub(crate) response_to_treatment_col: Option<StringChunked>,
    pub(crate) treatment_termination_reason_col: Option<StringChunked>,
}

pub(super) struct MedicalAction<'a> {
    pub(super) treatment_target: Option<&'a str>,
    pub(super) treatment_intent: Option<&'a str>,
    pub(super) response_to_treatment: Option<&'a str>,
    pub(super) treatment_termination_reason: Option<&'a str>,
}

impl MedicalActionData {
    pub(crate) fn new(
        patient_cdf: &ContextualizedDataFrame,
        building_block: Option<&str>,
    ) -> Result<Self, CollectorError> {
        Ok(Self {
            treatment_target_col: patient_cdf
                .get_single_linked_column_as_str(building_block, &[Context::TreatmentTarget])?,
            treatment_intent_col: patient_cdf
                .get_single_linked_column_as_str(building_block, &[Context::TreatmentIntent])?,
            response_to_treatment_col: patient_cdf
                .get_single_linked_column_as_str(building_block, &[Context::ResponseToTreatment])?,
            treatment_termination_reason_col: patient_cdf.get_single_linked_column_as_str(
                building_block,
                &[Context::TreatmentTerminationReason],
            )?,
        })
    }

    pub(super) fn get(&'_ self, idx: usize) -> MedicalAction<'_> {
        MedicalAction {
            treatment_target: self
                .treatment_target_col
                .as_ref()
                .and_then(|col| col.get(idx)),
            treatment_intent: self
                .treatment_intent_col
                .as_ref()
                .and_then(|col| col.get(idx)),
            response_to_treatment: self
                .response_to_treatment_col
                .as_ref()
                .and_then(|col| col.get(idx)),
            treatment_termination_reason: self
                .treatment_termination_reason_col
                .as_ref()
                .and_then(|col| col.get(idx)),
        }
    }
}
