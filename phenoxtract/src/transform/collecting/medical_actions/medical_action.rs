use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::transform::error::CollectorError;
use polars::datatypes::StringChunked;

pub(super) struct MedicalActionData {
    pub(super) treatment_target_col: Option<StringChunked>,
    pub(super) treatment_intent_col: Option<StringChunked>,
    pub(super) response_to_treatment_col: Option<StringChunked>,
    pub(super) treatment_termination_reason_col: Option<StringChunked>,
}

pub(super) struct MedicalAction<'a> {
    pub(super) treatment_target: Option<&'a str>,
    pub(super) treatment_intent: Option<&'a str>,
    pub(super) response_to_treatment: Option<&'a str>,
    pub(super) treatment_termination_reason: Option<&'a str>,
}

impl MedicalActionData {
    pub(super) fn new(
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
pub(super) struct Procedure<'a> {
    pub(super) procedure: Option<&'a str>,
    pub(super) body_part: Option<&'a str>,
    pub(super) time_element: Option<&'a str>,
}
pub(super) struct ProcedureData {
    pub(super) procedure_col: StringChunked,
    pub(super) body_part_col: Option<StringChunked>,
    pub(super) time_element_col: Option<StringChunked>,
}

impl ProcedureData {
    pub(super) fn new(
        patient_cdf: &ContextualizedDataFrame,
        building_block: Option<&str>,
    ) -> Result<Self, CollectorError> {
        match patient_cdf
            .get_single_linked_column_as_str(building_block, &[Context::ProcedureLabelOrId])?
        {
            None => Err(CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                table_name: patient_cdf.context().name().to_string(),
                bb_id: building_block
                    .unwrap_or("Missing Building Block")
                    .to_string(),
                contexts: vec![Context::ProcedureLabelOrId],
                n_found: 0,
                n_expected: 1,
            }),
            Some(procedure_col) => Ok(Self {
                procedure_col,
                body_part_col: patient_cdf.get_single_linked_column_as_str(
                    building_block,
                    &[Context::ProcedureBodySite],
                )?,
                time_element_col: patient_cdf.get_single_linked_column_as_str(
                    building_block,
                    &[Context::ProcedureTimeElement],
                )?,
            }),
        }
    }

    pub(super) fn get(&'_ self, idx: usize) -> Procedure<'_> {
        Procedure {
            procedure: self.procedure_col.as_ref().get(idx),
            body_part: self.body_part_col.as_ref().and_then(|col| col.get(idx)),
            time_element: self.time_element_col.as_ref().and_then(|col| col.get(idx)),
        }
    }
}
