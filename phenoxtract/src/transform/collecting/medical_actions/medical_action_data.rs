use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::transform::collecting::traits::GetRows;
use crate::transform::error::{CollectorError, GetterError};
use polars::datatypes::StringChunked;

pub(super) struct MedicalActionRow<'a> {
    pub(super) treatment_target: Option<&'a str>,
    pub(super) treatment_intent: Option<&'a str>,
    pub(super) response_to_treatment: Option<&'a str>,
    pub(super) treatment_termination_reason: Option<&'a str>,
}

pub(super) struct MedicalActionData {
    pub(super) treatment_target_col: Option<StringChunked>,
    pub(super) treatment_intent_col: Option<StringChunked>,
    pub(super) response_to_treatment_col: Option<StringChunked>,
    pub(super) treatment_termination_reason_col: Option<StringChunked>,
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
}

impl GetRows for MedicalActionData {
    type Item<'a> = MedicalActionRow<'a>;

    fn construct_data_unchecked(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError> {
        if self.treatment_target_col.is_none()
            && self.treatment_intent_col.is_none()
            && self.treatment_termination_reason_col.is_none()
            && self.response_to_treatment_col.is_none()
        {
            return Ok(None);
        }

        Ok(Some(MedicalActionRow {
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
        }))
    }

    fn len(&self) -> usize {
        if let Some(tt_col) = &self.treatment_target_col {
            tt_col.len()
        } else if let Some(ti_col) = &self.treatment_intent_col {
            ti_col.len()
        } else if let Some(reason_col) = &self.treatment_termination_reason_col {
            reason_col.len()
        } else if let Some(response_col) = &self.response_to_treatment_col {
            response_col.len()
        } else {
            0
        }
    }
}
