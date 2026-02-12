use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::transform::error::CollectorError;
use polars::datatypes::StringChunked;

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
                    &[Context::AgeAtProcedure, Context::DateOfProcedure],
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
