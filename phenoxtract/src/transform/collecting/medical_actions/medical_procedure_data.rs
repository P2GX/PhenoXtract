use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::transform::collecting::traits::GetRows;
use crate::transform::error::{CollectorError, GetterError};
use polars::datatypes::StringChunked;
use std::collections::HashSet;

pub(super) struct ProcedureRow<'a> {
    pub(super) procedure: &'a str,
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
    ) -> Result<Option<Self>, CollectorError> {
        let procedure =
            patient_cdf.get_single_linked_column_as_str(building_block, &[Context::Procedure])?;
        let body_part_col = patient_cdf
            .get_single_linked_column_as_str(building_block, &[Context::ProcedureBodySite])?;
        let time_element_col = patient_cdf
            .get_single_linked_column_as_str(building_block, Context::TIME_OF_PROCEDURE_VARIANTS)?;

        match procedure {
            Some(procedure) => Ok(Some(Self {
                procedure_col: procedure,
                body_part_col,
                time_element_col,
            })),
            None if body_part_col.is_some() || time_element_col.is_some() => {
                let found_contexts = [
                    body_part_col
                        .as_ref()
                        .map(|_| vec![Context::ProcedureBodySite]),
                    time_element_col
                        .as_ref()
                        .map(|_| Context::TIME_OF_PROCEDURE_VARIANTS.to_vec()),
                ]
                .into_iter()
                .flatten()
                .flatten()
                .collect::<Vec<_>>();

                Err(CollectorError::ExpectedLinkedContexts {
                    bb_id: building_block.unwrap_or("No Building Block").to_string(),
                    expected_contexts: vec![Context::Procedure],
                    found_contexts,
                })
            }
            None => Ok(None),
        }
    }
}

impl GetRows for ProcedureData {
    type Item<'a> = ProcedureRow<'a>;

    fn construct_data_unchecked(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError> {
        if let Some(procedure) = self.procedure_col.as_ref().get(idx) {
            Ok(Some(ProcedureRow {
                procedure,
                body_part: self.body_part_col.as_ref().and_then(|col| col.get(idx)),
                time_element: self.time_element_col.as_ref().and_then(|col| col.get(idx)),
            }))
        } else {
            Ok(None)
        }
    }

    fn len(&self) -> usize {
        self.procedure_col.len()
    }
}
