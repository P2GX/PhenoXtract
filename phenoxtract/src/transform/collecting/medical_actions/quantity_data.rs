use crate::config::context::{Context, ContextKind};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::error::CollectorError;
use polars::prelude::Float64Chunked;

pub(super) struct Quantity<'a> {
    pub(super) unit: &'a str,
    pub(super) value: f64,
    pub(super) reference_range: Option<(f64, f64)>,
}

pub(super) struct QuantityData {
    pub(super) unit: String,
    pub(super) value: Float64Chunked,
    pub(super) reference_range_start: Option<Float64Chunked>,
    pub(super) reference_range_end: Option<Float64Chunked>,
}

impl QuantityData {
    pub(super) fn new(
        patient_cdf: &ContextualizedDataFrame,
        building_block: Option<&str>,
    ) -> Result<Option<Self>, CollectorError> {
        let bb = building_block.ok_or_else(|| {
            let patient_id = patient_cdf
                .get_subject_id_col()
                .get(0)
                .expect("CDF should always have patient id.")
                .to_string();
            CollectorError::ExpectedBuildingBlock {
                table_name: patient_cdf.context().name().to_string(),
                patient_id,
                context: ContextKind::CumulativeDose,
            }
        })?;

        let scs = patient_cdf
            .filter_series_context()
            .where_data_context_kind(Filter::Is(&ContextKind::CumulativeDose))
            .where_building_block(Filter::Is(bb))
            .collect();

        if scs.len() == 1 {
            let quantity_sc = scs.first().unwrap();
            let unit = quantity_sc
                .get_data_context()
                .try_as_cumulative_dose()
                .expect("Cumulative dose should be a Cumulative dose");

            let values = match patient_cdf.get_single_linked_column_as_float(
                building_block,
                &[Context::CumulativeDose {
                    unit_ontology_id: unit.to_string(),
                }],
            )? {
                None => Err(CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                    table_name: patient_cdf.context().name().to_string(),
                    bb_id: bb.to_string(),
                    contexts: vec![Context::CumulativeDose {
                        unit_ontology_id: unit.to_string(),
                    }],
                    n_found: 0,
                    n_expected: 1,
                }),
                Some(val) => Ok(val),
            }?;

            let reference_range_low = patient_cdf
                .get_single_linked_column_as_float(building_block, &[Context::ReferenceRangeLow])?;

            let reference_range_high = patient_cdf.get_single_linked_column_as_float(
                building_block,
                &[Context::ReferenceRangeHigh],
            )?;

            Ok(Some(Self {
                unit: unit.to_string(),
                value: values,
                reference_range_start: reference_range_low,
                reference_range_end: reference_range_high,
            }))
        } else {
            Ok(None)
        }
    }

    pub(super) fn get(&self, idx: usize) -> Option<Quantity> {
        let mut range: Option<(f64, f64)> = None;
        if let (Some(start), Some(end)) = (&self.reference_range_start, &self.reference_range_end) {
            let a = start.get(idx);
            let b = end.get(idx);

            if let (Some(a), Some(b)) = (a, b) {
                range = Some((a, b));
            }
        }

        if let Some(value) = self.value.get(idx) {
            Some(Quantity {
                unit: &self.unit,
                value,
                reference_range: range,
            })
        } else {
            None
        }
    }
}
