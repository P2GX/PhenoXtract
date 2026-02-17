use crate::config::context::{Boundary, Context, ContextKind};
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
        data_context: &ContextKind,
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
                context: *data_context,
            }
        })?;

        let scs = patient_cdf
            .filter_series_context()
            .where_data_context_kind(Filter::Is(data_context))
            .where_building_block(Filter::Is(bb))
            .collect();

        if scs.len() == 1 {
            let quantity_sc = scs.first().unwrap();
            let unit = quantity_sc
                .get_data_context()
                .try_as_cumulative_dose()
                .expect("Cumulative dose should be a Cumulative dose");

            let contextualized_context = match data_context {
                ContextKind::DoseIntervalQuantity => {
                    Ok::<Context, CollectorError>(Context::DoseIntervalQuantity {
                        unit_ontology_id: unit.to_string(),
                    })
                }
                ContextKind::CumulativeDose => Ok(Context::CumulativeDose {
                    unit_ontology_id: unit.to_string(),
                }),
                _ => {
                    return Err(CollectorError::UnexpectedContextError(
                        *data_context,
                        quantity_sc.get_identifier().clone(),
                    ));
                }
            }?;

            let values = match patient_cdf
                .get_single_linked_column_as_float(building_block, &[contextualized_context])?
            {
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

            let reference_range_low = patient_cdf.get_single_linked_column_as_float(
                building_block,
                &[Context::ReferenceRange(Boundary::Start)],
            )?;

            let reference_range_high = patient_cdf.get_single_linked_column_as_float(
                building_block,
                &[Context::ReferenceRange(Boundary::End)],
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

    pub(super) fn get(&'_ self, idx: usize) -> Option<Quantity<'_>> {
        let mut range: Option<(f64, f64)> = None;
        if let (Some(start), Some(end)) = (&self.reference_range_start, &self.reference_range_end) {
            let a = start.get(idx);
            let b = end.get(idx);

            if let (Some(a), Some(b)) = (a, b) {
                range = Some((a, b));
            }
        }

        self.value.get(idx).map(|value| Quantity {
            unit: &self.unit,
            value,
            reference_range: range,
        })
    }
}
