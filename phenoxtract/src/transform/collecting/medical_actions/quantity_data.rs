use crate::config::context::{Boundary, Context};
use crate::extract::ContextualizedDataFrame;

use crate::transform::collecting::traits::Getter;
use crate::transform::error::{CollectorError, GetterError};
use polars::datatypes::StringChunked;
use polars::prelude::Float64Chunked;

#[derive(Debug)]
pub(super) struct Quantity<'a> {
    pub(super) unit: &'a str,
    pub(super) value: f64,
    pub(super) reference_range: Option<(f64, f64)>,
}

pub(super) struct QuantityData {
    pub(super) unit: StringChunked,
    pub(super) value: Float64Chunked,
    pub(super) reference_range: Option<(Float64Chunked, Float64Chunked)>,
}

impl QuantityData {
    pub(super) fn new(
        patient_cdf: &ContextualizedDataFrame,
        building_block: &str,
    ) -> Result<Option<Self>, CollectorError> {
        let values = patient_cdf
            .get_single_linked_column_as_float(Some(building_block), &[Context::QuantityValue])?;
        let unit = patient_cdf
            .get_single_linked_column_as_str(Some(building_block), &[Context::QuantityUnit])?;

        let (values, unit) = match (values, unit) {
            (Some(v), Some(u)) => (v, u),
            (None, None) => return Ok(None),
            _ => {
                // TODO Refine error
                return Err(CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                    table_name: patient_cdf.context().name().to_string(),
                    bb_id: building_block.to_string(),
                    contexts: vec![Context::QuantityValue, Context::QuantityUnit],
                    n_found: 1,
                    n_expected: 2,
                });
            }
        };

        let reference_range = Self::parse_reference_range(patient_cdf, building_block)?;

        Ok(Some(Self {
            unit,
            value: values,
            reference_range,
        }))
    }

    fn parse_reference_range(
        patient_cdf: &ContextualizedDataFrame,
        building_block: &str,
    ) -> Result<Option<(Float64Chunked, Float64Chunked)>, CollectorError> {
        let low = patient_cdf.get_single_linked_column_as_float(
            Some(building_block),
            &[Context::ReferenceRange(Boundary::Start)],
        )?;
        let high = patient_cdf.get_single_linked_column_as_float(
            Some(building_block),
            &[Context::ReferenceRange(Boundary::End)],
        )?;

        match (low, high) {
            (Some(low), Some(high)) => Ok(Some((low, high))),
            (None, None) => Ok(None),
            // TODO: Refine error
            _ => Err(CollectorError::RequiredValueMissingError),
        }
    }
}

impl Getter for QuantityData {
    type Item<'a> = Quantity<'a>;

    fn get(&'_ self, idx: usize) -> Result<Option<Quantity<'_>>, GetterError> {
        if self.len() <= idx {
            return Err(GetterError::OutOfBounds);
        }

        let value = self
            .value
            .get(idx)
            .ok_or(GetterError::RequiredValueMissingError {
                n_required: 1,
                context: "QuantityValue".to_string(),
            })?;
        let unit = self
            .unit
            .get(idx)
            .ok_or(GetterError::RequiredValueMissingError {
                n_required: 1,
                context: "QuantityUnit".to_string(),
            })?;

        let reference_range = match &self.reference_range {
            Some((start, end)) => {
                let interval_start = start.get(idx);
                let interval_end = end.get(idx);

                if let (Some(start), Some(end)) = (interval_start, interval_end) {
                    Some((start, end))
                } else if interval_start.is_none() && interval_end.is_none() {
                    None
                } else {
                    return Err(GetterError::RequiredValueMissingError {
                        n_required: 2,
                        context: "ReferenceRange".to_string(),
                    });
                }
            }
            _ => None,
        };

        Ok(Some(Quantity {
            unit,
            value,
            reference_range,
        }))
    }

    fn len(&self) -> usize {
        self.value.len()
    }
}
