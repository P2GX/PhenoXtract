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
    pub(super) reference_range_start: Option<Float64Chunked>,
    pub(super) reference_range_end: Option<Float64Chunked>,
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
        let reference_range_low = patient_cdf.get_single_linked_column_as_float(
            Some(building_block),
            &[Context::ReferenceRange(Boundary::Start)],
        )?;
        let reference_range_high = patient_cdf.get_single_linked_column_as_float(
            Some(building_block),
            &[Context::ReferenceRange(Boundary::End)],
        )?;

        Self::linked_col_guard(
            patient_cdf,
            building_block,
            &values,
            &unit,
            &reference_range_low,
            &reference_range_high,
        )?;

        if let Some(values) = values
            && let Some(units) = unit
        {
            Ok(Some(Self {
                unit: units,
                value: values,
                reference_range_start: reference_range_low,
                reference_range_end: reference_range_high,
            }))
        } else {
            Ok(None)
        }
    }

    fn linked_col_guard(
        patient_cdf: &ContextualizedDataFrame,
        building_block: &str,
        values: &Option<Float64Chunked>,
        unit: &Option<StringChunked>,
        reference_range_low: &Option<Float64Chunked>,
        reference_range_high: &Option<Float64Chunked>,
    ) -> Result<(), CollectorError> {
        if values.is_none() || unit.is_none() {
            // TODO: Fill error
            return Err(CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                table_name: patient_cdf.context().name().to_string(),
                bb_id: building_block.to_string(),
                contexts: vec![],
                n_found: 0,
                n_expected: 0,
            });
        } else if reference_range_low.is_none() && reference_range_high.is_some()
            || reference_range_low.is_some() && unit.is_some() && reference_range_high.is_none()
        {
            // TODO: Fill error
            return Err(CollectorError::RequiredValueMissingError);
        }

        Ok(())
    }
}

impl Getter for QuantityData {
    type Item<'a> = Quantity<'a>;

    fn get(&'_ self, idx: usize) -> Result<Option<Quantity<'_>>, GetterError> {
        if self.len() <= idx {
            return Err(GetterError::OutOfBounds);
        }
        let mut range: Option<(f64, f64)> = None;
        if let (Some(start), Some(end)) = (&self.reference_range_start, &self.reference_range_end) {
            let interval_start = start.get(idx);
            let interval_end = end.get(idx);

            if let (Some(start), Some(end)) = (interval_start, interval_end) {
                range = Some((start, end));
            }
        }

        // TODO: This should throw an error, if either reference_range is found and value isn't
        Ok(Some(Quantity {
            unit: self.unit.get(idx).unwrap(),
            value: self.value.get(idx).unwrap(),
            reference_range: range,
        }))
    }

    fn len(&self) -> usize {
        self.value.len()
    }
}
