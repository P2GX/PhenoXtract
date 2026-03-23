#![allow(dead_code)]
use crate::config::context::{Boundary, Context, ContextKind};
use crate::extract::ContextualizedDataFrame;

use crate::transform::collecting::traits::Getter;
use crate::transform::error::{CollectorError, GetterError};
use polars::datatypes::StringChunked;
use polars::prelude::Float64Chunked;

#[derive(Debug)]
pub(super) struct QuantityRow<'a> {
    pub(super) unit: &'a str,
    pub(super) value: f64,
    pub(super) reference_range: Option<(f64, f64)>,
}
#[derive(Debug)]
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
            _ => Err(CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                table_name: patient_cdf.context().name().to_string(),
                bb_id: building_block.to_string(),
                contexts: vec![
                    Context::ReferenceRange(Boundary::Start),
                    Context::ReferenceRange(Boundary::End),
                ],
                n_found: 1,
                n_expected: 2,
            }),
        }
    }
}

impl Getter for QuantityData {
    type Item<'a> = QuantityRow<'a>;

    fn construct_data(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError> {
        let value = self.value.get(idx);
        let unit = self.unit.get(idx);

        let (value, unit) = match (value, unit) {
            (Some(v), Some(u)) => (v, u),
            (None, None) => return Ok(None),
            (Some(_), None) => {
                return Err(GetterError::RequiredValueMissingError {
                    idx,
                    context: ContextKind::QuantityUnit,
                });
            }
            (None, Some(_)) => {
                return Err(GetterError::RequiredValueMissingError {
                    idx,
                    context: ContextKind::QuantityValue,
                });
            }
        };

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
                        idx,
                        context: ContextKind::ReferenceRange,
                    });
                }
            }
            _ => None,
        };

        Ok(Some(QuantityRow {
            unit,
            value,
            reference_range,
        }))
    }

    fn len(&self) -> usize {
        self.value.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::SeriesContext;
    use crate::config::traits::SeriesContextBuilding;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::test_suite::phenopacket_component_generation::default_unit_oc;
    use polars::datatypes::AnyValue;
    use polars::prelude::{Column, IntoColumn, NamedFrom, Series};
    use rstest::{fixture, rstest};

    #[fixture]
    fn building_block() -> &'static str {
        "bb_1"
    }

    #[fixture]
    fn reference_range_start(building_block: &str) -> (SeriesContext, Column) {
        let col = Series::new(
            "ref_range_start".into(),
            &[AnyValue::Float64(0.0), AnyValue::Float64(1.0)],
        )
        .into_column();
        let sc = SeriesContext::from_identifier("ref_range_start")
            .with_data_context(Context::ReferenceRange(Boundary::Start))
            .with_building_block_id(building_block);
        (sc, col)
    }

    #[fixture]
    fn reference_range_end(building_block: &str) -> (SeriesContext, Column) {
        let col = Series::new(
            "ref_range_end".into(),
            &[AnyValue::Float64(0.0), AnyValue::Float64(1.0)],
        )
        .into_column();
        let sc = SeriesContext::from_identifier("ref_range_end")
            .with_data_context(Context::ReferenceRange(Boundary::End))
            .with_building_block_id(building_block);
        (sc, col)
    }

    #[fixture]
    fn cdf(building_block: &str) -> ContextualizedDataFrame {
        let mut patient_cdf = generate_minimal_cdf(1, 2);

        let values = Series::new(
            "values".into(),
            &[AnyValue::Float64(0.5), AnyValue::Float64(0.5)],
        );

        let units = Series::new(
            "unit".into(),
            &[
                AnyValue::String(&default_unit_oc().id),
                AnyValue::String(&default_unit_oc().label),
            ],
        );
        patient_cdf
            .builder()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("values")
                    .with_data_context(Context::QuantityValue)
                    .with_building_block_id(building_block.to_string()),
                vec![values.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("unit")
                    .with_data_context(Context::QuantityUnit)
                    .with_building_block_id(building_block.to_string()),
                vec![units.into_column()].as_ref(),
            )
            .unwrap()
            .build()
            .unwrap();

        patient_cdf
    }

    #[rstest]
    fn test_quantity_data_new(cdf: ContextualizedDataFrame, building_block: &str) {
        let quant_data = QuantityData::new(&cdf, building_block).unwrap().unwrap();

        assert_eq!(quant_data.value.len(), 2);
        assert_eq!(quant_data.unit.len(), 2);
        assert!(quant_data.reference_range.is_none());
    }

    #[rstest]
    fn test_quantity_data_missing_value_col(
        mut cdf: ContextualizedDataFrame,
        building_block: &str,
    ) {
        cdf.builder()
            .drop_scs_alongside_cols_with_context(&Context::None, &Context::QuantityValue)
            .unwrap()
            .build()
            .unwrap();

        let quant_data = QuantityData::new(&cdf, building_block);

        assert!(quant_data.is_err());
    }

    #[rstest]
    fn test_quantity_data_missing_unit_col(mut cdf: ContextualizedDataFrame, building_block: &str) {
        cdf.builder()
            .drop_scs_alongside_cols_with_context(&Context::None, &Context::QuantityUnit)
            .unwrap()
            .build()
            .unwrap();

        let quant_data = QuantityData::new(&cdf, building_block);

        assert!(quant_data.is_err());
    }

    #[rstest]
    fn test_quantity_data_not_configured(mut cdf: ContextualizedDataFrame, building_block: &str) {
        cdf.builder()
            .drop_scs_alongside_cols_with_context(&Context::None, &Context::QuantityUnit)
            .unwrap()
            .drop_scs_alongside_cols_with_context(&Context::None, &Context::QuantityValue)
            .unwrap()
            .build()
            .unwrap();

        let quant_data = QuantityData::new(&cdf, building_block);

        assert!(quant_data.is_ok());
        assert!(quant_data.unwrap().is_none())
    }

    #[rstest]
    fn test_quantity_data_reference_range(
        mut cdf: ContextualizedDataFrame,
        building_block: &str,
        reference_range_start: (SeriesContext, Column),
        reference_range_end: (SeriesContext, Column),
    ) {
        let (start_sc, start_col) = reference_range_start;
        let (end_sc, end_col) = reference_range_end;

        cdf.builder()
            .insert_sc_alongside_cols(start_sc, vec![start_col].as_ref())
            .unwrap()
            .insert_sc_alongside_cols(end_sc, vec![end_col].as_ref())
            .unwrap()
            .build()
            .unwrap();

        let quant_data = QuantityData::new(&cdf, building_block).unwrap().unwrap();

        assert_eq!(quant_data.value.len(), 2);
        assert_eq!(quant_data.unit.len(), 2);
        assert!(quant_data.reference_range.is_some());
    }

    #[rstest]
    fn test_quantity_data_reference_range_start_missing(
        mut cdf: ContextualizedDataFrame,
        building_block: &str,
        reference_range_end: (SeriesContext, Column),
    ) {
        let (end_sc, end_col) = reference_range_end;

        cdf.builder()
            .insert_sc_alongside_cols(end_sc, vec![end_col].as_ref())
            .unwrap()
            .build()
            .unwrap();

        let quant_data = QuantityData::new(&cdf, building_block);

        assert!(quant_data.is_err());
    }
    #[rstest]
    fn test_quantity_data_reference_range_end_missing(
        mut cdf: ContextualizedDataFrame,
        building_block: &str,
        reference_range_start: (SeriesContext, Column),
    ) {
        let (start_sc, start_col) = reference_range_start;

        cdf.builder()
            .insert_sc_alongside_cols(start_sc, vec![start_col].as_ref())
            .unwrap()
            .build()
            .unwrap();

        let quant_data = QuantityData::new(&cdf, building_block);

        assert!(quant_data.is_err());
    }
}

#[cfg(test)]
mod getter_tests {
    use super::*;
    use crate::config::context::{Boundary, Context};
    use crate::config::table_context::SeriesContext;
    use crate::config::traits::SeriesContextBuilding;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::test_suite::phenopacket_component_generation::default_unit_oc;
    use polars::datatypes::AnyValue;
    use polars::prelude::{IntoColumn, NamedFrom, Series};
    use rstest::{fixture, rstest};

    #[fixture]
    fn building_block() -> &'static str {
        "bb_1"
    }

    #[fixture]
    fn quantity_data(building_block: &str) -> QuantityData {
        let mut cdf = generate_minimal_cdf(1, 2);

        let values = Series::new(
            "values".into(),
            &[AnyValue::Float64(1.0), AnyValue::Float64(2.0)],
        );
        let units = Series::new(
            "unit".into(),
            &[
                AnyValue::String(&default_unit_oc().id),
                AnyValue::String(&default_unit_oc().id),
            ],
        );

        cdf.builder()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("values")
                    .with_data_context(Context::QuantityValue)
                    .with_building_block_id(building_block),
                &[values.into_column()],
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("unit")
                    .with_data_context(Context::QuantityUnit)
                    .with_building_block_id(building_block),
                &[units.into_column()],
            )
            .unwrap()
            .build()
            .unwrap();

        QuantityData::new(&cdf, building_block).unwrap().unwrap()
    }

    #[fixture]
    fn quantity_data_with_reference_range(building_block: &str) -> QuantityData {
        let mut cdf = generate_minimal_cdf(1, 2);

        let values = Series::new(
            "values".into(),
            &[AnyValue::Float64(1.5), AnyValue::Float64(2.5)],
        );
        let units = Series::new(
            "unit".into(),
            &[
                AnyValue::String(&default_unit_oc().id),
                AnyValue::String(&default_unit_oc().id),
            ],
        );
        let ref_start = Series::new(
            "ref_range_start".into(),
            &[AnyValue::Float64(0.0), AnyValue::Float64(1.0)],
        );
        let ref_end = Series::new(
            "ref_range_end".into(),
            &[AnyValue::Float64(5.0), AnyValue::Float64(10.0)],
        );

        cdf.builder()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("values")
                    .with_data_context(Context::QuantityValue)
                    .with_building_block_id(building_block),
                &[values.into_column()],
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("unit")
                    .with_data_context(Context::QuantityUnit)
                    .with_building_block_id(building_block),
                &[units.into_column()],
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("ref_range_start")
                    .with_data_context(Context::ReferenceRange(Boundary::Start))
                    .with_building_block_id(building_block),
                &[ref_start.into_column()],
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("ref_range_end")
                    .with_data_context(Context::ReferenceRange(Boundary::End))
                    .with_building_block_id(building_block),
                &[ref_end.into_column()],
            )
            .unwrap()
            .build()
            .unwrap();

        QuantityData::new(&cdf, building_block).unwrap().unwrap()
    }

    #[rstest]
    fn test_get_out_of_bounds_on_empty_index_equals_len(quantity_data: QuantityData) {
        // len() == 2, so index 2 is one past the end
        let result = quantity_data.get(quantity_data.len());
        assert!(matches!(result, Err(GetterError::OutOfBounds)));
    }

    #[rstest]
    fn test_get_out_of_bounds_well_past_end(quantity_data: QuantityData) {
        let result = quantity_data.get(usize::MAX);
        assert!(matches!(result, Err(GetterError::OutOfBounds)));
    }

    #[rstest]
    fn test_get_returns_some_for_valid_index(quantity_data: QuantityData) {
        let result = quantity_data.get(0).unwrap();
        assert!(result.is_some());
    }

    #[rstest]
    fn test_get_correct_value_first_row(quantity_data: QuantityData) {
        let item = quantity_data.get(0).unwrap().unwrap();
        assert_eq!(item.value, 1.0);
    }

    #[rstest]
    fn test_get_correct_value_second_row(quantity_data: QuantityData) {
        let item = quantity_data.get(1).unwrap().unwrap();
        assert_eq!(item.value, 2.0);
    }

    #[rstest]
    fn test_get_no_reference_range_when_not_configured(quantity_data: QuantityData) {
        let item = quantity_data.get(0).unwrap().unwrap();
        assert!(item.reference_range.is_none());
    }

    #[rstest]
    fn test_get_reference_range_present(quantity_data_with_reference_range: QuantityData) {
        let item = quantity_data_with_reference_range.get(0).unwrap().unwrap();
        assert!(item.reference_range.is_some());
    }

    #[rstest]
    fn test_get_reference_range_correct_values_first_row(
        quantity_data_with_reference_range: QuantityData,
    ) {
        let item = quantity_data_with_reference_range.get(0).unwrap().unwrap();
        let (start, end) = item.reference_range.unwrap();
        assert_eq!(start, 0.0);
        assert_eq!(end, 5.0);
    }

    #[rstest]
    fn test_get_reference_range_correct_values_second_row(
        quantity_data_with_reference_range: QuantityData,
    ) {
        let item = quantity_data_with_reference_range.get(1).unwrap().unwrap();
        let (start, end) = item.reference_range.unwrap();
        assert_eq!(start, 1.0);
        assert_eq!(end, 10.0);
    }
}
