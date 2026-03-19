use crate::config::context::{Boundary, Context, ContextKind};
use crate::extract::ContextualizedDataFrame;
use crate::transform::collecting::medical_actions::quantity_data::{Quantity, QuantityData};
use crate::transform::collecting::traits::Getter;
use crate::transform::error::{CollectorError, GetterError};
use polars::datatypes::StringChunked;

#[derive(Debug)]
pub struct DoseInterval<'a> {
    quantity: Quantity<'a>,
    schedule_frequency: &'a str,
    interval_start: &'a str,
    interval_end: &'a str,
}

pub(super) struct DoseIntervalData {
    quantity: QuantityData,
    schedule_frequency: StringChunked,
    interval_start: StringChunked,
    interval_end: StringChunked,
}

impl DoseIntervalData {
    pub(super) fn new(
        building_block: &str,
        patient_cdf: &ContextualizedDataFrame,
    ) -> Result<Option<Self>, CollectorError> {
        let quantity = QuantityData::new(patient_cdf, building_block)?;
        let schedule_frequency = patient_cdf.get_single_linked_column_as_str(
            Some(building_block),
            &[Context::DoseScheduleFrequency],
        )?;
        let interval_start = patient_cdf.get_single_linked_column_as_str(
            Some(building_block),
            &[Context::DoseInterval(Boundary::Start)],
        )?;
        let interval_end = patient_cdf.get_single_linked_column_as_str(
            Some(building_block),
            &[Context::DoseInterval(Boundary::End)],
        )?;

        match (quantity, schedule_frequency, interval_start, interval_end) {
            (None, None, None, None) => Ok(None),
            (
                Some(quantity),
                Some(schedule_frequency),
                Some(interval_start),
                Some(interval_end),
            ) => Ok(Some(DoseIntervalData {
                quantity,
                schedule_frequency,
                interval_start,
                interval_end,
            })),
            (quantity, schedule_frequency, interval_start, interval_end) => {
                let missing_contexts = [
                    quantity.is_none().then_some(Context::QuantityValue),
                    quantity.is_none().then_some(Context::QuantityUnit),
                    schedule_frequency
                        .is_none()
                        .then_some(Context::DoseScheduleFrequency),
                    interval_start
                        .is_none()
                        .then_some(Context::DoseInterval(Boundary::Start)),
                    interval_end
                        .is_none()
                        .then_some(Context::DoseInterval(Boundary::End)),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

                Err(CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                    table_name: patient_cdf.context().name().to_string(),
                    bb_id: building_block.to_string(),
                    n_found: 5 - missing_contexts.len(),
                    n_expected: 5,
                    contexts: missing_contexts,
                })
            }
        }
    }
}

impl Getter for DoseIntervalData {
    type Item<'a> = DoseInterval<'a>;

    fn construct_data(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError> {
        match (
            self.quantity.get(idx)?,
            self.schedule_frequency.get(idx),
            self.interval_start.get(idx),
            self.interval_end.get(idx),
        ) {
            (Some(quantity), Some(schedule_frequency), Some(start), Some(end)) => {
                Ok(Some(DoseInterval {
                    quantity,
                    schedule_frequency,
                    interval_start: start,
                    interval_end: end,
                }))
            }
            (None, None, None, None) => Ok(None),
            _ => Err(GetterError::RequiredValueMissingError {
                idx,
                context: ContextKind::DoseInterval,
            }),
        }
    }

    fn len(&self) -> usize {
        self.schedule_frequency.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::SeriesContext;
    use crate::config::traits::SeriesContextBuilding;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::test_suite::phenopacket_component_generation::{
        default_schedule_frequency, default_timestamp, default_unit_oc,
    };
    use polars::datatypes::AnyValue;
    use polars::prelude::{IntoColumn, NamedFrom, Series};
    use rstest::{fixture, rstest};

    #[fixture]
    fn building_block() -> String {
        "block_1".to_string()
    }

    #[fixture]
    fn cdf(building_block: String) -> ContextualizedDataFrame {
        let mut patient_cdf = generate_minimal_cdf(1, 2);

        let schedule_frequency = Series::new(
            "schedule_frequency".into(),
            &[
                AnyValue::String(&default_schedule_frequency().clone().label),
                AnyValue::String(&default_schedule_frequency().clone().label),
            ],
        );

        let interval_start = Series::new(
            "interval_start".into(),
            &[
                AnyValue::String(&default_timestamp().to_string()),
                AnyValue::String(&default_timestamp().to_string()),
            ],
        );

        let interval_end = Series::new(
            "interval_end".into(),
            &[
                AnyValue::String(&default_timestamp().to_string()),
                AnyValue::String(&default_timestamp().to_string()),
            ],
        );

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
                SeriesContext::from_identifier("schedule_frequency")
                    .with_data_context(Context::DoseScheduleFrequency)
                    .with_building_block_id(building_block.to_string()),
                vec![schedule_frequency.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("interval_start")
                    .with_data_context(Context::DoseInterval(Boundary::Start))
                    .with_building_block_id(building_block.to_string()),
                vec![interval_start.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("interval_end")
                    .with_data_context(Context::DoseInterval(Boundary::End))
                    .with_building_block_id(building_block.to_string()),
                vec![interval_end.into_column()].as_ref(),
            )
            .unwrap()
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
    #[fixture]
    fn dose_interval_data(
        cdf: ContextualizedDataFrame,
        building_block: String,
    ) -> DoseIntervalData {
        DoseIntervalData::new(&building_block, &cdf)
            .unwrap()
            .unwrap()
    }

    #[rstest]
    #[case(Context::DoseInterval(Boundary::End))]
    #[case(Context::DoseInterval(Boundary::Start))]
    #[case(Context::DoseScheduleFrequency)]
    fn test_incomplete_dose_interval_data(
        mut cdf: ContextualizedDataFrame,
        building_block: String,
        #[case] drop_context: Context,
    ) {
        let cdf = cdf
            .builder()
            .drop_scs_alongside_cols_with_context(&Context::None, &drop_context)
            .unwrap()
            .build()
            .unwrap();

        match DoseIntervalData::new(&building_block, cdf) {
            Err(err) => match err {
                CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                    table_name: _,
                    bb_id: _,
                    contexts,
                    n_found,
                    n_expected: _,
                } => {
                    println!("{:?}", contexts);
                    assert_eq!(n_found, 4);
                    assert!(contexts.contains(&drop_context));
                }
                _ => {
                    panic!("Unexpected error")
                }
            },
            Ok(_) => {
                panic!("Should not be ok")
            }
        };
    }
    #[rstest]
    fn test_incomplete_dose_interval_data_ok(cdf: ContextualizedDataFrame, building_block: String) {
        assert!(DoseIntervalData::new(&building_block, &cdf).is_ok());
    }

    #[rstest]
    fn test_incomplete_dose_interval_data_none(building_block: String) {
        let patient_cdf = generate_minimal_cdf(1, 2);
        assert!(
            DoseIntervalData::new(&building_block, &patient_cdf)
                .unwrap()
                .is_none()
        );
    }

    #[rstest]
    fn test_dose_interval_data(dose_interval_data: DoseIntervalData) {
        let dose_interval_data = dose_interval_data.get(0).unwrap().unwrap();

        dbg!(&dose_interval_data);
    }
}

#[cfg(test)]
mod getter_tests {
    use super::*;
    use crate::config::table_context::SeriesContext;
    use crate::config::traits::SeriesContextBuilding;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::test_suite::phenopacket_component_generation::{
        default_schedule_frequency, default_timestamp, default_unit_oc,
    };
    use crate::transform::error::GetterError;
    use polars::datatypes::AnyValue;
    use polars::prelude::{IntoColumn, NamedFrom, Series};
    use rstest::{fixture, rstest};

    #[fixture]
    fn building_block() -> String {
        "bb_1".to_string()
    }

    #[fixture]
    fn dose_interval_data(building_block: String) -> DoseIntervalData {
        let mut cdf = generate_minimal_cdf(1, 2);

        let schedule_frequency = Series::new(
            "schedule_frequency".into(),
            &[
                AnyValue::String(&default_schedule_frequency().label),
                AnyValue::String(&default_schedule_frequency().label),
            ],
        );
        let interval_start = Series::new(
            "interval_start".into(),
            &[
                AnyValue::String(&default_timestamp().to_string()),
                AnyValue::String(&default_timestamp().to_string()),
            ],
        );
        let interval_end = Series::new(
            "interval_end".into(),
            &[
                AnyValue::String(&default_timestamp().to_string()),
                AnyValue::String(&default_timestamp().to_string()),
            ],
        );
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
                SeriesContext::from_identifier("schedule_frequency")
                    .with_data_context(Context::DoseScheduleFrequency)
                    .with_building_block_id(&building_block),
                &[schedule_frequency.into_column()],
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("interval_start")
                    .with_data_context(Context::DoseInterval(Boundary::Start))
                    .with_building_block_id(&building_block),
                &[interval_start.into_column()],
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("interval_end")
                    .with_data_context(Context::DoseInterval(Boundary::End))
                    .with_building_block_id(&building_block),
                &[interval_end.into_column()],
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("values")
                    .with_data_context(Context::QuantityValue)
                    .with_building_block_id(&building_block),
                &[values.into_column()],
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("unit")
                    .with_data_context(Context::QuantityUnit)
                    .with_building_block_id(&building_block),
                &[units.into_column()],
            )
            .unwrap()
            .build()
            .unwrap();

        DoseIntervalData::new(&building_block, &cdf)
            .unwrap()
            .unwrap()
    }

    #[rstest]
    fn test_get_out_of_bounds_at_len(dose_interval_data: DoseIntervalData) {
        let result = dose_interval_data.get(dose_interval_data.len());
        assert!(matches!(result, Err(GetterError::OutOfBounds)));
    }

    #[rstest]
    fn test_get_out_of_bounds_well_past_end(dose_interval_data: DoseIntervalData) {
        let result = dose_interval_data.get(usize::MAX);
        assert!(matches!(result, Err(GetterError::OutOfBounds)));
    }

    #[rstest]
    fn test_get_returns_some_for_valid_index(dose_interval_data: DoseIntervalData) {
        assert!(dose_interval_data.get(0).unwrap().is_some());
    }

    #[rstest]
    fn test_get_quantity_value_first_row(dose_interval_data: DoseIntervalData) {
        let item = dose_interval_data.get(0).unwrap().unwrap();
        assert_eq!(item.quantity.value, 1.0);
    }

    #[rstest]
    fn test_get_quantity_value_second_row(dose_interval_data: DoseIntervalData) {
        let item = dose_interval_data.get(1).unwrap().unwrap();
        assert_eq!(item.quantity.value, 2.0);
    }

    #[rstest]
    fn test_get_schedule_frequency_first_row(dose_interval_data: DoseIntervalData) {
        let item = dose_interval_data.get(0).unwrap().unwrap();
        assert_eq!(item.schedule_frequency, default_schedule_frequency().label);
    }

    #[rstest]
    fn test_get_interval_start_first_row(dose_interval_data: DoseIntervalData) {
        let item = dose_interval_data.get(0).unwrap().unwrap();
        assert_eq!(item.interval_start, default_timestamp().to_string());
    }

    #[rstest]
    fn test_get_interval_end_first_row(dose_interval_data: DoseIntervalData) {
        let item = dose_interval_data.get(0).unwrap().unwrap();
        assert_eq!(item.interval_end, default_timestamp().to_string());
    }
}
