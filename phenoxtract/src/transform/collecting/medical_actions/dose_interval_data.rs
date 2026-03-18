use crate::config::context::{Boundary, Context};
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

        if quantity.is_none()
            && schedule_frequency.is_none()
            && interval_start.is_none()
            && interval_end.is_none()
        {
            return Ok(None);
        }

        Self::linked_col_guard(
            patient_cdf,
            building_block,
            &quantity,
            &schedule_frequency,
            &interval_start,
            &interval_end,
        )?;

        Ok(Some(DoseIntervalData {
            quantity: quantity.expect("Missing quantity"),
            schedule_frequency: schedule_frequency.expect("Missing schedule_frequency"),
            interval_start: interval_start.expect("Missing interval_start"),
            interval_end: interval_end.expect("Missing interval_end"),
        }))
    }

    fn linked_col_guard(
        patient_cdf: &ContextualizedDataFrame,
        building_block: &str,
        quantity: &Option<QuantityData>,
        schedule_frequency: &Option<StringChunked>,
        interval_start: &Option<StringChunked>,
        interval_end: &Option<StringChunked>,
    ) -> Result<(), CollectorError> {
        if quantity.is_none()
            || schedule_frequency.is_none()
            || interval_start.is_none()
            || interval_end.is_none()
        {
            let contexts: Vec<Context> = [
                quantity.is_none().then_some(Context::QuantityValue),
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
            .collect();

            Err(CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                table_name: patient_cdf.context().name().to_string(),
                bb_id: building_block.to_string(),
                contexts: contexts.clone(),
                n_found: 4 - contexts.len(),
                n_expected: 4,
            })
        } else {
            Ok(())
        }
    }
}

impl Getter for DoseIntervalData {
    type Item<'a> = DoseInterval<'a>;

    fn get(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError> {
        if self.len() <= idx {
            return Err(GetterError::OutOfBounds);
        }

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
                n_required: 4,
                context: "DoseIntervalData".to_string(),
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
        default_schedule_frequency, default_timestamp,
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
                    .with_building_block_id(building_block),
                vec![interval_end.into_column()].as_ref(),
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
    #[case(Context::QuantityValue)]
    #[case(Context::QuantityUnit)]
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

        match DoseIntervalData::new(&building_block, &cdf) {
            Err(err) => match err {
                CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                    table_name: _,
                    bb_id: _,
                    contexts,
                    n_found,
                    n_expected: _,
                } => {
                    assert_eq!(n_found, 3);
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
    fn test_dose_interval_data(dose_interval_data: DoseIntervalData) {
        let dose_interval_data = dose_interval_data.get(0).unwrap().unwrap();

        dbg!(&dose_interval_data);
    }
}
