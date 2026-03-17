use crate::config::context::{Boundary, Context, ContextKind};
use crate::extract::ContextualizedDataFrame;
use crate::transform::collecting::medical_actions::quantity_data::{Quantity, QuantityData};
use crate::transform::collecting::traits::Getter;
use crate::transform::error::{CollectorError, GetterError};
use polars::datatypes::StringChunked;

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
        building_block: Option<&str>,
        patient_cdf: &ContextualizedDataFrame,
    ) -> Result<Option<Self>, CollectorError> {
        // TODO: Rethink. Shouldn't this be treated like the others?
        if let Some(quantity) = QuantityData::new(
            patient_cdf,
            building_block,
            &ContextKind::DoseIntervalQuantity,
        )? {
            let schedule_frequency = patient_cdf.get_single_linked_column_as_str(
                building_block,
                &[Context::DoseScheduleFrequency],
            )?;

            let interval_start = patient_cdf.get_single_linked_column_as_str(
                building_block,
                &[Context::DoseInterval(Boundary::Start)],
            )?;
            let interval_end = patient_cdf.get_single_linked_column_as_str(
                building_block,
                &[Context::DoseInterval(Boundary::End)],
            )?;

            Self::linked_col_guard(
                patient_cdf,
                building_block,
                &schedule_frequency,
                &interval_start,
                &interval_end,
            )?;

            Ok(Some(DoseIntervalData {
                quantity,
                schedule_frequency: schedule_frequency.expect("Missing schedule_frequency"),
                interval_start: interval_start.expect("Missing interval_start"),
                interval_end: interval_end.expect("Missing interval_end"),
            }))
        } else {
            Ok(None)
        }
    }

    fn linked_col_guard(
        patient_cdf: &ContextualizedDataFrame,
        building_block: Option<&str>,
        schedule_frequency: &Option<StringChunked>,
        interval_start: &Option<StringChunked>,
        interval_end: &Option<StringChunked>,
    ) -> Result<(), CollectorError> {
        if schedule_frequency.is_none() || interval_start.is_none() || interval_end.is_none() {
            let contexts: Vec<Context> = [
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
                bb_id: building_block
                    .unwrap_or("Missing Building Block")
                    .to_string(),
                contexts: contexts.clone(),
                n_found: contexts.len(),
                n_expected: 3,
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
            self.quantity.get(idx),
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
