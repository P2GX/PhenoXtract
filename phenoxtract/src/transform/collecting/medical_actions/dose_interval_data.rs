use crate::config::context::ContextKind;
use crate::extract::ContextualizedDataFrame;
use crate::transform::collecting::medical_actions::quantity_data::QuantityData;
use crate::transform::error::CollectorError;
use polars::datatypes::StringChunked;

pub(super) struct DoseInterval {}

pub(super) struct DoseIntervalData {
    quantity: QuantityData,
    schedule_frequency: StringChunked,
    interval_start: StringChunked,
    interval_end: StringChunked,
}

impl DoseIntervalData {
    pub(super) fn new(
        patient_cdf: &ContextualizedDataFrame,
        building_block: Option<&str>,
    ) -> Result<Option<Self>, CollectorError> {
        if let Some(quantity) = QuantityData::new(
            patient_cdf,
            building_block,
            &ContextKind::DoseIntervalQuantity,
        )? {
            // TODO: Gather other contexts here!
            Ok(Some(DoseIntervalData {
                quantity,
                schedule_frequency: Default::default(),
                interval_start: Default::default(),
                interval_end: Default::default(),
            }))
        } else {
            Ok(None)
        }
    }
}
