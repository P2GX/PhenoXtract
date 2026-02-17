use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::transform::collecting::medical_actions::quantity_data::QuantityData;
use crate::transform::error::CollectorError;
use polars::datatypes::StringChunked;

pub(super) struct Treatment<'a> {
    pub(super) agent: Option<&'a str>,
    pub(super) route_of_administration: Option<&'a str>,
    pub(super) dose_intervals: Option<&'a str>, // TODO
    pub(super) drug_type: Option<&'a str>,
    pub(super) quantity_data: Option<&'a QuantityData>,
}
pub(super) struct TreatmentData {
    pub(super) agent: StringChunked,
    pub(super) route_of_administration: Option<StringChunked>,
    pub(super) dose_intervals: Option<StringChunked>,
    pub(super) drug_type: Option<StringChunked>,
    pub(super) cumulative_dose: Option<QuantityData>,
}

impl TreatmentData {
    pub(super) fn new(
        patient_cdf: &ContextualizedDataFrame,
        building_block: Option<&str>,
    ) -> Result<Self, CollectorError> {
        match patient_cdf
            .get_single_linked_column_as_str(building_block, &[Context::TreatmentAgent])?
        {
            None => Err(CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                table_name: patient_cdf.context().name().to_string(),
                bb_id: building_block
                    .unwrap_or("Missing Building Block")
                    .to_string(),
                contexts: vec![Context::TreatmentAgent],
                n_found: 0,
                n_expected: 1,
            }),
            Some(agent) => {
                let route_of_administration = patient_cdf.get_single_linked_column_as_str(
                    building_block,
                    &[Context::RouteOfAdministration],
                )?;

                let drug_type = patient_cdf
                    .get_single_linked_column_as_str(building_block, &[Context::DrugType])?;

                Ok(Self {
                    agent,
                    route_of_administration,
                    dose_intervals: None,
                    drug_type,
                    cumulative_dose: QuantityData::new(patient_cdf, building_block)?,
                })
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        self.agent.len()
    }
    pub(super) fn get(&'_ self, idx: usize) -> Treatment<'_> {
        Treatment {
            agent: self.agent.as_ref().get(idx),
            route_of_administration: self
                .route_of_administration
                .as_ref()
                .and_then(|col| col.get(idx)),
            dose_intervals: None, //TODO
            drug_type: self.drug_type.as_ref().and_then(|col| col.get(idx)),
            quantity_data: self.cumulative_dose.as_ref(),
        }
    }
}
