use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;

use crate::extract::enums::Filter;
use crate::transform::collecting::medical_actions::medical_action::MedicalActionData;
use crate::transform::collecting::medical_actions::medical_treatment_data::{
    DoseIntervalRow, TreatmentData,
};
use crate::transform::collecting::traits::{Collect, GetRows, Pluck};
use crate::transform::error::{CollectorError, GetterError};
use crate::transform::traits::PhenopacketBuilding;
use std::any::Any;

struct MedicalTreatmentIterator<'a> {
    treatment_data: &'a TreatmentData,
    medical_action_data: Option<&'a MedicalActionData>,
    max_iterations: usize,
    current_index: usize,
}

impl<'a> MedicalTreatmentIterator<'a> {
    pub fn new(
        treatment_data: &'a TreatmentData,
        medical_action_data: Option<&'a MedicalActionData>,
    ) -> Self {
        Self {
            treatment_data,
            medical_action_data,
            max_iterations: treatment_data.len(),
            current_index: 0,
        }
    }
}

impl<'a> Iterator for MedicalTreatmentIterator<'a> {
    type Item = Result<MedicalTreatmentIterElement<'a>, CollectorError>;

    fn next(&mut self) -> Option<Self::Item> {
        for _ in 0..self.max_iterations {
            let treatment = match self.treatment_data.get(self.current_index) {
                Ok(treatment_opt) => match treatment_opt {
                    Some(treatment) => treatment,
                    None => {
                        self.current_index += 1;
                        continue;
                    }
                },
                Err(err) => {
                    return match err {
                        GetterError::RequiredValueMissingError { .. } => {
                            Some(Err(CollectorError::from(err)))
                        }
                        GetterError::OutOfBounds => None,
                    };
                }
            };

            let medical_action_data = if let Some(medical_action_data) = self.medical_action_data {
                match medical_action_data.get(self.current_index) {
                    Ok(mad) => mad,
                    Err(err) => return Some(Err(CollectorError::from(err))),
                }
            } else {
                None
            };

            self.current_index += 1;

            return Some(Ok(MedicalTreatmentIterElement {
                agent: treatment.agent,
                route_of_administration: treatment.route_of_administration,
                dose_intervals: treatment.dose_intervals,
                drug_type: treatment.drug_type,
                unit: treatment.cumulative_dose.pluck(|cd| Some(cd.unit)),
                value: treatment.cumulative_dose.pluck(|cd| Some(cd.value)),
                reference_range: treatment.cumulative_dose.pluck(|cd| cd.reference_range),
                treatment_target: medical_action_data.pluck(|mad| mad.treatment_target),
                treatment_intent: medical_action_data.pluck(|mad| mad.treatment_intent),
                response_to_treatment: medical_action_data.pluck(|mad| mad.response_to_treatment),
                treatment_termination_reason: medical_action_data
                    .pluck(|mad| mad.treatment_termination_reason),
            }));
        }

        None
    }
}

struct MedicalTreatmentIterElement<'a> {
    agent: &'a str,
    route_of_administration: Option<&'a str>,
    dose_intervals: Vec<DoseIntervalRow>,
    drug_type: Option<&'a str>,
    unit: Option<&'a str>,
    value: Option<f64>,
    reference_range: Option<(f64, f64)>,
    treatment_target: Option<&'a str>,
    treatment_intent: Option<&'a str>,
    response_to_treatment: Option<&'a str>,
    treatment_termination_reason: Option<&'a str>,
}

#[derive(Debug)]
pub struct MedicalTreatmentCollector;

impl Collect for MedicalTreatmentCollector {
    fn collect(
        &self,
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
    ) -> Result<(), CollectorError> {
        for patient_cdf in patient_cdfs {
            let treatment_agents_sc = patient_cdf
                .filter_series_context()
                .where_data_context(Filter::Is(&Context::TreatmentAgent))
                .collect();

            for treatment_agent_sc in treatment_agents_sc {
                let treatment_data =
                    TreatmentData::new(patient_cdf, treatment_agent_sc.get_building_block_id())?;
                let medical_action_data = MedicalActionData::new(
                    patient_cdf,
                    treatment_agent_sc.get_building_block_id(),
                )?;

                if let Some(treatment_data) = treatment_data {
                    for treatment_values in
                        MedicalTreatmentIterator::new(&treatment_data, medical_action_data.as_ref())
                    {
                        let treatment_values = treatment_values?;
                        builder.insert_medical_treatment(
                            patient_id,
                            treatment_values.agent,
                            treatment_values.route_of_administration,
                            treatment_values.dose_intervals,
                            treatment_values.drug_type,
                            treatment_values.unit,
                            treatment_values.value,
                            treatment_values.reference_range,
                            treatment_values.treatment_target,
                            treatment_values.treatment_intent,
                            treatment_values.response_to_treatment,
                            treatment_values.treatment_termination_reason,
                        )?
                    }
                }
            }
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::SeriesContext;
    use crate::extract::ContextualizedDataFrame;
    use crate::test_suite::cdf_generation::{default_patient_id, generate_minimal_cdf};
    use crate::test_suite::phenopacket_component_generation::{
        default_disease_oc, default_drug_type, default_treatment_agent_oc,
        default_treatment_intent, default_treatment_response, default_treatment_termination_reason,
    };
    use crate::test_suite::phenopacket_component_generation::{
        default_route_of_administration_oc, default_unit_oc,
    };

    use crate::config::traits::SeriesContextBuilding;
    use crate::test_suite::mocks::MockPhenopacketBuilding;
    use polars::datatypes::AnyValue;
    use polars::prelude::{IntoColumn, NamedFrom, Series};
    use rstest::{fixture, rstest};

    #[fixture]
    fn medical_treatment_cdf() -> ContextualizedDataFrame {
        let mut patient_cdf = generate_minimal_cdf(1, 2);
        let procedure = Series::new(
            "agent".into(),
            &[
                AnyValue::Null,
                AnyValue::String(&default_treatment_agent_oc().clone().label),
            ],
        );

        let route_of_administration = Series::new(
            "route_of_administration".into(),
            &[
                AnyValue::Null,
                AnyValue::String(&default_route_of_administration_oc().id),
            ],
        );

        let drug_type = Series::new(
            "drug_type".into(),
            &[AnyValue::Null, AnyValue::String(default_drug_type())],
        );

        let quantity_values =
            Series::new("values".into(), &[AnyValue::Null, AnyValue::Float64(0.5)]);

        let units = Series::new(
            "unit".into(),
            &[AnyValue::Null, AnyValue::String(&default_unit_oc().label)],
        );

        let treatment_target = Series::new(
            "treatment_target".into(),
            &[
                AnyValue::Null,
                AnyValue::String(&default_disease_oc().label),
            ],
        );

        let treatment_intent = Series::new(
            "treatment_intent".into(),
            &[
                AnyValue::Null,
                AnyValue::String(&default_treatment_intent().label),
            ],
        );

        let treatment_response = Series::new(
            "treatment_response".into(),
            &[
                AnyValue::Null,
                AnyValue::String(&default_treatment_response().id),
            ],
        );

        let treatment_termination_reason = Series::new(
            "treatment_termination_reason".into(),
            &[
                AnyValue::Null,
                AnyValue::String(&default_treatment_termination_reason().id),
            ],
        );

        let building_block = "treatment_1";
        patient_cdf
            .builder()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("agent")
                    .with_data_context(Context::TreatmentAgent)
                    .with_building_block_id(Some(building_block)),
                vec![procedure.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("route_of_administration")
                    .with_data_context(Context::RouteOfAdministration)
                    .with_building_block_id(building_block),
                vec![route_of_administration.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("drug_type")
                    .with_data_context(Context::DrugType)
                    .with_building_block_id(building_block),
                vec![drug_type.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("values")
                    .with_data_context(Context::QuantityValue)
                    .with_building_block_id(building_block),
                vec![quantity_values.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("unit")
                    .with_data_context(Context::QuantityUnit)
                    .with_building_block_id(building_block),
                vec![units.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("treatment_target")
                    .with_data_context(Context::TreatmentTarget)
                    .with_building_block_id(building_block),
                vec![treatment_target.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("treatment_intent")
                    .with_data_context(Context::TreatmentIntent)
                    .with_building_block_id(building_block),
                vec![treatment_intent.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("treatment_response")
                    .with_data_context(Context::ResponseToTreatment)
                    .with_building_block_id(building_block),
                vec![treatment_response.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("treatment_termination_reason")
                    .with_data_context(Context::TreatmentTerminationReason)
                    .with_building_block_id(building_block),
                vec![treatment_termination_reason.into_column()].as_ref(),
            )
            .unwrap()
            .build()
            .unwrap()
            .clone()
    }

    #[rstest]
    fn test_collect_procedure(medical_treatment_cdf: ContextualizedDataFrame) {
        let mut builder = MockPhenopacketBuilding::new();
        let collector = MedicalTreatmentCollector;

        let patient_id = default_patient_id();

        builder
            .expect_insert_medical_treatment()
            .withf(
                |id,
                 agent,
                 route_of_admin,
                 dose_intervals,
                 drug_type,
                 unit,
                 quant_value,
                 ref_range,
                 treatment_target,
                 treatment_intent,
                 response_to_treatment,
                 termination_reason| {
                    id == default_patient_id()
                        && agent == default_treatment_agent_oc().label
                        && *route_of_admin == Some(&default_route_of_administration_oc().id)
                        && *dose_intervals == vec![]
                        && *drug_type == Some(default_drug_type())
                        && *unit == Some(&default_unit_oc().label)
                        && *quant_value == Some(0.5)
                        && ref_range.is_none()
                        && *treatment_target == Some(&default_disease_oc().label)
                        && *treatment_intent == Some(&default_treatment_intent().label)
                        && *response_to_treatment == Some(&default_treatment_response().id)
                        && *termination_reason == Some(&default_treatment_termination_reason().id)
                },
            )
            .times(1)
            .returning(|_, _, _, _, _, _, _, _, _, _, _, _| Ok(()));

        collector
            .collect(&mut builder, &[medical_treatment_cdf], &patient_id)
            .unwrap();
    }
}
