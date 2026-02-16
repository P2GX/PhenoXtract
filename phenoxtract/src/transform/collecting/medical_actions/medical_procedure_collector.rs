use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;

use crate::transform::collecting::medical_actions::medical_action::{
    MedicalActionData, ProcedureData,
};
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use crate::transform::traits::PhenopacketBuilding;
use std::any::Any;

struct MedicalProcedureIterator<'a> {
    procedure_data: &'a ProcedureData,
    medical_action_data: &'a MedicalActionData,
    max_iterations: usize,
    current_index: usize,
}

impl<'a> MedicalProcedureIterator<'a> {
    pub fn new(
        procedure_data: &'a ProcedureData,
        medical_action_data: &'a MedicalActionData,
    ) -> Self {
        Self {
            procedure_data,
            medical_action_data,
            max_iterations: procedure_data.procedure_col.len(),
            current_index: 0,
        }
    }
}

impl<'a> Iterator for MedicalProcedureIterator<'a> {
    type Item = MedicalProcedureIterElement<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.max_iterations {
            return None;
        }

        let procedure_data = self.procedure_data.get(self.current_index);

        let general_medical_action_data = self.medical_action_data.get(self.current_index);

        self.current_index += 1;

        Some(MedicalProcedureIterElement {
            procedure: procedure_data.procedure,
            body_part: procedure_data.body_part,
            time_element: procedure_data.time_element,
            treatment_target: general_medical_action_data.treatment_target,
            treatment_intent: general_medical_action_data.treatment_intent,
            response_to_treatment: general_medical_action_data.response_to_treatment,
            treatment_termination_reason: general_medical_action_data.treatment_termination_reason,
        })
    }
}
struct MedicalProcedureIterElement<'a> {
    procedure: Option<&'a str>,
    body_part: Option<&'a str>,
    time_element: Option<&'a str>,
    treatment_target: Option<&'a str>,
    treatment_intent: Option<&'a str>,
    response_to_treatment: Option<&'a str>,
    treatment_termination_reason: Option<&'a str>,
}

#[derive(Debug)]
pub struct MedicalProcedureCollector;

impl Collect for MedicalProcedureCollector {
    fn collect(
        &self,
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
    ) -> Result<(), CollectorError> {
        for patient_cdf in patient_cdfs {
            let procedures = patient_cdf
                .filter_series_context()
                .where_data_context(Filter::Is(&Context::ProcedureLabelOrId))
                .collect();

            for procedure_sc in procedures {
                let procedure_data =
                    ProcedureData::new(patient_cdf, procedure_sc.get_building_block_id())?;
                let medical_action_data =
                    MedicalActionData::new(patient_cdf, procedure_sc.get_building_block_id())?;

                let procedure_iterator =
                    MedicalProcedureIterator::new(&procedure_data, &medical_action_data);

                for procedure_values in procedure_iterator {
                    if let Some(procedure) = procedure_values.procedure {
                        builder.insert_medical_procedure(
                            patient_id,
                            procedure,
                            procedure_values.body_part,
                            procedure_values.time_element,
                            procedure_values.treatment_target,
                            procedure_values.treatment_intent,
                            procedure_values.response_to_treatment,
                            procedure_values.treatment_termination_reason,
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
        default_disease_oc, default_procedure, default_procedure_oc, default_treatment_intent,
        default_treatment_response, default_treatment_termination_reason,
    };
    use crate::test_suite::phenopacket_component_generation::{
        default_procedure_body_side_oc, default_timestamp,
    };

    use crate::config::context::TimeElementType;
    use crate::config::traits::SeriesContextBuilding;
    use crate::test_suite::mocks::MockPhenopacketBuilding;
    use polars::datatypes::AnyValue;
    use polars::prelude::{IntoColumn, NamedFrom, Series, TimeUnit};
    use rstest::{fixture, rstest};

    #[fixture]
    fn procedure_cdf() -> ContextualizedDataFrame {
        let mut patient_cdf = generate_minimal_cdf(1, 2);
        let procedure = Series::new(
            "procedure".into(),
            &[
                AnyValue::Null,
                AnyValue::String(&default_procedure().clone().code.unwrap().label),
            ],
        );

        let body_site = Series::new(
            "body_site".into(),
            &[
                AnyValue::String(&default_procedure_body_side_oc().label),
                AnyValue::String(&default_procedure_body_side_oc().label),
            ],
        );

        let time_element = Series::new(
            "at".into(),
            &[
                AnyValue::Null,
                AnyValue::Datetime(
                    default_timestamp().nanos as i64,
                    TimeUnit::Nanoseconds,
                    None,
                ),
            ],
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

        patient_cdf
            .builder()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("procedure")
                    .with_data_context(Context::ProcedureLabelOrId)
                    .with_building_block_id("procedure_1"),
                vec![procedure.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("body_site")
                    .with_data_context(Context::ProcedureBodySite)
                    .with_building_block_id("procedure_1"),
                vec![body_site.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("at")
                    .with_data_context(Context::TimeAtProcedure(TimeElementType::Date))
                    .with_building_block_id("procedure_1"),
                vec![time_element.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("treatment_target")
                    .with_data_context(Context::TreatmentTarget)
                    .with_building_block_id("procedure_1"),
                vec![treatment_target.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("treatment_intent")
                    .with_data_context(Context::TreatmentIntent)
                    .with_building_block_id("procedure_1"),
                vec![treatment_intent.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("treatment_response")
                    .with_data_context(Context::ResponseToTreatment)
                    .with_building_block_id("procedure_1"),
                vec![treatment_response.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::default()
                    .with_identifier("treatment_termination_reason")
                    .with_data_context(Context::TreatmentTerminationReason)
                    .with_building_block_id("procedure_1"),
                vec![treatment_termination_reason.into_column()].as_ref(),
            )
            .unwrap()
            .build()
            .unwrap()
            .clone()
    }

    #[rstest]
    fn test_collect_procedure(procedure_cdf: ContextualizedDataFrame) {
        let mut builder = MockPhenopacketBuilding::new();
        let collector = MedicalProcedureCollector;

        let patient_id = default_patient_id();

        builder
            .expect_insert_medical_procedure()
            .withf(
                |id,
                 name,
                 body_site,
                 date,
                 treatment_target,
                 treatment_intent,
                 response_to_treatment,
                 termination_reason| {
                    id == default_patient_id()
                        && name == default_procedure_oc().label
                        && *body_site == Some(&default_procedure_body_side_oc().label)
                        && *date == Some("1970-01-01 00:00:00.000000000")
                        && *treatment_target == Some(&default_disease_oc().label)
                        && *treatment_intent == Some(&default_treatment_intent().label)
                        && *response_to_treatment == Some(&default_treatment_response().id)
                        && *termination_reason == Some(&default_treatment_termination_reason().id)
                },
            )
            .times(1)
            .returning(|_, _, _, _, _, _, _, _| Ok(()));

        collector
            .collect(&mut builder, &[procedure_cdf], &patient_id)
            .unwrap();
    }
}
