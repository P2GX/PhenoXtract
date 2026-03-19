use crate::config::context::{Boundary, Context, ContextKind};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::collecting::medical_actions::dose_interval_data::{
    DoseInterval, DoseIntervalData,
};
use crate::transform::collecting::medical_actions::quantity_data::{Quantity, QuantityData};
use crate::transform::collecting::traits::Getter;
use crate::transform::error::{CollectorError, GetterError};
use polars::datatypes::StringChunked;
use std::collections::HashSet;

#[derive(Debug)]
pub(super) struct Treatment<'a> {
    pub(super) agent: &'a str,
    pub(super) route_of_administration: Option<&'a str>,
    pub(super) dose_intervals: Vec<DoseInterval<'a>>,
    pub(super) drug_type: Option<&'a str>,
    pub(super) cumulative_dose: Option<Quantity<'a>>,
}
#[derive(Debug)]
pub(super) struct TreatmentData {
    pub(super) agent: StringChunked,
    pub(super) route_of_administration: Option<StringChunked>,
    pub(super) dose_intervals: Vec<DoseIntervalData>,
    pub(super) drug_type: Option<StringChunked>,
    pub(super) cumulative_dose: Option<QuantityData>,
}

impl TreatmentData {
    pub(super) fn new(
        patient_cdf: &ContextualizedDataFrame,
        building_block: Option<&str>,
    ) -> Result<Option<Self>, CollectorError> {
        match patient_cdf
            .get_single_linked_column_as_str(building_block, &[Context::TreatmentAgent])?
        {
            None => Ok(None),
            Some(agent) => {
                let route_of_administration = patient_cdf.get_single_linked_column_as_str(
                    building_block,
                    &[Context::RouteOfAdministration],
                )?;

                let drug_type = patient_cdf
                    .get_single_linked_column_as_str(building_block, &[Context::DrugType])?;

                Ok(Some(Self {
                    agent,
                    route_of_administration,
                    dose_intervals: Self::find_dose_interval_data(building_block, patient_cdf)?,
                    drug_type,
                    cumulative_dose: building_block
                        .map(|bb| QuantityData::new(patient_cdf, bb))
                        .transpose()?
                        .flatten(),
                }))
            }
        }
    }

    fn find_dose_interval_data(
        building_block: Option<&str>,
        patient_cdf: &ContextualizedDataFrame,
    ) -> Result<Vec<DoseIntervalData>, CollectorError> {
        if let Some(bb_id) = building_block {
            let agent_scs = patient_cdf
                .filter_series_context()
                .where_data_context(Filter::Is(&Context::TreatmentAgent))
                .where_building_block(Filter::Is(bb_id))
                .collect();

            let child_bb = patient_cdf.get_building_block_childs(agent_scs.first().unwrap());

            let dose_building_block = child_bb
                .iter()
                .filter_map(|sc| {
                    let context = sc.get_data_context();
                    if context.is_dose_schedule_frequency()
                        || [
                            Context::DoseInterval(Boundary::Start),
                            Context::DoseInterval(Boundary::End),
                        ]
                        .contains(context)
                    {
                        sc.get_building_block_id()
                    } else {
                        None
                    }
                })
                .collect::<HashSet<&str>>();

            let intervals = dose_building_block
                .into_iter()
                .filter_map(|bb| DoseIntervalData::new(bb, patient_cdf).transpose())
                .collect::<Result<Vec<_>, _>>()?;

            Ok(intervals)
        } else {
            Ok(vec![])
        }
    }
}

impl Getter for TreatmentData {
    type Item<'a> = Treatment<'a>;

    fn construct_data(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError> {
        let agent_opt = self.agent.as_ref().get(idx);
        let route_of_administration = self
            .route_of_administration
            .as_ref()
            .and_then(|col| col.get(idx));
        let drug_type = self.drug_type.as_ref().and_then(|col| col.get(idx));

        let cumulative_dose = self
            .cumulative_dose
            .as_ref()
            .map(|col| col.get(idx))
            .transpose()
            .map(|o| o.flatten())?;

        let dose_intervals: Vec<DoseInterval> = self
            .dose_intervals
            .iter()
            .filter_map(|di| di.get(idx).transpose())
            .collect::<Result<Vec<DoseInterval>, GetterError>>()?;

        match agent_opt {
            Some(agent) => Ok(Some(Treatment {
                agent,
                route_of_administration,
                dose_intervals,
                drug_type,
                cumulative_dose,
            })),
            None => {
                let has_other_values = route_of_administration.is_some()
                    || drug_type.is_some()
                    || cumulative_dose.is_some()
                    || !dose_intervals.is_empty();

                if has_other_values {
                    Err(GetterError::RequiredValueMissingError {
                        idx,
                        context: ContextKind::TreatmentAgent,
                    })
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn len(&self) -> usize {
        self.agent.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::SeriesContext;
    use crate::config::traits::SeriesContextBuilding;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::test_suite::phenopacket_component_generation::{
        default_route_of_administration_oc, default_schedule_frequency_oc, default_timestamp,
        default_treatment_agent_oc, default_unit_oc,
    };
    use polars::datatypes::AnyValue;
    use polars::prelude::{IntoColumn, NamedFrom, Series};
    use rstest::{fixture, rstest};

    #[fixture]
    fn building_block() -> &'static str {
        "block_1"
    }
    #[fixture]
    fn sub_block_1() -> &'static str {
        "dose_interval_1"
    }

    #[fixture]
    fn sub_block_2() -> &'static str {
        "dose_interval_2"
    }
    #[fixture]
    fn cdf(building_block: &str, sub_block_1: &str, sub_block_2: &str) -> ContextualizedDataFrame {
        let mut patient_cdf = generate_minimal_cdf(1, 2);

        let agent = Series::new(
            "agent".into(),
            &[
                AnyValue::String(&default_treatment_agent_oc().clone().label),
                AnyValue::String(&default_treatment_agent_oc().clone().label),
            ],
        );

        let route_of_administration = Series::new(
            "route_of_administration".into(),
            &[
                AnyValue::String(&default_route_of_administration_oc().clone().label),
                AnyValue::String(&default_route_of_administration_oc().clone().label),
            ],
        );

        let drug_type = Series::new(
            "drug_type".into(),
            &[
                AnyValue::String("PRESCRIPTION"),
                AnyValue::String("PRESCRIPTION"),
            ],
        );

        let mut builder = patient_cdf
            .builder()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("agent")
                    .with_data_context(Context::TreatmentAgent)
                    .with_building_block_id(building_block.to_string())
                    .with_sub_blocks(vec![sub_block_1.to_string()]),
                vec![agent.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("route_of_administration")
                    .with_data_context(Context::RouteOfAdministration)
                    .with_building_block_id(building_block.to_string())
                    .with_sub_blocks(vec![sub_block_1.to_string()]),
                vec![route_of_administration.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("drug_type")
                    .with_data_context(Context::DrugType)
                    .with_building_block_id(building_block.to_string())
                    .with_sub_blocks(vec![sub_block_1.to_string()]),
                vec![drug_type.into_column()].as_ref(),
            )
            .unwrap();

        for sub_block in [sub_block_1, sub_block_2].iter() {
            let schedule_frequency_col_name = format!("schedule_frequency_{}", sub_block);
            let interval_start_col_name = format!("interval_start_{}", sub_block);
            let interval_end_col_name = format!("interval_end_{}", sub_block);
            let values_col_name = format!("values_{}", sub_block);
            let unit_col_name = format!("unit_{}", sub_block);

            let schedule_frequency = Series::new(
                schedule_frequency_col_name.clone().into(),
                &[
                    AnyValue::String(&default_schedule_frequency_oc().clone().label),
                    AnyValue::String(&default_schedule_frequency_oc().clone().label),
                ],
            );

            let interval_start = Series::new(
                interval_start_col_name.clone().into(),
                &[
                    AnyValue::String(&default_timestamp().to_string()),
                    AnyValue::String(&default_timestamp().to_string()),
                ],
            );

            let interval_end = Series::new(
                interval_end_col_name.clone().into(),
                &[
                    AnyValue::String(&default_timestamp().to_string()),
                    AnyValue::String(&default_timestamp().to_string()),
                ],
            );

            let values = Series::new(
                values_col_name.clone().into(),
                &[AnyValue::Float64(0.5), AnyValue::Float64(0.5)],
            );

            let units = Series::new(
                unit_col_name.clone().into(),
                &[
                    AnyValue::String(&default_unit_oc().id),
                    AnyValue::String(&default_unit_oc().label),
                ],
            );

            builder = builder
                .insert_sc_alongside_cols(
                    SeriesContext::from_identifier(schedule_frequency_col_name)
                        .with_data_context(Context::DoseScheduleFrequency)
                        .with_building_block_id(sub_block.to_string()),
                    vec![schedule_frequency.into_column()].as_ref(),
                )
                .unwrap()
                .insert_sc_alongside_cols(
                    SeriesContext::from_identifier(interval_start_col_name)
                        .with_data_context(Context::DoseInterval(Boundary::Start))
                        .with_building_block_id(sub_block.to_string()),
                    vec![interval_start.into_column()].as_ref(),
                )
                .unwrap()
                .insert_sc_alongside_cols(
                    SeriesContext::from_identifier(interval_end_col_name)
                        .with_data_context(Context::DoseInterval(Boundary::End))
                        .with_building_block_id(sub_block.to_string()),
                    vec![interval_end.into_column()].as_ref(),
                )
                .unwrap()
                .insert_sc_alongside_cols(
                    SeriesContext::from_identifier(values_col_name)
                        .with_data_context(Context::QuantityValue)
                        .with_building_block_id(sub_block.to_string()),
                    vec![values.into_column()].as_ref(),
                )
                .unwrap()
                .insert_sc_alongside_cols(
                    SeriesContext::from_identifier(unit_col_name)
                        .with_data_context(Context::QuantityUnit)
                        .with_building_block_id(sub_block.to_string()),
                    vec![units.into_column()].as_ref(),
                )
                .unwrap();
        }
        builder.build().unwrap();

        patient_cdf
    }

    #[rstest]
    fn test_treatment_data_new_none() {
        let patient_cdf = generate_minimal_cdf(1, 2);

        assert!(TreatmentData::new(&patient_cdf, None).unwrap().is_none());
    }

    #[rstest]
    fn test_treatment_data_new_ok(cdf: ContextualizedDataFrame, building_block: &str) {
        assert!(
            TreatmentData::new(&cdf, Some(building_block))
                .unwrap()
                .is_some()
        );
    }

    #[rstest]
    fn test_find_dose_interval_data(cdf: ContextualizedDataFrame, building_block: &str) {
        let res = TreatmentData::find_dose_interval_data(Some(building_block), &cdf).unwrap();
        assert_eq!(res.len(), 1);
    }

    #[rstest]
    fn test_get_out_of_bounds_returns_err(cdf: ContextualizedDataFrame, building_block: &str) {
        let treatment_data = TreatmentData::new(&cdf, Some(building_block))
            .unwrap()
            .unwrap();

        assert!(matches!(
            treatment_data.get(99),
            Err(GetterError::OutOfBounds)
        ));
    }

    #[rstest]
    fn test_get_in_bounds_returns_some(cdf: ContextualizedDataFrame, building_block: &str) {
        let treatment_data = TreatmentData::new(&cdf, Some(building_block))
            .unwrap()
            .unwrap();

        let result = treatment_data.get(0).unwrap();
        assert!(result.is_some());
    }

    #[rstest]
    fn test_get_in_bounds_returns_none() {
        let treatment_data = TreatmentData {
            agent: StringChunked::new("agent".into(), &[None::<&str>]),
            route_of_administration: None,
            dose_intervals: vec![],
            drug_type: None,
            cumulative_dose: None,
        };

        assert!(treatment_data.get(0).unwrap().is_none());
    }
}
