#![allow(dead_code)]

use crate::config::context::{Context, ContextKind};
use crate::extract::ContextualizedDataFrame;

use crate::transform::collecting::medical_actions::quantity_data::{QuantityData, QuantityRow};
use crate::transform::collecting::traits::GetRows;
use crate::transform::error::{CollectorError, GetterError};

use crate::transform::collecting::utils::validate_no_unexpected_contexts;
use polars::datatypes::StringChunked;

#[derive(Debug, PartialEq)]
pub struct DoseIntervalRow;
#[derive(Debug)]
pub(super) struct DoseIntervalData;

#[derive(Debug)]
pub(super) struct TreatmentRow<'a> {
    pub(super) agent: &'a str,
    pub(super) route_of_administration: Option<&'a str>,
    pub(super) dose_intervals: Vec<DoseIntervalRow>,
    pub(super) drug_type: Option<&'a str>,
    pub(super) cumulative_dose: Option<QuantityRow<'a>>,
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
        let agent = patient_cdf
            .get_single_linked_column_as_str(building_block, &[Context::TreatmentAgent])?;

        let route_of_administration = patient_cdf
            .get_single_linked_column_as_str(building_block, &[Context::RouteOfAdministration])?;

        let drug_type =
            patient_cdf.get_single_linked_column_as_str(building_block, &[Context::DrugType])?;

        let cumulative_dose = building_block
            .map(|bb| QuantityData::new(patient_cdf, bb))
            .transpose()?
            .flatten();

        match agent {
            None => validate_no_unexpected_contexts(
                vec![
                    (
                        route_of_administration.is_some(),
                        vec![Context::RouteOfAdministration],
                    ),
                    (drug_type.is_some(), vec![Context::DrugType]),
                    (cumulative_dose.is_some(), vec![Context::QuantityUnit]),
                ],
                vec![Context::TreatmentAgent],
                building_block,
            ),
            Some(agent) => Ok(Some(Self {
                agent,
                route_of_administration,
                dose_intervals: vec![],
                drug_type,
                cumulative_dose,
            })),
        }
    }
}

impl GetRows for TreatmentData {
    type Item<'a> = TreatmentRow<'a>;

    fn construct_data_unchecked(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError> {
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

        match agent_opt {
            Some(agent) => Ok(Some(TreatmentRow {
                agent,
                route_of_administration,
                dose_intervals: vec![],
                drug_type,
                cumulative_dose,
            })),
            None => {
                let has_other_values = route_of_administration.is_some()
                    || drug_type.is_some()
                    || cumulative_dose.is_some();

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
        default_drug_type, default_route_of_administration_oc, default_treatment_agent_oc,
        default_unit_oc,
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
                AnyValue::String(default_drug_type()),
                AnyValue::String(default_drug_type()),
            ],
        );

        let mut builder = patient_cdf
            .builder()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("agent")
                    .with_data_context(Context::TreatmentAgent)
                    .with_building_block_id(building_block.to_string()),
                vec![agent.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("route_of_administration")
                    .with_data_context(Context::RouteOfAdministration)
                    .with_building_block_id(building_block.to_string()),
                vec![route_of_administration.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("drug_type")
                    .with_data_context(Context::DrugType)
                    .with_building_block_id(building_block.to_string()),
                vec![drug_type.into_column()].as_ref(),
            )
            .unwrap();

        for (idx, sub_block) in [sub_block_1, sub_block_2].iter().enumerate() {
            let values_col_name = format!("values_{}", sub_block);
            let unit_col_name = format!("unit_{}", sub_block);

            let values = Series::new(
                values_col_name.clone().into(),
                &[AnyValue::Float64(idx as f64), AnyValue::Float64(idx as f64)],
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
