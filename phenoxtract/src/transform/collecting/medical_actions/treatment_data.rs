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

pub(super) struct Treatment<'a> {
    pub(super) agent: &'a str,
    pub(super) route_of_administration: Option<&'a str>,
    pub(super) dose_intervals: Vec<DoseInterval<'a>>,
    pub(super) drug_type: Option<&'a str>,
    pub(super) cumulative_dose: Option<Quantity<'a>>,
}

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
                    dose_intervals: Self::find_dose_interval_data(building_block, patient_cdf)?,
                    drug_type,
                    cumulative_dose: QuantityData::new(
                        patient_cdf,
                        building_block,
                        &ContextKind::CumulativeDose,
                    )?,
                })
            }
        }
    }

    pub(super) fn find_dose_interval_data(
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
                        || context.is_dose_interval_quantity()
                    {
                        sc.get_building_block_id()
                    } else {
                        None
                    }
                })
                .collect::<HashSet<&str>>();

            let intervals = dose_building_block
                .into_iter()
                .filter_map(|bb| DoseIntervalData::new(Some(bb), patient_cdf).transpose())
                .collect::<Result<Vec<_>, _>>()?;

            Ok(intervals)
        } else {
            Err(CollectorError::ExpectedBuildingBlock {
                table_name: patient_cdf.context().name().to_string(),
                patient_id: patient_cdf.get_subject_id_col().get(0)?.to_string(),
                context: ContextKind::DoseInterval,
            })
        }
    }
}

impl Getter for TreatmentData {
    type Item<'a> = Treatment<'a>;

    fn get(&self, idx: usize) -> Result<Option<Self::Item<'_>>, GetterError> {
        if self.len() <= idx {
            return Err(GetterError::OutOfBounds);
        }

        let agent_opt = self.agent.as_ref().get(idx);
        let route_of_administration = self
            .route_of_administration
            .as_ref()
            .and_then(|col| col.get(idx));
        let drug_type = self.drug_type.as_ref().and_then(|col| col.get(idx));
        let quantity_data = self.cumulative_dose.as_ref().and_then(|col| col.get(idx));

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
                cumulative_dose: quantity_data,
            })),
            None => Ok(None),
        }
    }

    fn len(&self) -> usize {
        self.agent.len()
    }
}
