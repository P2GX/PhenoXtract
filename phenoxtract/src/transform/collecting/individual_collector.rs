use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::transform::collecting::traits::Collect;
use crate::transform::collecting::utils::get_single_multiplicity_element;
use crate::transform::error::CollectorError;
use crate::transform::traits::PhenopacketBuilding;
use std::any::Any;

#[derive(Debug)]
pub struct IndividualCollector;

impl Collect for IndividualCollector {
    fn collect(
        &self,
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
    ) -> Result<(), CollectorError> {
        let date_of_birth =
            get_single_multiplicity_element(patient_cdfs, &Context::DateOfBirth, &Context::None)?;

        let subject_sex =
            get_single_multiplicity_element(patient_cdfs, &Context::SubjectSex, &Context::None)?;

        let time_at_last_encounter =
            Self::find_single_time_element(patient_cdfs, Context::LAST_ENCOUNTER_VARIANTS)?;

        builder.upsert_individual(
            patient_id,
            None,
            date_of_birth.as_deref(),
            time_at_last_encounter.as_deref(),
            subject_sex.as_deref(),
            None,
            None,
            None,
        )?;

        Self::collect_vitality_status(builder, patient_cdfs, patient_id)?;

        Ok(())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl IndividualCollector {
    fn collect_vitality_status(
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
    ) -> Result<(), CollectorError> {
        let status =
            get_single_multiplicity_element(patient_cdfs, &Context::VitalStatus, &Context::None)?;

        if let Some(status) = status {
            let time_of_death =
                Self::find_single_time_element(patient_cdfs, Context::TIME_OF_DEATH_VARIANTS)?;

            let cause_of_death = get_single_multiplicity_element(
                patient_cdfs,
                &Context::CauseOfDeath,
                &Context::None,
            )?;

            let survival_time_days = get_single_multiplicity_element(
                patient_cdfs,
                &Context::SurvivalTimeDays,
                &Context::None,
            )?;

            let survival_time_days = survival_time_days
                .map(|str| str.parse::<u32>())
                .transpose()?;

            builder.upsert_vital_status(
                patient_id,
                status.as_ref(),
                time_of_death.as_deref(),
                cause_of_death.as_deref(),
                survival_time_days,
            )?;
        }
        Ok(())
    }

    fn find_single_time_element(
        patient_cdfs: &[ContextualizedDataFrame],
        time_element_contexts: &[Context],
    ) -> Result<Option<String>, CollectorError> {
        let mut time_element = None;
        for last_encounter_context in time_element_contexts.iter() {
            time_element = get_single_multiplicity_element(
                patient_cdfs,
                last_encounter_context,
                &Context::None,
            )?;
            if time_element.is_some() {
                break;
            }
        }

        Ok(time_element)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TableContext;
    use crate::config::context::TimeElementType;
    use crate::config::table_context::{Identifier, SeriesContext};
    use crate::test_suite::cdf_generation::default_patient_id;
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::phenopacket_component_generation::default_meta_data;
    use crate::test_suite::phenopacket_component_generation::default_phenopacket_id;
    use crate::test_suite::phenopacket_component_generation::{
        default_disease_oc, default_iso_age,
    };
    use crate::test_suite::resource_references::mondo_meta_data_resource;
    use crate::test_suite::utils::assert_phenopackets;
    use crate::utils::phenopacket_schema_version;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::time_element::Element;
    use phenopackets::schema::v2::core::vital_status::Status;
    use phenopackets::schema::v2::core::{
        Age, Individual, MetaData, Sex, TimeElement, VitalStatus,
    };
    use polars::datatypes::AnyValue;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use prost_types::Timestamp;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[fixture]
    fn patient_id() -> String {
        default_patient_id()
    }

    #[fixture]
    fn individual_info_tc() -> TableContext {
        let id_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("subject_id".to_string()))
            .with_data_context(Context::SubjectId);

        let dob_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("dob".to_string()))
            .with_data_context(Context::DateOfBirth);

        let tale_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("time_at_last_encounter".to_string()))
            .with_data_context(Context::TimeAtLastEncounter(TimeElementType::Age));

        let time_of_death_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("time_of_death".to_string()))
            .with_data_context(Context::TimeOfDeath(TimeElementType::Age));

        let sex_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("sex".to_string()))
            .with_data_context(Context::SubjectSex);

        let vital_status_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("vital_status".to_string()))
            .with_data_context(Context::VitalStatus);

        let cause_of_death_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("cause_of_death".to_string()))
            .with_data_context(Context::CauseOfDeath);

        let survival_time_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("survival_time".to_string()))
            .with_data_context(Context::SurvivalTimeDays);

        TableContext::new(
            "patient_data".to_string(),
            vec![
                id_sc,
                dob_sc,
                tale_sc,
                sex_sc,
                vital_status_sc,
                time_of_death_sc,
                cause_of_death_sc,
                survival_time_sc,
            ],
        )
    }

    #[fixture]
    fn individual_info_df(patient_id: String) -> DataFrame {
        let id_col = Column::new("subject_id".into(), [patient_id]);
        let subject_sex_col = Column::new("sex".into(), [AnyValue::String("MALE")]);
        let vital_status_col = Column::new("vital_status".into(), [AnyValue::String("ALIVE")]);
        let dob_col = Column::new("dob".into(), [AnyValue::String("1960-02-05")]);
        let tale_col = Column::new(
            "time_at_last_encounter".into(),
            [AnyValue::String(default_iso_age().as_str())],
        );
        let time_of_death_col =
            Column::new("time_of_death".into(), [AnyValue::String("2001-01-29")]);
        let cause_of_death_col = Column::new(
            "cause_of_death".into(),
            [AnyValue::String(default_disease_oc().label.as_str())],
        );
        let survival_time_col = Column::new("survival_time".into(), [AnyValue::Int32(155)]);

        DataFrame::new(vec![
            id_col,
            subject_sex_col,
            vital_status_col,
            time_of_death_col,
            cause_of_death_col,
            survival_time_col,
            dob_col,
            tale_col,
        ])
        .unwrap()
    }
    #[fixture]
    fn individual_info_cdf(
        individual_info_df: DataFrame,
        individual_info_tc: TableContext,
    ) -> ContextualizedDataFrame {
        ContextualizedDataFrame::new(individual_info_tc, individual_info_df).unwrap()
    }

    #[rstest]
    fn test_collect_individual(
        temp_dir: TempDir,
        individual_info_cdf: ContextualizedDataFrame,
        patient_id: String,
    ) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        IndividualCollector
            .collect(&mut builder, &[individual_info_cdf], &patient_id)
            .unwrap();

        let mut phenopackets = builder.build();

        let indiv = Individual {
            id: patient_id,
            date_of_birth: Some(Timestamp {
                seconds: -312595200,
                nanos: 0,
            }),
            sex: Sex::Male as i32,
            vital_status: Some(VitalStatus {
                status: Status::Alive as i32,
                time_of_death: Some(TimeElement {
                    element: Some(Element::Timestamp(Timestamp {
                        seconds: 980726400,
                        nanos: 0,
                    })),
                }),
                cause_of_death: Some(default_disease_oc()),
                survival_time_in_days: 155,
            }),
            time_at_last_encounter: Some(TimeElement {
                element: Some(Element::Age(Age {
                    iso8601duration: default_iso_age(),
                })),
            }),
            ..Default::default()
        };

        let mut expected_phenopacket = Phenopacket {
            id: default_phenopacket_id(),
            subject: Some(indiv),
            meta_data: Some(MetaData {
                phenopacket_schema_version: phenopacket_schema_version(),
                resources: vec![mondo_meta_data_resource()],
                submitted_by: default_meta_data().submitted_by,
                created_by: default_meta_data().created_by,
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phenopacket);
    }
}
