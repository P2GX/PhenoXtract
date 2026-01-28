use crate::extract::ContextualizedDataFrame;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use std::any::Any;

/// Ensures that if a patient ID appears in the data,
/// then there will be a Phenopacket for the patient,
/// even if there is no other data about the patient collected.
#[derive(Debug)]
pub struct PatientIdCollector;

impl Collect for PatientIdCollector {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        _patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
    ) -> Result<(), CollectorError> {
        builder.get_or_create_phenopacket(patient_id);
        Ok(())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::config::context::Context;
    use crate::config::table_context::SeriesContext;
    use crate::test_suite::cdf_generation::{default_patient_id, generate_minimal_cdf};
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::phenopacket_component_generation::{
        default_meta_data, default_phenopacket_id,
    };
    use crate::test_suite::utils::assert_phenopackets;
    use crate::transform::collecting::phenopacket_collector::PatientIdCollector;
    use crate::transform::collecting::traits::Collect;
    use crate::utils::phenopacket_schema_version;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::MetaData;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_collect_patient_ids(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());

        let df_height = 4;

        let mut cdf = generate_minimal_cdf(1, df_height);
        cdf.builder()
            .insert_columns_with_series_context(
                SeriesContext::default()
                    .with_identifier("age".into())
                    .with_data_context(Context::AgeAtLastEncounter),
                &[Column::new("age".into(), vec![32; df_height as usize])],
            )
            .unwrap()
            .build()
            .unwrap();

        PatientIdCollector
            .collect(&mut builder, &[cdf], &default_patient_id())
            .unwrap();

        let mut phenopackets = builder.build();

        let mut expected_phenopacket = Phenopacket {
            id: default_phenopacket_id(),
            meta_data: Some(MetaData {
                phenopacket_schema_version: phenopacket_schema_version(),
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
