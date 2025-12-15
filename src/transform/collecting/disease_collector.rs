use crate::config::context::{Context, DISEASE_LABEL_OR_ID_CONTEXTS};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use polars::datatypes::StringChunked;
use polars::error::PolarsError;

#[derive(Debug)]
pub struct DiseaseCollector;

impl Collect for DiseaseCollector {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        patient_cdfs: &[ContextualizedDataFrame],
        phenopacket_id: &str,
    ) -> Result<(), CollectorError> {
        for patient_cdf in patient_cdfs {
            let disease_in_cells_scs = patient_cdf
                .filter_series_context()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_contexts_are(DISEASE_LABEL_OR_ID_CONTEXTS.as_slice())
                .collect();

            for disease_sc in disease_in_cells_scs {
                let sc_id = disease_sc.get_identifier();
                let bb_id = disease_sc.get_building_block_id();

                let stringified_disease_cols = patient_cdf
                    .get_columns(sc_id)
                    .iter()
                    .map(|col| col.str())
                    .collect::<Result<Vec<&StringChunked>, PolarsError>>()?;

                let stringified_linked_onset_col = patient_cdf.get_single_linked_column_as_str(
                    bb_id,
                    &[Context::OnsetAge, Context::OnsetDate],
                )?;

                for row_idx in 0..patient_cdf.data().height() {
                    for stringified_disease_col in stringified_disease_cols.iter() {
                        let disease = stringified_disease_col.get(row_idx);
                        if let Some(disease) = disease {
                            let disease_onset =
                                if let Some(onset_col) = &stringified_linked_onset_col {
                                    onset_col.get(row_idx)
                                } else {
                                    None
                                };

                            builder.insert_disease(
                                phenopacket_id,
                                disease,
                                None,
                                disease_onset,
                                None,
                                None,
                                None,
                                None,
                                None,
                            )?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::SeriesContext;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::phenopacket_component_generation::{
        default_disease_with_age_onset, default_iso_age, generate_disease,
    };
    use crate::test_suite::resource_references::mondo_meta_data_resource;
    use crate::test_suite::utils::assert_phenopackets;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::MetaData;
    use polars::prelude::{AnyValue, Column};
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[rstest]
    fn test_collect_diseases(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let phenopacket_id = "cohort2019-P002".to_string();

        let mut cdf = generate_minimal_cdf(1, 2);
        let diseases = vec![
            default_disease_with_age_onset(),
            generate_disease("MONDO:0008258", None),
        ];

        let disease_col = Column::new(
            "disease".into(),
            diseases
                .iter()
                .map(|s| s.term.clone().unwrap().id)
                .collect::<Vec<String>>(),
        );

        let onset_col = Column::new(
            "onset".into(),
            [AnyValue::String(&default_iso_age()), AnyValue::Null],
        );

        cdf.builder()
            .insert_columns_with_series_context(
                SeriesContext::default()
                    .with_identifier("disease".into())
                    .with_data_context(Context::MondoLabelOrId)
                    .with_building_block_id(Some("disease_1".to_string())),
                vec![disease_col].as_ref(),
            )
            .unwrap()
            .insert_columns_with_series_context(
                SeriesContext::default()
                    .with_identifier("onset".into())
                    .with_data_context(Context::OnsetAge)
                    .with_building_block_id(Some("disease_1".to_string())),
                vec![onset_col].as_ref(),
            )
            .unwrap()
            .build()
            .unwrap();

        DiseaseCollector
            .collect(&mut builder, &vec![cdf], &phenopacket_id)
            .unwrap();

        let mut phenopackets = builder.build();

        let mut expected_phenopacket = Phenopacket {
            id: phenopacket_id.to_string(),
            diseases,
            meta_data: Some(MetaData {
                resources: vec![mondo_meta_data_resource()],
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phenopacket);
    }
}
