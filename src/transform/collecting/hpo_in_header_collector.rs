use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use crate::transform::utils::HpoColMaker;
use log::warn;
use std::collections::HashSet;

#[derive(Debug)]
pub struct HpoInHeaderCollector;

impl Collect for HpoInHeaderCollector {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        patient_cdfs: &[ContextualizedDataFrame],
        phenopacket_id: &str,
    ) -> Result<(), CollectorError> {
        for patient_cdf in patient_cdfs {
            let patient_id = patient_cdf
                .get_subject_id_col()
                .get(0)
                .expect("Should have one patient id")
                .to_string();

            let hpo_term_in_header_scs = patient_cdf
                .filter_series_context()
                .where_header_context(Filter::Is(&Context::HpoLabelOrId))
                .where_data_context(Filter::Is(&Context::ObservationStatus))
                .collect();

            for hpo_sc in hpo_term_in_header_scs {
                let sc_id = hpo_sc.get_identifier();
                let hpo_cols = patient_cdf.get_columns(sc_id);

                let stringified_linked_onset_col = patient_cdf.get_single_linked_column_as_str(
                    hpo_sc.get_building_block_id(),
                    &[Context::OnsetAge, Context::OnsetDate],
                )?;

                for hpo_col in hpo_cols {
                    let hpo_id = HpoColMaker::new().decode_column_header(hpo_col).0;

                    let boolified_hpo_col = hpo_col.bool()?;

                    let mut seen_pairs = HashSet::new();

                    for row_idx in 0..boolified_hpo_col.len() {
                        let obs_status = boolified_hpo_col.get(row_idx);
                        let onset = if let Some(onset_col) = &stringified_linked_onset_col {
                            onset_col.get(row_idx)
                        } else {
                            None
                        };
                        seen_pairs.insert((obs_status, onset));
                    }

                    seen_pairs.remove(&(None, None));

                    if seen_pairs.len() == 1 {
                        let (obs_status, onset) = seen_pairs.into_iter().next().unwrap();
                        //if the observation_status is None, no phenotype is upserted
                        //if the observation_status is true, the phenotype is upserted with excluded = None
                        //if the observation_status is false, the phenotype is upserted with excluded = true
                        if let Some(obs_status) = obs_status {
                            let excluded = if obs_status { None } else { Some(true) };
                            builder.upsert_phenotypic_feature(
                                phenopacket_id,
                                hpo_id,
                                None,
                                excluded,
                                None,
                                None,
                                onset,
                                None,
                                None,
                            )?;
                        } else if let Some(onset) = onset {
                            warn!(
                                "Non-null onset {onset} found for null observation status for patient {patient_id}."
                            )
                        }
                    } else if seen_pairs.len() > 2 {
                        return Err(CollectorError::ExpectedUniquePhenotypeData {
                            table_name: patient_cdf.context().name().to_string(),
                            patient_id: patient_id.to_string(),
                            phenotype: hpo_id.to_string(),
                        });
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
    use crate::config::TableContext;
    use crate::config::table_context::SeriesContext;
    use crate::extract::ContextualizedDataFrame;
    use crate::test_suite::cdf_generation::{
        generate_minimal_cdf, generate_minimal_cdf_components,
    };
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::phenopacket_component_generation::{
        default_age_element, default_iso_age, default_phenopacket_id, default_phenotype,
        generate_phenotype,
    };
    use crate::test_suite::resource_references::hp_meta_data_resource;
    use crate::test_suite::utils::assert_phenopackets;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::{MetaData, PhenotypicFeature};
    use polars::datatypes::{AnyValue, DataType};
    use polars::prelude::{Column, DataFrame, IntoColumn, NamedFrom, Series};
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn spasmus_nutans_pf_with_onset() -> PhenotypicFeature {
        generate_phenotype("HP:0010533", Some(default_age_element()))
    }

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[fixture]
    fn phenotypes_in_rows_cdf(
        spasmus_nutans_pf_with_onset: PhenotypicFeature,
    ) -> ContextualizedDataFrame {
        let mut patient_cdf = generate_minimal_cdf(1, 2);
        let phenotypes = Series::new(
            "phenotypes".into(),
            &[
                default_phenotype().clone().r#type.unwrap().label,
                spasmus_nutans_pf_with_onset.clone().r#type.unwrap().label,
            ],
        );

        let onset = Series::new(
            "onset".into(),
            &[AnyValue::Null, AnyValue::String(&default_iso_age())],
        );

        patient_cdf
            .builder()
            .insert_columns_with_series_context(
                SeriesContext::default()
                    .with_identifier("phenotypes".into())
                    .with_data_context(Context::HpoLabelOrId)
                    .with_building_block_id(Some("phenotype_1".to_string())),
                vec![phenotypes.into_column()].as_ref(),
            )
            .unwrap()
            .insert_columns_with_series_context(
                SeriesContext::default()
                    .with_identifier("onset".into())
                    .with_data_context(Context::OnsetAge)
                    .with_building_block_id(Some("phenotype_1".to_string())),
                vec![onset.into_column()].as_ref(),
            )
            .unwrap()
            .build()
            .unwrap()
            .clone()
    }

    #[rstest]
    fn test_collect_hpo_in_header_col(temp_dir: TempDir) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let collector = HpoInHeaderCollector;

        let (patient_col, sc) = generate_minimal_cdf_components(1, 2);

        let mut fractured_nose_excluded = default_phenotype().clone();
        fractured_nose_excluded.excluded = true;
        let phenotype_col_name = format!(
            "{}#(block foo)",
            fractured_nose_excluded.r#type.clone().unwrap().id
        );
        let pneumonia_col = Column::new(
            phenotype_col_name.clone().into(),
            [AnyValue::Boolean(false), AnyValue::Null],
        );
        let pneumonia_onset_col = Column::from(Series::full_null(
            "null_onset_col".into(),
            2,
            &DataType::String,
        ));

        let context = vec![
            sc,
            SeriesContext::default()
                .with_data_context(Context::ObservationStatus)
                .with_building_block_id(Some("bb1".to_string()))
                .with_header_context(Context::HpoLabelOrId)
                .with_identifier(phenotype_col_name.into()),
            SeriesContext::default()
                .with_data_context(Context::OnsetAge)
                .with_building_block_id(Some("bb1".to_string()))
                .with_identifier(pneumonia_onset_col.name().to_string().into()),
        ];

        let cdf = ContextualizedDataFrame::new(
            TableContext::new("TestTable", context),
            DataFrame::new(vec![patient_col, pneumonia_col, pneumonia_onset_col]).unwrap(),
        )
        .unwrap();

        let pp_id = default_phenopacket_id();

        collector.collect(&mut builder, &vec![cdf], &pp_id).unwrap();

        let mut phenopackets = builder.build();

        let mut expected_phenopacket = Phenopacket {
            id: pp_id.to_string(),
            phenotypic_features: vec![fractured_nose_excluded],
            meta_data: Some(MetaData {
                resources: vec![hp_meta_data_resource()],
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phenopacket);
    }
}
