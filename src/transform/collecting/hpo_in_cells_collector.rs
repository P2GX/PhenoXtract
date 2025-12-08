use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;

#[derive(Debug)]
pub struct HpoInCellsCollector;

impl Collect for HpoInCellsCollector {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
    ) -> Result<(), CollectorError> {
        let hpo_terms_in_cells_scs = patient_cdf
            .filter_series_context()
            .where_header_context(Filter::Is(&Context::None))
            .where_data_context(Filter::Is(&Context::HpoLabelOrId))
            .collect();

        for hpo_sc in hpo_terms_in_cells_scs {
            let sc_id = hpo_sc.get_identifier();
            let hpo_cols = patient_cdf.get_columns(sc_id);

            let onset_column = patient_cdf.get_single_linked_column_as_str(
                hpo_sc.get_building_block_id(),
                &[Context::OnsetAge, Context::OnsetDate],
            )?;

            for hpo_col in hpo_cols {
                let hpo_column = hpo_col.str()?;

                for row_idx in 0..hpo_column.len() {
                    let hpo = hpo_column.get(row_idx);
                    if let Some(hpo) = hpo {
                        let hpo_onset = if let Some(onset_col) = &onset_column {
                            onset_col.get(row_idx)
                        } else {
                            None
                        };

                        builder.upsert_phenotypic_feature(
                            phenopacket_id,
                            hpo,
                            None,
                            None,
                            None,
                            None,
                            hpo_onset,
                            None,
                            None,
                        )?;
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
    use crate::extract::ContextualizedDataFrame;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::phenopacket_component_generation::{
        default_age_element, default_iso_age, default_phenopacket_id, default_phenotype,
        generate_phenotype,
    };
    use crate::test_suite::resource_references::hp_meta_data_resource;
    use crate::test_suite::utils::assert_phenopackets;
    use phenopackets::schema::v2::Phenopacket;

    use phenopackets::schema::v2::core::{MetaData, PhenotypicFeature};
    use polars::datatypes::AnyValue;
    use polars::prelude::{IntoColumn, NamedFrom, Series};
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
    fn test_collect_phenotypic_features(
        spasmus_nutans_pf_with_onset: PhenotypicFeature,
        phenotypes_in_rows_cdf: ContextualizedDataFrame,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let pp_id = default_phenopacket_id();
        HpoInCellsCollector
            .collect(&mut builder, &phenotypes_in_rows_cdf, &pp_id)
            .unwrap();

        let mut phenopackets = builder.build();

        let mut expected_phenopacket = Phenopacket {
            id: pp_id,
            phenotypic_features: vec![default_phenotype(), spasmus_nutans_pf_with_onset],
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
