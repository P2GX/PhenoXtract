use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::collecting::traits::Collect;
use crate::transform::collecting::utils::get_str_at_index;
use crate::transform::error::CollectorError;
use crate::transform::traits::PhenopacketBuilding;
use std::any::Any;

#[derive(Debug, Default)]
pub struct HpoInCellsCollector {
    allow_duplicate_phenotypes: bool,
}

impl Collect for HpoInCellsCollector {
    fn collect(
        &self,
        builder: &mut dyn PhenopacketBuilding,
        patient_cdfs: &[ContextualizedDataFrame],
        patient_id: &str,
    ) -> Result<(), CollectorError> {
        for patient_cdf in patient_cdfs {
            let hpo_terms_in_cells_scs = patient_cdf
                .filter_series_context()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&Context::Hpo))
                .collect();

            for hpo_sc in hpo_terms_in_cells_scs {
                let sc_id = hpo_sc.get_identifier();
                let hpo_cols = patient_cdf.identify_columns(sc_id);

                let onset_column = patient_cdf.get_single_linked_column_as_str(
                    hpo_sc.get_building_block_id(),
                    Context::ONSET_VARIANTS,
                )?;

                let resolution_column = patient_cdf.get_single_linked_column_as_str(
                    hpo_sc.get_building_block_id(),
                    Context::TIME_OF_RESOLUTION_VARIANTS,
                )?;

                let severity_column = patient_cdf.get_single_linked_column_as_str(
                    hpo_sc.get_building_block_id(),
                    &[Context::Severity],
                )?;

                for hpo_col in hpo_cols {
                    let stringified_hpo_col = hpo_col.str()?;

                    for row_idx in 0..stringified_hpo_col.len() {
                        let hpo = stringified_hpo_col.get(row_idx);
                        if let Some(hpo) = hpo {
                            let hpo_onset = get_str_at_index(onset_column.as_ref(), row_idx);
                            let hpo_resolution =
                                get_str_at_index(resolution_column.as_ref(), row_idx);
                            let hpo_severity = get_str_at_index(severity_column.as_ref(), row_idx);

                            if self.allow_duplicate_phenotypes {
                                builder.insert_phenotypic_feature(
                                    patient_id,
                                    hpo,
                                    None,
                                    None,
                                    hpo_severity,
                                    None,
                                    hpo_onset,
                                    hpo_resolution,
                                    None,
                                )?;
                            } else {
                                builder.upsert_phenotypic_feature(
                                    patient_id,
                                    hpo,
                                    None,
                                    None,
                                    hpo_severity,
                                    None,
                                    hpo_onset,
                                    hpo_resolution,
                                    None,
                                )?;
                            }
                        }
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
    use crate::test_suite::component_building::build_test_phenopacket_builder;
    use crate::test_suite::phenopacket_component_generation::{
        default_age_element, default_iso_age, default_phenopacket_id, default_phenotype,
        generate_phenotype,
    };
    use crate::test_suite::resource_references::hp_meta_data_resource;
    use crate::test_suite::utils::assert_phenopackets;
    use phenopackets::schema::v2::Phenopacket;

    use crate::config::context::TimeElementType;
    use crate::config::traits::SeriesContextBuilding;
    use crate::test_suite::phenopacket_component_generation::default_meta_data;
    use crate::utils::phenopacket_schema_version;
    use phenopackets::schema::v2::core::{MetaData, PhenotypicFeature};
    use polars::datatypes::AnyValue;
    use polars::prelude::{IntoColumn, NamedFrom, Series};
    use rstest::{fixture, rstest};

    #[fixture]
    fn spasmus_nutans_pf_with_onset() -> PhenotypicFeature {
        generate_phenotype("HP:0010533", Some(default_age_element()))
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
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("phenotypes")
                    .with_data_context(Context::Hpo)
                    .with_building_block_id("phenotype_1"),
                vec![phenotypes.into_column()].as_ref(),
            )
            .unwrap()
            .insert_sc_alongside_cols(
                SeriesContext::from_identifier("onset")
                    .with_data_context(Context::Onset(TimeElementType::Age))
                    .with_building_block_id("phenotype_1"),
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
    ) {
        let mut builder = build_test_phenopacket_builder();
        let patient_id = default_patient_id();
        HpoInCellsCollector::default()
            .collect(&mut builder, &[phenotypes_in_rows_cdf], &patient_id)
            .unwrap();

        let mut phenopackets = builder.build();

        let mut expected_phenopacket = Phenopacket {
            id: default_phenopacket_id(),
            phenotypic_features: vec![default_phenotype(), spasmus_nutans_pf_with_onset],
            meta_data: Some(MetaData {
                phenopacket_schema_version: phenopacket_schema_version(),
                resources: vec![hp_meta_data_resource()],
                created_by: default_meta_data().created_by.clone(),
                submitted_by: default_meta_data().submitted_by.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phenopacket);
    }
}
