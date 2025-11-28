use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::collecting::utils;
use crate::transform::error::CollectorError;
use crate::transform::utils::HpoColMaker;
use log::warn;
use polars::datatypes::StringChunked;
use polars::prelude::Column;
use std::any::Any;
use std::collections::HashSet;

#[derive(Debug)]
pub struct PhenotypeCollector;

impl Collect for PhenotypeCollector {
    fn collect(
        &self,
        mut builder: &mut PhenopacketBuilder,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
    ) -> Result<(), CollectorError> {
        let hpo_terms_in_cells_scs = patient_cdf
            .filter_series_context()
            .where_header_context(Filter::Is(&Context::None))
            .where_data_context(Filter::Is(&Context::HpoLabelOrId))
            .collect();

        let hpo_term_in_header_scs = patient_cdf
            .filter_series_context()
            .where_header_context(Filter::Is(&Context::HpoLabelOrId))
            .where_data_context(Filter::Is(&Context::ObservationStatus))
            .collect();

        let hpo_scs = [hpo_terms_in_cells_scs, hpo_term_in_header_scs].concat();

        for hpo_sc in hpo_scs {
            let sc_id = hpo_sc.get_identifier();
            let hpo_cols = patient_cdf.get_columns(sc_id);

            let stringified_linked_onset_col =
                utils::get_single_stringified_column_with_data_contexts_in_bb(
                    patient_cdf,
                    hpo_sc.get_building_block_id(),
                    vec![&Context::OnsetAge, &Context::OnsetDateTime],
                )?;

            for hpo_col in hpo_cols {
                if hpo_sc.get_header_context() == &Context::None
                    && hpo_sc.get_data_context() == &Context::HpoLabelOrId
                {
                    self.collect_hpo_in_cells_col(
                        &mut builder,
                        phenopacket_id,
                        hpo_col,
                        stringified_linked_onset_col.as_ref(),
                    )?;
                } else {
                    self.collect_hpo_in_header_col(
                        &mut builder,
                        patient_cdf.context().name(),
                        phenopacket_id,
                        hpo_col,
                        stringified_linked_onset_col.as_ref(),
                    )?;
                }
            }
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PhenotypeCollector {
    fn collect_hpo_in_cells_col(
        &self,
        builder: &mut PhenopacketBuilder,
        phenopacket_id: &str,
        patient_hpo_col: &Column,
        stringified_onset_col: Option<&StringChunked>,
    ) -> Result<(), CollectorError> {
        let stringified_hpo_col = patient_hpo_col.str()?;

        for row_idx in 0..stringified_hpo_col.len() {
            let hpo = stringified_hpo_col.get(row_idx);
            if let Some(hpo) = hpo {
                let hpo_onset = if let Some(onset_col) = &stringified_onset_col {
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
        Ok(())
    }

    fn collect_hpo_in_header_col(
        &self,
        builder: &mut PhenopacketBuilder,
        table_name: &str,
        phenopacket_id: &str,
        patient_hpo_col: &Column,
        stringified_onset_col: Option<&StringChunked>,
    ) -> Result<(), CollectorError> {
        let p_id = "TODO".to_owned();
        let hpo_id = HpoColMaker::new().decode_column_header(patient_hpo_col).0;

        let boolified_hpo_col = patient_hpo_col.bool()?;

        let mut seen_pairs = HashSet::new();

        for row_idx in 0..boolified_hpo_col.len() {
            let obs_status = boolified_hpo_col.get(row_idx);
            let onset = if let Some(onset_col) = &stringified_onset_col {
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
                    "Non-null onset {onset} found for null observation status for patient {p_id}."
                )
            }
        } else if seen_pairs.len() > 2 {
            return Err(CollectorError::ExpectedUniquePhenotypeData {
                table_name: table_name.to_string(),
                patient_id: p_id.to_string(),
                phenotype: hpo_id.to_string(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::SeriesContext;
    use crate::extract::ContextualizedDataFrame;
    use crate::test_utils::{
        assert_phenopackets, build_test_phenopacket_builder, generate_minimal_cdf,
    };
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::time_element::Element;
    use phenopackets::schema::v2::core::{
        Age, MetaData, OntologyClass, PhenotypicFeature, Resource, TimeElement,
    };
    use polars::datatypes::{AnyValue, DataType};
    use polars::prelude::{Column, IntoColumn, NamedFrom, Series};
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn spasmus_nutans_onset_age() -> Age {
        Age {
            iso8601duration: "P12Y5M028D".to_string(),
        }
    }

    #[fixture]
    fn spasmus_nutans_pf_with_onset(spasmus_nutans_onset_age: Age) -> PhenotypicFeature {
        PhenotypicFeature {
            r#type: Some(OntologyClass {
                id: "HP:0010533".to_string(),
                label: "Spasmus nutans".to_string(),
            }),
            onset: Some(TimeElement {
                element: Some(Element::Age(spasmus_nutans_onset_age)),
            }),
            ..Default::default()
        }
    }

    #[fixture]
    fn fractured_nose_pf() -> PhenotypicFeature {
        PhenotypicFeature {
            r#type: Some(OntologyClass {
                id: "HP:0041249".to_string(),
                label: "Fractured nose".to_string(),
            }),
            ..Default::default()
        }
    }

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[fixture]
    fn pp_id() -> String {
        "cohort2019-P002".to_string()
    }

    #[fixture]
    fn hp_meta_data_resource() -> Resource {
        Resource {
            id: "hp".to_string(),
            name: "Human Phenotype Ontology".to_string(),
            url: "http://purl.obolibrary.org/obo/hp.json".to_string(),
            version: "2025-09-01".to_string(),
            namespace_prefix: "HP".to_string(),
            iri_prefix: "http://purl.obolibrary.org/obo/HP_$1".to_string(),
        }
    }

    #[fixture]
    fn phenotypes_in_rows_cdf(
        fractured_nose_pf: PhenotypicFeature,
        spasmus_nutans_pf_with_onset: PhenotypicFeature,
        spasmus_nutans_onset_age: Age,
    ) -> ContextualizedDataFrame {
        let mut patient_cdf = generate_minimal_cdf(1, 2);
        let phenotypes = Series::new(
            "phenotypes".into(),
            &[
                fractured_nose_pf.clone().r#type.unwrap().label,
                spasmus_nutans_pf_with_onset.clone().r#type.unwrap().label,
            ],
        );

        let onset = Series::new(
            "onset".into(),
            &[
                AnyValue::Null,
                AnyValue::String(&spasmus_nutans_onset_age.iso8601duration),
            ],
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
        fractured_nose_pf: PhenotypicFeature,
        spasmus_nutans_pf_with_onset: PhenotypicFeature,
        phenotypes_in_rows_cdf: ContextualizedDataFrame,
        hp_meta_data_resource: Resource,
        pp_id: String,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(&temp_dir.path());

        PhenotypeCollector
            .collect(&mut builder, &phenotypes_in_rows_cdf, &pp_id)
            .unwrap();

        let mut phenopackets = builder.build();

        let mut expected_phenopacket = Phenopacket {
            id: pp_id.to_string(),
            phenotypic_features: vec![fractured_nose_pf, spasmus_nutans_pf_with_onset],
            meta_data: Some(MetaData {
                resources: vec![hp_meta_data_resource],
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phenopacket);
    }

    #[rstest]
    fn test_collect_hpo_in_cells_col(
        fractured_nose_pf: PhenotypicFeature,
        spasmus_nutans_pf_with_onset: PhenotypicFeature,
        phenotypes_in_rows_cdf: ContextualizedDataFrame,
        hp_meta_data_resource: Resource,
        pp_id: String,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(&temp_dir.path());

        let patient_hpo_col = phenotypes_in_rows_cdf.data().column("phenotypes").unwrap();
        let patient_onset_col = phenotypes_in_rows_cdf.data().column("onset").unwrap();

        let stringified_onset_col = patient_onset_col.str().unwrap();
        let collector = PhenotypeCollector;

        collector
            .collect_hpo_in_cells_col(
                &mut builder,
                &pp_id,
                patient_hpo_col,
                Some(stringified_onset_col),
            )
            .unwrap();

        let mut phenopackets = builder.build();

        let mut expected_phneopacket = Phenopacket {
            id: pp_id.to_string(),
            phenotypic_features: vec![fractured_nose_pf, spasmus_nutans_pf_with_onset],
            meta_data: Some(MetaData {
                resources: vec![hp_meta_data_resource],
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phneopacket);
    }

    #[rstest]
    fn test_collect_hpo_in_header_col(
        fractured_nose_pf: PhenotypicFeature,
        hp_meta_data_resource: Resource,
        pp_id: String,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(&temp_dir.path());
        let collector = PhenotypeCollector;

        let mut fractured_nose_excluded = fractured_nose_pf.clone();
        fractured_nose_excluded.excluded = true;

        let pneumonia_col = Column::new(
            format!(
                "{}#(block foo)",
                fractured_nose_excluded.r#type.clone().unwrap().id
            )
            .into(),
            [AnyValue::Boolean(false), AnyValue::Null],
        );
        let pneumonia_onset_col = Column::from(Series::full_null(
            "null_onset_col".into(),
            2,
            &DataType::String,
        ));

        let stringified_onset_col = pneumonia_onset_col.str().unwrap();

        collector
            .collect_hpo_in_header_col(
                &mut builder,
                "P002",
                &pp_id,
                &pneumonia_col,
                Some(stringified_onset_col),
            )
            .unwrap();

        let mut phenopackets = builder.build();

        let mut expected_phenopacket = Phenopacket {
            id: pp_id.to_string(),
            phenotypic_features: vec![fractured_nose_excluded],
            meta_data: Some(MetaData {
                resources: vec![hp_meta_data_resource],
                ..Default::default()
            }),
            ..Default::default()
        };

        pretty_assertions::assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_phenopacket);
    }
}
