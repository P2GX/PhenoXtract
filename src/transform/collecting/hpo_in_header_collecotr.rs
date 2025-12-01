use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::PhenopacketBuilder;
use crate::transform::collecting::traits::Collect;
use crate::transform::error::CollectorError;
use crate::transform::utils::HpoColMaker;
use log::warn;
use std::any::Any;
use std::collections::HashSet;

#[derive(Debug)]
pub struct HpoInHeaderCollector;

impl Collect for HpoInHeaderCollector {
    fn collect(
        &self,
        builder: &mut PhenopacketBuilder,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
    ) -> Result<(), CollectorError> {
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

            let stringified_linked_onset_col = patient_cdf.get_single_linked_column(
                hpo_sc.get_building_block_id(),
                &[Context::OnsetAge, Context::OnsetDateTime],
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

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TableContext;
    use crate::config::table_context::SeriesContext;
    use crate::extract::ContextualizedDataFrame;
    use crate::test_utils::{
        assert_phenopackets, build_test_phenopacket_builder, generate_minimal_cdf,
        generate_minimal_cdf_components,
    };
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::time_element::Element;
    use phenopackets::schema::v2::core::{
        Age, MetaData, OntologyClass, PhenotypicFeature, Resource, TimeElement,
    };
    use polars::datatypes::{AnyValue, DataType};
    use polars::prelude::{Column, DataFrame, IntoColumn, NamedFrom, Series};
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
    fn test_collect_hpo_in_header_col(
        fractured_nose_pf: PhenotypicFeature,
        hp_meta_data_resource: Resource,
        pp_id: String,
        temp_dir: TempDir,
    ) {
        let mut builder = build_test_phenopacket_builder(temp_dir.path());
        let collector = HpoInHeaderCollector;

        let (patient_col, sc) = generate_minimal_cdf_components(1, 2);

        let mut fractured_nose_excluded = fractured_nose_pf.clone();
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
        collector.collect(&mut builder, &cdf, &pp_id).unwrap();

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
