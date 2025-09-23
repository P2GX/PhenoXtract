use crate::config::table_context::Context::{HpoLabel, SubjectId};
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::CollectionError;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use crate::transform::strategies::utils::convert_col_to_string_vec;
use phenopackets::schema::v2::Phenopacket;
use polars::prelude::{IntoLazy, col, lit};

#[allow(dead_code)]
struct Collector {
    phenopacket_builder: PhenopacketBuilder,
    cohort_name: String,
}

#[allow(dead_code)]
impl Collector {
    pub fn new(phenopacket_builder: PhenopacketBuilder, cohort_name: String) -> Collector {
        Collector {
            phenopacket_builder,
            cohort_name,
        }
    }
    pub fn collect(
        &mut self,
        cdfs: Vec<ContextualizedDataFrame>,
    ) -> Result<Vec<Phenopacket>, TransformError> {
        for cdf in cdfs {
            let subject_id_col =
                cdf.get_cols_with_data_context(SubjectId)
                    .pop()
                    .ok_or(CollectionError(format!(
                        "Could not find SubjectID column in table {}",
                        cdf.context().name
                    )))?;
            let subject_id_col_name = subject_id_col.name().to_string();
            let unique_patient_ids =
                convert_col_to_string_vec(&subject_id_col.unique().map_err(|_err| {
                    CollectionError(format!(
                        "Failed to extract unique subject IDs from {subject_id_col_name}"
                    ))
                })?)?;

            for patient_id in &unique_patient_ids {
                let phenopacket_id = format!("{}-{}", self.cohort_name.clone(), patient_id);

                let patient_df = cdf
                    .data
                    .clone()
                    .lazy()
                    .filter(col(&subject_id_col_name).eq(lit(patient_id.clone())))
                    .collect()
                    .map_err(|_err| {
                        CollectionError(format!(
                            "Could not extract sub-Dataframe for patient {} in table {}.",
                            patient_id,
                            cdf.context().name
                        ))
                    })?;
                let patient_cdf = ContextualizedDataFrame::new(cdf.context().clone(), patient_df);
                self.collect_individual(&patient_cdf, &phenopacket_id, patient_id)?;
                self.collect_phenotypic_features(&patient_cdf, &phenopacket_id)?;
            }
        }

        Ok(self.phenopacket_builder.build())
    }

    fn collect_phenotypic_features(
        &mut self,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
    ) -> Result<(), TransformError> {
        let pf_cols = patient_cdf.get_cols_with_data_context(HpoLabel);

        for pf_col in pf_cols {
            let stringified_pf_col_no_nulls = convert_col_to_string_vec(&pf_col.drop_nulls())?;
            for hpo_label in &stringified_pf_col_no_nulls {
                self.phenopacket_builder
                    .upsert_phenotypic_feature(
                        phenopacket_id,
                        hpo_label,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    .map_err(|_err| {
                        CollectionError(format!("Error when upserting {}", pf_col.name()))
                    })?
            }
        }

        // todo deal with other types of pf col
        /*let pf_observation_cols = patient_cdf.get_cols_with_header_and_data_context(HpoLabel,ObservationStatus);*/
        /*let multi_pf_cols = patient_cdf.get_cols_with_data_context(MultiHpoLabel);*/

        // todo deal with onset, severity etc.
        // todo I think this will involve refactoring linking a lot, if we want to do this well and logically
        /*let onset_scs = patient_cdf.get_cols_with_data_context(Onset);*/

        Ok(())
    }

    fn collect_individual(
        &mut self,
        _patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
        patient_id: &str,
    ) -> Result<(), TransformError> {
        // Find the necessary values to construct an individual building block and upsert them to the PhenopacketBuilder
        // PLACEHOLDER CODE!
        self.phenopacket_builder
            .upsert_individual(
                phenopacket_id,
                patient_id,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .map_err(|_err| {
                CollectionError(format!(
                    "Error when upserting individual data for {phenopacket_id}"
                ))
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::Context::{HpoLabel, Onset, SubjectId};
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
    use crate::ontology::traits::OntologyRegistry;
    use crate::ontology::utils::init_ontolius;
    use crate::transform::collector::Collector;
    use crate::transform::phenopacket_builder::PhenopacketBuilder;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::{Individual, OntologyClass, PhenotypicFeature};
    use polars::datatypes::AnyValue;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;

    #[fixture]
    fn collector() -> Collector {
        let tmp = TempDir::new().unwrap();
        let hpo_registry = GithubOntologyRegistry::default_hpo_registry()
            .unwrap()
            .with_registry_path(tmp.path().into());
        let hpo_path = hpo_registry.register("latest").unwrap();
        let hpo_ontology = init_ontolius(hpo_path).unwrap();
        let phenopacket_builder = PhenopacketBuilder::new(hpo_ontology);
        Collector {
            phenopacket_builder,
            cohort_name: "cohort2019".to_string(),
        }
    }

    #[fixture]
    fn tc() -> TableContext {
        let id_sc = SeriesContext::new(
            Identifier::Regex("subject_id".to_string()),
            Context::None,
            SubjectId,
            None,
            None,
            vec![],
        );
        let pf_sc = SeriesContext::new(
            Identifier::Regex("phenotypic_features".to_string()),
            Context::None,
            HpoLabel,
            None,
            None,
            vec![],
        );
        let onset_sc = SeriesContext::new(
            Identifier::Regex("onset_age".to_string()),
            Context::None,
            Onset,
            None,
            None,
            vec![],
        );
        TableContext::new("patient_data".to_string(), vec![id_sc, pf_sc, onset_sc])
    }

    #[fixture]
    fn pf_pneumonia() -> PhenotypicFeature {
        PhenotypicFeature {
            r#type: Some(OntologyClass {
                id: "HP:0002090".to_string(),
                label: "Pneumonia".to_string(),
            }),
            ..Default::default()
        }
    }

    #[fixture]
    fn pf_asthma() -> PhenotypicFeature {
        PhenotypicFeature {
            r#type: Some(OntologyClass {
                id: "HP:0002099".to_string(),
                label: "Asthma".to_string(),
            }),
            ..Default::default()
        }
    }

    #[fixture]
    fn pf_nail_psoriasis() -> PhenotypicFeature {
        PhenotypicFeature {
            r#type: Some(OntologyClass {
                id: "HP:0033327".to_string(),
                label: "Nail psoriasis".to_string(),
            }),
            ..Default::default()
        }
    }

    #[fixture]
    fn pf_macrocephaly() -> PhenotypicFeature {
        PhenotypicFeature {
            r#type: Some(OntologyClass {
                id: "HP:0000256".to_string(),
                label: "Macrocephaly".to_string(),
            }),
            ..Default::default()
        }
    }

    #[rstest]
    fn test_collect(
        tc: TableContext,
        mut collector: Collector,
        pf_pneumonia: PhenotypicFeature,
        pf_asthma: PhenotypicFeature,
        pf_nail_psoriasis: PhenotypicFeature,
        pf_macrocephaly: PhenotypicFeature,
    ) {
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_collect");
            return;
        }

        let id_col = Column::new(
            "subject_id".into(),
            ["P001", "P001", "P002", "P002", "P002", "P003"],
        );
        let pf_col = Column::new(
            "phenotypic_features".into(),
            [
                AnyValue::String("Pneumonia"),
                AnyValue::Null,
                AnyValue::String("Asthma"),
                AnyValue::String("Nail psoriasis"),
                AnyValue::String("Macrocephaly"),
                AnyValue::Null,
            ],
        );
        let onset_col = Column::new(
            "onset_age".into(),
            [
                AnyValue::Int32(15),
                AnyValue::Null,
                AnyValue::Int32(65),
                AnyValue::Int32(82),
                AnyValue::Int32(20),
                AnyValue::Null,
            ],
        );
        let df = DataFrame::new(vec![id_col, pf_col, onset_col]).unwrap();
        let cdf = ContextualizedDataFrame::new(tc, df);

        let collect_result = collector.collect(vec![cdf]);
        let phenopackets = collect_result.unwrap();

        let mut expected_p001 = Phenopacket {
            id: "cohort2019-P001".to_string(),
            ..Default::default()
        };
        let mut expected_p002 = Phenopacket {
            id: "cohort2019-P002".to_string(),
            ..Default::default()
        };
        let mut expected_p003 = Phenopacket {
            id: "cohort2019-P003".to_string(),
            ..Default::default()
        };
        let indiv1 = Individual {
            id: "P001".to_string(),
            ..Default::default()
        };
        let indiv2 = Individual {
            id: "P002".to_string(),
            ..Default::default()
        };
        let indiv3 = Individual {
            id: "P003".to_string(),
            ..Default::default()
        };
        expected_p001.subject = Some(indiv1);
        expected_p001.phenotypic_features.push(pf_pneumonia);
        expected_p002.subject = Some(indiv2);
        expected_p002.phenotypic_features.push(pf_asthma);
        expected_p002.phenotypic_features.push(pf_nail_psoriasis);
        expected_p002.phenotypic_features.push(pf_macrocephaly);
        expected_p003.subject = Some(indiv3);

        assert_eq!(phenopackets.len(), 3);
        for phenopacket in phenopackets {
            if phenopacket.id == "cohort2019-P001" {
                assert_eq!(phenopacket, expected_p001);
            }
            if phenopacket.id == "cohort2019-P002" {
                assert_eq!(phenopacket, expected_p002);
            }
            if phenopacket.id == "cohort2019-P003" {
                assert_eq!(phenopacket, expected_p003);
            }
        }
    }

    #[rstest]
    fn test_collect_phenotypic_features(
        tc: TableContext,
        mut collector: Collector,
        pf_pneumonia: PhenotypicFeature,
        pf_asthma: PhenotypicFeature,
        pf_nail_psoriasis: PhenotypicFeature,
    ) {
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_collect_phenotypic_features");
            return;
        }

        let id_col = Column::new("subject_id".into(), ["P006", "P006", "P006", "P006"]);
        let pf_col = Column::new(
            "phenotypic_features".into(),
            [
                AnyValue::String("Pneumonia"),
                AnyValue::Null,
                AnyValue::String("Asthma"),
                AnyValue::String("Nail psoriasis"),
            ],
        );
        let onset_col = Column::new(
            "onset_age".into(),
            [
                AnyValue::Int32(15),
                AnyValue::Null,
                AnyValue::Int32(65),
                AnyValue::Int32(82),
            ],
        );
        let df = DataFrame::new(vec![id_col, pf_col, onset_col]).unwrap();
        let cdf = ContextualizedDataFrame::new(tc, df);

        let phenopacket_id = "cohort2019-P006".to_string();

        let collect_pfs_result = collector.collect_phenotypic_features(&cdf, &phenopacket_id);
        assert!(collect_pfs_result.is_ok());
        let phenopackets = collector.phenopacket_builder.build();

        let mut expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            ..Default::default()
        };
        expected_p006.phenotypic_features.push(pf_pneumonia);
        expected_p006.phenotypic_features.push(pf_asthma);
        expected_p006.phenotypic_features.push(pf_nail_psoriasis);

        assert_eq!(phenopackets.len(), 1);
        assert_eq!(phenopackets[0], expected_p006);
    }
}
