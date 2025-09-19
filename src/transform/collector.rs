//todo get rid of these allowances below
#![allow(dead_code)]
#![allow(unused)]
use crate::config::table_context::Context::{HpoLabel, Onset, SubjectId};
use crate::config::table_context::TableContext;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::CollectionError;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use crate::transform::strategies::utils::convert_col_to_string_vec;
use phenopackets::schema::v1::core::PhenotypicFeature;
use phenopackets::schema::v2::Phenopacket;
use polars::prelude::{ChunkCompareEq, DataFrame, IntoLazy, col, lit};

struct Collector {
    phenopacket_builder: PhenopacketBuilder,
    cohort_name: String,
}

impl Collector {
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
                //this means that a phenopacket for the individual is created, even if there is no PF data
                //will possibly be made obsolete by later collect functions
                self.phenopacket_builder
                    .get_or_create_phenopacket(&phenopacket_id);

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
                self.collect_phenotypic_features(&patient_cdf, &phenopacket_id);

                //todo self.collect_individual(&patient_cdf, &phenopacket_id);
            }
        }

        Ok(self.phenopacket_builder.build())
    }

    fn collect_phenotypic_features(
        &mut self,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
    ) -> Result<(), TransformError> {
        let single_pf_cols = patient_cdf.get_cols_with_data_context(HpoLabel);

        for single_pf_col in single_pf_cols {
            let stringified_single_pf_col = convert_col_to_string_vec(single_pf_col)?;
            for pf in &stringified_single_pf_col {
                if pf == "null" {
                    continue;
                } else {
                    self.phenopacket_builder
                        .upsert_phenotypic_feature(
                            phenopacket_id,
                            pf,
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
                                "Error when upserting {}",
                                single_pf_col.name()
                            ))
                        })?
                }
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

    fn collect_individual(&mut self, patient_cdf: &ContextualizedDataFrame, phenopacket_id: &str) {
        // Find the necessary values to construct an individual building block and upsert them to the PhenopacketBuilder
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::Context::{HpoLabel, Onset, SubjectAge, SubjectId};
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
    use crate::ontology::traits::OntologyRegistry;
    use crate::ontology::utils::init_ontolius;
    use crate::transform::collector::Collector;
    use crate::transform::error::TransformError;
    use crate::transform::phenopacket_builder::PhenopacketBuilder;
    use crate::transform::traits::Strategy;
    use ontolius::ontology::csr::FullCsrOntology;
    use phenopackets::schema::v2::core::PhenotypicFeature;
    use polars::datatypes::AnyValue;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};
    use std::rc::Rc;
    use tempfile::TempDir;

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

    #[rstest]
    fn test_collect_phenotypic_features(tc: TableContext, mut collector: Collector) {
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

        let phenopacket_id = "cohort-P006".to_string();

        let collect_pfs_result = collector.collect_phenotypic_features(&cdf, &phenopacket_id);
        let phenopackets = collector.phenopacket_builder.build();
        assert_eq!(phenopackets.len(), 1);
        assert_eq!(phenopackets[0].id, phenopacket_id);
        assert_eq!(phenopackets[0].phenotypic_features.len(), 3);
        //todo I am not actually sure how to look through the PFs to see if they are the right ones
    }

    #[rstest]
    fn test_collect(tc: TableContext, mut collector: Collector) {
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
        let phenopackets = collector.phenopacket_builder.build();

        assert_eq!(phenopackets.len(), 3);
        for phenopacket in phenopackets {
            if phenopacket.id == "cohort2019-P001" {
                assert_eq!(phenopacket.phenotypic_features.len(), 1);
            }
            if phenopacket.id == "cohort2019-P002" {
                assert_eq!(phenopacket.phenotypic_features.len(), 3);
            }
            if phenopacket.id == "cohort2019-P003" {
                assert_eq!(phenopacket.phenotypic_features.len(), 0);
            }
        }
    }
}
