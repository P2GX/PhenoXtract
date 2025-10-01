use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::CollectionError;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use crate::transform::strategies::utils::convert_col_to_string_vec;
use log::warn;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::time_element::Element::Age;
use phenopackets::schema::v2::core::vital_status::Status;
use phenopackets::schema::v2::core::{Age as age_struct, TimeElement, VitalStatus};
use polars::prelude::{Column, IntoLazy, col, lit};
use std::collections::HashSet;

#[allow(dead_code)]
#[derive(Debug)]
pub struct Collector {
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
        cdfs: &[ContextualizedDataFrame],
    ) -> Result<Vec<Phenopacket>, TransformError> {
        for cdf in cdfs {
            let subject_id_cols = cdf.get_cols_with_data_context(&Context::SubjectId);
            if subject_id_cols.len() > 1 {
                return Err(CollectionError(format!(
                    "Multiple SubjectID columns were found in table {}.",
                    cdf.context().name
                )));
            }

            let subject_id_col = subject_id_cols.last().ok_or(CollectionError(format!(
                "Could not find SubjectID column in table {}",
                cdf.context().name
            )))?;
            let subject_id_col_name = subject_id_col.name().to_string();
            let unique_patient_ids =
                convert_col_to_string_vec(&subject_id_col.unique().map_err(|_| {
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
                    .map_err(|_| {
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

    //todo better tests after MVP, e.g. test the errors appear when they should
    fn collect_phenotypic_features(
        &mut self,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
    ) -> Result<(), TransformError> {
        let pf_scs =
            patient_cdf.get_series_context_with_contexts(&Context::None, &Context::HpoLabel);

        for pf_sc in pf_scs {
            let pf_cols = patient_cdf.get_columns(pf_sc.get_identifier());
            let linked_onset_cols = patient_cdf.get_building_block_with_contexts(
                pf_sc.get_building_block_id(),
                &Context::None,
                &Context::OnsetAge,
            );
            // it is very unclear how linking would work otherwise
            let valid_onset_linking = linked_onset_cols.len() == 1;

            for pf_col in pf_cols {
                let onset_col = if valid_onset_linking {
                    linked_onset_cols.first().unwrap()
                } else {
                    &&Column::new("OnsetAge".into(), vec!["null"; pf_col.len()])
                };

                let stringified_pf_col = convert_col_to_string_vec(pf_col)?;
                let stringified_onset_col = convert_col_to_string_vec(onset_col)?;

                let pf_onset_pairs: Vec<(&String, &String)> = stringified_pf_col
                    .iter()
                    .zip(stringified_onset_col.iter())
                    .collect();

                for (hpo_label, onset_age) in pf_onset_pairs {
                    if hpo_label == "null" {
                        if onset_age != "null" {
                            warn!(
                                "Non-null Onset {} found for null HPO Label in table {} for phenopacket {}",
                                onset_age,
                                patient_cdf.context().name,
                                phenopacket_id
                            );
                        }
                    } else {
                        let onset_te = if onset_age == "null" {
                            None
                        } else {
                            Some(TimeElement {
                                element: Some(Age(age_struct {
                                    iso8601duration: onset_age.to_string(),
                                })),
                            })
                        };

                        self.phenopacket_builder
                            .upsert_phenotypic_feature(
                                phenopacket_id,
                                hpo_label,
                                None,
                                None,
                                None,
                                None,
                                onset_te,
                                None,
                                None,
                            )
                            .map_err(|_| {
                                CollectionError(format!(
                                    "Error when upserting HPO term {} in column {}",
                                    hpo_label,
                                    pf_col.name()
                                ))
                            })?;
                    }
                }
            }
        }

        // todo deal with other types of pf col
        //ideally we will create a multi_pf col to several single_pf cols strategy
        //and we also want to deal with pf columns with observation status as data context

        Ok(())
    }

    fn collect_individual(
        &mut self,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
        patient_id: &str,
    ) -> Result<(), TransformError> {
        let subject_sex = Self::collect_single_multiplicity_element(
            patient_cdf,
            Context::SubjectSex,
            patient_id,
        )?;
        let vital_status_string = Self::collect_single_multiplicity_element(
            patient_cdf,
            Context::VitalStatus,
            patient_id,
        )?;
        let vital_status = match vital_status_string {
            None => None,
            Some(s) => {
                let status = Status::from_str_name(s.as_str()).ok_or(CollectionError(format!(
                    "Could not interpret {s} as status for {patient_id}."
                )))? as i32;
                Some(VitalStatus {
                    status,
                    ..Default::default()
                })
            }
        };

        self.phenopacket_builder
            .upsert_individual(
                phenopacket_id,
                patient_id,
                None,
                None,
                None,
                vital_status,
                subject_sex.as_deref(),
                None,
                None,
                None,
            )
            .map_err(|_| {
                CollectionError(format!(
                    "Error when upserting individual data for {phenopacket_id}"
                ))
            })?;
        Ok(())
    }

    /// Given a CDF corresponding to a single patient and a desired property (encoded by the variable context)
    /// for which there can only be ONE value, e.g. Age, Vital Status, Sex, Gender...
    /// this function will:
    /// -find all values for that context
    /// -throw an error if it finds multiple distinct values
    /// return Ok(None) if it finds no values
    /// return Ok(unique_val) if there is a single unique value
    fn collect_single_multiplicity_element(
        patient_cdf: &ContextualizedDataFrame,
        context: Context,
        patient_id: &str,
    ) -> Result<Option<String>, TransformError> {
        let cols_of_element_type = patient_cdf.get_cols_with_data_context(&context);

        if cols_of_element_type.is_empty() {
            return Ok(None);
        }

        let mut unique_values: HashSet<String> = HashSet::new();

        for col in cols_of_element_type {
            let stringified_col_no_nulls = convert_col_to_string_vec(&col.drop_nulls())?;
            stringified_col_no_nulls.iter().for_each(|val| {
                unique_values.insert(val.clone());
            });
        }

        if unique_values.len() > 1 {
            Err(CollectionError(format!(
                "Found multiple values of {context} for {patient_id} when there should only be one: {unique_values:?}."
            )))
        } else {
            Ok(unique_values.iter().next().cloned())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::test_utils::HPO;
    use crate::transform::collector::Collector;
    use crate::transform::error::TransformError::CollectionError;
    use crate::transform::phenopacket_builder::PhenopacketBuilder;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::time_element::Element::Age;
    use phenopackets::schema::v2::core::vital_status::Status;
    use phenopackets::schema::v2::core::{
        Age as age_struct, Individual, OntologyClass, PhenotypicFeature, Sex, TimeElement,
        VitalStatus,
    };
    use polars::datatypes::AnyValue;
    use polars::frame::DataFrame;
    use polars::prelude::{Column, NamedFrom, Series};
    use rstest::{fixture, rstest};

    fn init_collector() -> Collector {
        let phenopacket_builder = PhenopacketBuilder::new(HPO.clone());
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
            Context::SubjectId,
            None,
            None,
            None,
        );
        let pf_sc = SeriesContext::new(
            Identifier::Regex("phenotypic_features".to_string()),
            Context::None,
            Context::HpoLabel,
            None,
            None,
            Some("Block_1".to_string()),
        );
        let onset_sc = SeriesContext::new(
            Identifier::Regex("onset_age".to_string()),
            Context::None,
            Context::OnsetAge,
            None,
            None,
            Some("Block_1".to_string()),
        );
        let sex_sc = SeriesContext::new(
            Identifier::Regex("sex".to_string()),
            Context::None,
            Context::SubjectSex,
            None,
            None,
            None,
        );
        let vital_status_sc = SeriesContext::new(
            Identifier::Regex("vital_status".to_string()),
            Context::None,
            Context::VitalStatus,
            None,
            None,
            None,
        );
        TableContext::new(
            "patient_data".to_string(),
            vec![id_sc, pf_sc, onset_sc, sex_sc, vital_status_sc],
        )
    }

    #[fixture]
    fn pf_pneumonia() -> PhenotypicFeature {
        PhenotypicFeature {
            r#type: Some(OntologyClass {
                id: "HP:0002090".to_string(),
                label: "Pneumonia".to_string(),
            }),
            onset: Some(TimeElement {
                element: Some(Age(age_struct {
                    iso8601duration: "P40Y10M05D".to_string(),
                })),
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
            onset: Some(TimeElement {
                element: Some(Age(age_struct {
                    iso8601duration: "P12Y5M028D".to_string(),
                })),
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
            onset: Some(TimeElement {
                element: Some(Age(age_struct {
                    iso8601duration: "P48Y4M21D".to_string(),
                })),
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

    #[fixture]
    fn df_multi_patient() -> DataFrame {
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
                AnyValue::String("P40Y10M05D"),
                AnyValue::Null,
                AnyValue::String("P12Y5M028D"),
                AnyValue::String("P48Y4M21D"),
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        let subject_sex_col = Column::new(
            "sex".into(),
            [
                AnyValue::String("MALE"),
                AnyValue::String("MALE"),
                AnyValue::Null,
                AnyValue::String("FEMALE"),
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        let vital_status_col = Column::new(
            "vital_status".into(),
            [
                AnyValue::String("UNKNOWN_STATUS"),
                AnyValue::Null,
                AnyValue::String("ALIVE"),
                AnyValue::String("ALIVE"),
                AnyValue::String("ALIVE"),
                AnyValue::String("DECEASED"),
            ],
        );

        DataFrame::new(vec![
            id_col,
            pf_col,
            onset_col,
            subject_sex_col,
            vital_status_col,
        ])
        .unwrap()
    }

    #[fixture]
    fn df_single_patient() -> DataFrame {
        let id_col = Column::new("subject_id".into(), ["P006", "P006", "P006", "P006"]);
        let subject_sex_col = Column::new(
            "sex".into(),
            [
                AnyValue::String("FEMALE"),
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        let vital_status_col = Column::new(
            "vital_status".into(),
            [
                AnyValue::String("ALIVE"),
                AnyValue::String("ALIVE"),
                AnyValue::String("ALIVE"),
                AnyValue::Null,
            ],
        );
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
                AnyValue::String("P40Y10M05D"),
                AnyValue::Null,
                AnyValue::String("P12Y5M028D"),
                AnyValue::String("P48Y4M21D"),
            ],
        );
        DataFrame::new(vec![
            id_col,
            subject_sex_col,
            vital_status_col,
            pf_col,
            onset_col,
        ])
        .unwrap()
    }

    #[rstest]
    fn test_collect(
        df_multi_patient: DataFrame,
        tc: TableContext,
        pf_pneumonia: PhenotypicFeature,
        pf_asthma: PhenotypicFeature,
        pf_nail_psoriasis: PhenotypicFeature,
        pf_macrocephaly: PhenotypicFeature,
    ) {
        let mut collector = init_collector();

        let cdf = ContextualizedDataFrame::new(tc, df_multi_patient);

        let collect_result = collector.collect([cdf].as_slice());
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
            sex: Sex::Male as i32,
            vital_status: Some(VitalStatus {
                status: Status::UnknownStatus as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        let indiv2 = Individual {
            id: "P002".to_string(),
            sex: Sex::Female as i32,
            vital_status: Some(VitalStatus {
                status: Status::Alive as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        let indiv3 = Individual {
            id: "P003".to_string(),
            vital_status: Some(VitalStatus {
                status: Status::Deceased as i32,
                ..Default::default()
            }),
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
        pf_pneumonia: PhenotypicFeature,
        pf_asthma: PhenotypicFeature,
        pf_nail_psoriasis: PhenotypicFeature,
        df_single_patient: DataFrame,
    ) {
        let mut collector = init_collector();

        let patient_cdf = ContextualizedDataFrame::new(tc, df_single_patient);

        let phenopacket_id = "cohort2019-P006".to_string();

        let collect_pfs_result =
            collector.collect_phenotypic_features(&patient_cdf, &phenopacket_id);
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

    #[rstest]
    fn test_collect_individual(tc: TableContext, df_single_patient: DataFrame) {
        let mut collector = init_collector();

        let patient_cdf = ContextualizedDataFrame::new(tc, df_single_patient);

        let phenopacket_id = "cohort2019-P006".to_string();
        let patient_id = "P006".to_string();

        let collect_pfs_result =
            collector.collect_individual(&patient_cdf, &phenopacket_id, &patient_id);
        assert!(collect_pfs_result.is_ok());
        let phenopackets = collector.phenopacket_builder.build();

        let indiv = Individual {
            id: "P006".to_string(),
            sex: Sex::Female as i32,
            vital_status: Some(VitalStatus {
                status: Status::Alive as i32,
                ..Default::default()
            }),
            ..Default::default()
        };

        let expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            subject: Some(indiv),
            ..Default::default()
        };

        assert_eq!(phenopackets.len(), 1);
        assert_eq!(phenopackets[0], expected_p006);
    }

    #[rstest]
    fn test_collect_single_multiplicity_element_nulls_and_non_nulls(
        tc: TableContext,
        df_single_patient: DataFrame,
    ) {
        let patient_cdf = ContextualizedDataFrame::new(tc.clone(), df_single_patient.clone());
        let sme = Collector::collect_single_multiplicity_element(
            &patient_cdf,
            Context::SubjectSex,
            "P006",
        )
        .unwrap()
        .unwrap();
        assert_eq!(sme, "FEMALE");
    }

    #[rstest]
    fn test_collect_single_multiplicity_element_nulls(
        tc: TableContext,
        mut df_single_patient: DataFrame,
    ) {
        let null_subject_sex_col = Series::new(
            "sex".into(),
            [
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        df_single_patient
            .replace("sex", null_subject_sex_col)
            .unwrap();
        let patient_cdf = ContextualizedDataFrame::new(tc.clone(), df_single_patient.clone());
        let sme = Collector::collect_single_multiplicity_element(
            &patient_cdf,
            Context::SubjectSex,
            "P006",
        )
        .unwrap();
        assert_eq!(sme, None);
    }

    #[rstest]
    fn test_collect_single_multiplicity_element_multiple(
        tc: TableContext,
        mut df_single_patient: DataFrame,
    ) {
        let many_subject_sex_col = Series::new(
            "sex".into(),
            [
                AnyValue::String("MALE"),
                AnyValue::String("MALE"),
                AnyValue::String("MALE"),
                AnyValue::String("MALE"),
            ],
        );
        df_single_patient
            .replace("sex", many_subject_sex_col)
            .unwrap();
        let patient_cdf = ContextualizedDataFrame::new(tc.clone(), df_single_patient.clone());
        let sme = Collector::collect_single_multiplicity_element(
            &patient_cdf,
            Context::SubjectSex,
            "P006",
        )
        .unwrap()
        .unwrap();
        assert_eq!(sme, "MALE");
    }

    #[rstest]
    fn test_collect_single_multiplicity_element_err(
        tc: TableContext,
        mut df_single_patient: DataFrame,
    ) {
        let invalid_subject_sex_col = Series::new(
            "sex".into(),
            [
                AnyValue::String("FEMALE"),
                AnyValue::Null,
                AnyValue::String("MALE"),
                AnyValue::Null,
            ],
        );
        df_single_patient
            .replace("sex", invalid_subject_sex_col)
            .unwrap();
        let patient_cdf = ContextualizedDataFrame::new(tc.clone(), df_single_patient.clone());
        let sme = Collector::collect_single_multiplicity_element(
            &patient_cdf,
            Context::SubjectSex,
            "P006",
        );
        assert!(sme.is_err());
        assert!(
            sme.as_ref().err().unwrap()
                == &CollectionError(
                "Found multiple values of SubjectSex for P006 when there should only be one: {\"MALE\", \"FEMALE\"}."
                    .to_string(),
            )
                || sme.as_ref().err().unwrap()
                == &CollectionError(
                "Found multiple values of SubjectSex for P006 when there should only be one: {\"FEMALE\", \"MALE\"}."
                    .to_string(),
            )
        )
    }
}
