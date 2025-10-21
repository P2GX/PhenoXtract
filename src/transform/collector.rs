use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::CollectionError;
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use log::warn;
use phenopackets::schema::v2::Phenopacket;
use polars::prelude::{Column, DataType, Series, StringChunked};
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
        cdfs: Vec<ContextualizedDataFrame>,
    ) -> Result<Vec<Phenopacket>, TransformError> {
        for cdf in cdfs {
            let subject_id_cols = cdf
                .filter_columns()
                .where_data_context(Filter::Is(&Context::SubjectId))
                .collect();
            if subject_id_cols.len() > 1 {
                return Err(CollectionError(format!(
                    "Multiple SubjectID columns were found in table {}.",
                    cdf.context().name()
                )));
            }

            let subject_id_col = subject_id_cols.last().ok_or(CollectionError(format!(
                "Could not find SubjectID column in table {}",
                cdf.context().name()
            )))?;

            let patient_dfs = cdf
                .data()
                .partition_by(vec![subject_id_col.name().as_str()], true)
                .map_err(|_| {
                    CollectionError(format!(
                        "Error when partitioning dataframe {} by SubjectID column.",
                        cdf.context().name()
                    ))
                })?;

            for patient_df in patient_dfs.iter() {
                let patient_id = patient_df
                    .column(subject_id_col.name())
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .str_value();

                let phenopacket_id = format!("{}-{}", self.cohort_name.clone(), patient_id);

                let patient_cdf =
                    ContextualizedDataFrame::new(cdf.context().clone(), patient_df.clone());
                self.collect_individual(&patient_cdf, &phenopacket_id, &patient_id)?;
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

        let null_col = &&Column::from(Series::full_null(
            "null_col".into(),
            patient_cdf.data().height(),
            &DataType::String,
        ));

        for hpo_sc in hpo_scs {
            let sc_id = hpo_sc.get_identifier();
            let hpo_cols = patient_cdf.get_columns(sc_id);

            let linked_onset_cols = hpo_sc.get_building_block_id().map_or(vec![], |bb_id| {
                patient_cdf
                    .filter_columns()
                    .where_building_block(Filter::Is(bb_id))
                    .where_header_context(Filter::IsNone)
                    .where_data_context(Filter::Is(&Context::OnsetAge))
                    .where_data_context(Filter::Is(&Context::OnsetDateTime))
                    .collect()
            });

            let valid_onset_linking = linked_onset_cols.len() == 1;

            if linked_onset_cols.len() > 1 {
                warn!(
                    "Multiple onset columns for Series Context with identifier {sc_id:?} and Phenotypic Feature context. This cannot be interpreted and will be ignored."
                );
            }

            let onset_col = if valid_onset_linking {
                linked_onset_cols.first().unwrap()
            } else {
                null_col
            };

            let onset_col_cast_to_string = onset_col.cast(&DataType::String).map_err(|_| {
                CollectionError(format!(
                    "Could not cast column {} to String for phenopacket {}.",
                    onset_col.name(),
                    phenopacket_id
                ))
            })?;
            let stringified_onset_col = onset_col_cast_to_string.str().map_err(|_| {
                CollectionError(format!(
                    "Error when converting onset col {} to StringChunked.",
                    onset_col.name()
                ))
            })?;

            for hpo_col in hpo_cols {
                if hpo_sc.get_header_context() == &Context::None
                    && hpo_sc.get_data_context() == &Context::HpoLabelOrId
                {
                    self.collect_hpo_in_cells_col(phenopacket_id, hpo_col, stringified_onset_col)?;
                } else {
                    self.collect_hpo_in_header_col(phenopacket_id, hpo_col, stringified_onset_col)?;
                }
            }
        }

        Ok(())
    }

    fn collect_hpo_in_cells_col(
        &mut self,
        phenopacket_id: &str,
        patient_hpo_col: &Column,
        stringified_onset_col: &StringChunked,
    ) -> Result<(), TransformError> {
        let stringified_hpo_col = patient_hpo_col.str().map_err(|_| {
            CollectionError(format!(
                "Error when converting HPO col {} to StringChunked.",
                patient_hpo_col.name()
            ))
        })?;

        let hpo_onset_pairs = stringified_hpo_col.iter().zip(stringified_onset_col.iter());

        for (hpo, onset) in hpo_onset_pairs {
            if let Some(hpo) = hpo {
                self.phenopacket_builder
                    .upsert_phenotypic_feature(
                        phenopacket_id,
                        hpo,
                        None,
                        None,
                        None,
                        None,
                        onset,
                        None,
                        None,
                    )
                    .map_err(|_| {
                        CollectionError(format!(
                            "Error when upserting HPO term {} in column {}",
                            hpo,
                            patient_hpo_col.name()
                        ))
                    })?;
            } else if let Some(onset) = onset {
                warn!(
                    "Non-null Onset {} found for null HPO value in column {} for phenopacket {}",
                    onset,
                    patient_hpo_col.name(),
                    phenopacket_id
                );
            }
        }
        Ok(())
    }

    fn collect_hpo_in_header_col(
        &mut self,
        phenopacket_id: &str,
        patient_hpo_col: &Column,
        stringified_onset_col: &StringChunked,
    ) -> Result<(), TransformError> {
        let hpo = Self::get_hpo_from_col_header(patient_hpo_col);

        let boolified_hpo_col = patient_hpo_col.bool().map_err(|_| {
            CollectionError(format!(
                "Error when converting HPO col {} to BooleanChunked.",
                patient_hpo_col.name()
            ))
        })?;

        let bool_onset_pairs = boolified_hpo_col.iter().zip(stringified_onset_col.iter());

        //if the cell bool is null, no phenotype is upserted
        //if the cell bool is true, the phenotype is upserted with excluded = None
        //if the cell bool is false, the phenotype is upserted with excluded = true
        for (bool, onset) in bool_onset_pairs {
            if let Some(bool) = bool {
                let excluded = if bool { None } else { Some(true) };

                self.phenopacket_builder
                    .upsert_phenotypic_feature(
                        phenopacket_id,
                        hpo,
                        None,
                        excluded,
                        None,
                        None,
                        onset,
                        None,
                        None,
                    )
                    .map_err(|_| {
                        CollectionError(format!(
                            "Error when upserting HPO term {} in column {}",
                            hpo,
                            patient_hpo_col.name()
                        ))
                    })?;
            } else if let Some(onset) = onset {
                warn!(
                    "Non-null Onset {} found for null HPO value in column {} for phenopacket {}",
                    onset,
                    patient_hpo_col.name(),
                    phenopacket_id
                );
            }
        }
        Ok(())
    }

    fn collect_individual(
        &mut self,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
        patient_id: &str,
    ) -> Result<(), TransformError> {
        let date_of_birth = Self::collect_single_multiplicity_element(
            patient_cdf,
            Context::DateOfBirth,
            patient_id,
        )?;

        let subject_sex = Self::collect_single_multiplicity_element(
            patient_cdf,
            Context::SubjectSex,
            patient_id,
        )?;

        self.phenopacket_builder
            .upsert_individual(
                phenopacket_id,
                patient_id,
                None,
                date_of_birth.as_deref(),
                None,
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

        let status = Self::collect_single_multiplicity_element(
            patient_cdf,
            Context::VitalStatus,
            patient_id,
        )?;

        if let Some(status) = status {
            let time_of_death = Self::collect_single_multiplicity_element(
                patient_cdf,
                Context::TimeOfDeath,
                patient_id,
            )?;

            let cause_of_death = Self::collect_single_multiplicity_element(
                patient_cdf,
                Context::CauseOfDeath,
                patient_id,
            )?;

            let survival_time_days = Self::collect_single_multiplicity_element(
                patient_cdf,
                Context::SurvivalTimeDays,
                patient_id,
            )?;
            let survival_time_days = survival_time_days
                .map(|str| str.parse::<f64>().map(|f| f as u32))
                .transpose()
                .map_err(|_| {
                    CollectionError(format!(
                        "Could not parse survival time in days as u32 for {phenopacket_id}."
                    ))
                })?;

            self.phenopacket_builder
                .upsert_vital_status(
                    phenopacket_id,
                    status.as_str(),
                    time_of_death.as_deref(),
                    cause_of_death.as_deref(),
                    survival_time_days,
                )
                .map_err(|_| {
                    CollectionError(format!(
                        "Error when upserting vital status data for {phenopacket_id}"
                    ))
                })?;
        }

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
        let cols_of_element_type = patient_cdf
            .filter_columns()
            .where_data_context(Filter::Is(&context))
            .collect();

        if cols_of_element_type.is_empty() {
            return Ok(None);
        }

        let mut unique_values: HashSet<String> = HashSet::new();

        for col in cols_of_element_type {
            let stringified_col = col.cast(&DataType::String).map_err(|_| CollectionError(format!("Could not cast column {} to String when searching for {} element for patient {}.", col.name(), context, patient_id)))?;
            let stringified_col_str = stringified_col.str().map_err(|_| CollectionError(format!("Could not convert column {} to StringChunked when searching for {} element for patient {}.", col.name(), context, patient_id)))?;
            stringified_col_str.into_iter().for_each(|opt_val| {
                if let Some(val) = opt_val {
                    unique_values.insert(val.to_string());
                }
            });
        }

        if unique_values.len() > 1 {
            Err(CollectionError(format!(
                "Found multiple values of {context} for {patient_id} when there should only be one: {unique_values:?}."
            )))
        } else {
            match unique_values.iter().next() {
                Some(unique_val) => Ok(Some(unique_val.clone())),
                None => Ok(None),
            }
        }
    }

    /// after applying the MultiHPOColumnExpansion strategy
    /// the headers of HPO columns will have the format HP:1234567#(block A) or just HP:1234567
    /// this function takes such a header and returns just the HPO ID, i.e. HP:1234567
    fn get_hpo_from_col_header(col: &Column) -> &str {
        let split_col_name: Vec<&str> = col.name().split("#").collect();
        split_col_name[0]
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::test_utils::HPO_DICT;
    use crate::transform::collector::Collector;
    use crate::transform::error::TransformError::CollectionError;
    use crate::transform::phenopacket_builder::PhenopacketBuilder;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::time_element::Element::{Age, Timestamp};
    use phenopackets::schema::v2::core::vital_status::Status;
    use phenopackets::schema::v2::core::{
        Age as age_struct, Individual, OntologyClass, PhenotypicFeature, Sex, TimeElement,
        VitalStatus,
    };
    use polars::datatypes::{AnyValue, DataType};
    use polars::frame::DataFrame;
    use polars::prelude::{Column, NamedFrom, Series};
    use prost_types::Timestamp as TimestampProtobuf;
    use rstest::{fixture, rstest};

    fn init_collector() -> Collector {
        let phenopacket_builder = PhenopacketBuilder::new(HPO_DICT.clone());
        Collector {
            phenopacket_builder,
            cohort_name: "cohort2019".to_string(),
        }
    }

    #[fixture]
    fn tc() -> TableContext {
        let id_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("subject_id".to_string()))
            .with_data_context(Context::SubjectId);

        let pf_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("phenotypic_features".to_string()))
            .with_data_context(Context::HpoLabelOrId)
            .with_building_block_id(Some("Block_1".to_string()));

        let onset_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("onset_age".to_string()))
            .with_data_context(Context::OnsetAge)
            .with_building_block_id(Some("Block_1".to_string()));

        let dob_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("dob".to_string()))
            .with_data_context(Context::DateOfBirth);

        let sex_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("sex".to_string()))
            .with_data_context(Context::SubjectSex);

        let vital_status_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("vital_status".to_string()))
            .with_data_context(Context::VitalStatus);

        let time_of_death_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("time_of_death".to_string()))
            .with_data_context(Context::TimeOfDeath);

        let survival_time_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("survival_time".to_string()))
            .with_data_context(Context::SurvivalTimeDays);

        let runny_nose_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("HP:0031417".to_string()))
            .with_header_context(Context::HpoLabelOrId)
            .with_data_context(Context::ObservationStatus)
            .with_building_block_id(Some("Block_2".to_string()));

        let runny_nose_onset_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("runny_nose_onset".to_string()))
            .with_data_context(Context::OnsetDateTime)
            .with_building_block_id(Some("Block_2".to_string()));

        TableContext::new(
            "patient_data".to_string(),
            vec![
                id_sc,
                pf_sc,
                onset_sc,
                dob_sc,
                sex_sc,
                vital_status_sc,
                time_of_death_sc,
                survival_time_sc,
                runny_nose_sc,
                runny_nose_onset_sc,
            ],
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
    fn pf_pneumonia_no_onset() -> PhenotypicFeature {
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
            onset: Some(TimeElement {
                element: Some(Age(age_struct {
                    iso8601duration: "P12Y5M028D".to_string(),
                })),
            }),
            ..Default::default()
        }
    }

    #[fixture]
    fn pf_asthma_no_onset() -> PhenotypicFeature {
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
            onset: Some(TimeElement {
                element: Some(Age(age_struct {
                    iso8601duration: "P48Y4M21D".to_string(),
                })),
            }),
            ..Default::default()
        }
    }

    #[fixture]
    fn pf_nail_psoriasis_no_onset() -> PhenotypicFeature {
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

    #[fixture]
    fn pf_runny_nose() -> PhenotypicFeature {
        PhenotypicFeature {
            r#type: Some(OntologyClass {
                id: "HP:0031417".to_string(),
                label: "Rhinorrhea".to_string(),
            }),
            onset: Some(TimeElement {
                element: Some(Timestamp(TimestampProtobuf {
                    seconds: -154742400,
                    nanos: 0,
                })),
            }),
            ..Default::default()
        }
    }

    #[fixture]
    fn pf_runny_nose_excluded() -> PhenotypicFeature {
        PhenotypicFeature {
            r#type: Some(OntologyClass {
                id: "HP:0031417".to_string(),
                label: "Rhinorrhea".to_string(),
            }),
            excluded: true,
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
        let dob_col = Column::new(
            "dob".into(),
            [
                AnyValue::String("1960-02-05"),
                AnyValue::String("1960-02-05"),
                AnyValue::Null,
                AnyValue::String("1960-02-05"),
            ],
        );
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
        let time_of_death_col = Column::new(
            "time_of_death".into(),
            [
                AnyValue::String("2001-01-29"),
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        let survival_time_col = Column::new(
            "survival_time".into(),
            [
                AnyValue::Int32(155),
                AnyValue::Int32(155),
                AnyValue::Int32(155),
                AnyValue::Int32(155),
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
        let runny_nose_col = Column::new(
            "HP:0031417".into(),
            [
                AnyValue::Boolean(true),
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        let runny_nose_onset_col = Column::new(
            "runny_nose_onset".into(),
            [
                AnyValue::String("1965-02-05"),
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        DataFrame::new(vec![
            id_col,
            dob_col,
            subject_sex_col,
            vital_status_col,
            survival_time_col,
            time_of_death_col,
            pf_col,
            onset_col,
            runny_nose_col,
            runny_nose_onset_col,
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
        pf_runny_nose: PhenotypicFeature,
        df_single_patient: DataFrame,
    ) {
        let mut collector = init_collector();

        let patient_cdf = ContextualizedDataFrame::new(tc, df_single_patient);

        let phenopacket_id = "cohort2019-P006".to_string();
        collector
            .collect_phenotypic_features(&patient_cdf, &phenopacket_id)
            .unwrap();
        let phenopackets = collector.phenopacket_builder.build();

        let mut expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            ..Default::default()
        };
        expected_p006.phenotypic_features.push(pf_pneumonia);
        expected_p006.phenotypic_features.push(pf_asthma);
        expected_p006.phenotypic_features.push(pf_nail_psoriasis);
        expected_p006.phenotypic_features.push(pf_runny_nose);

        assert_eq!(phenopackets.len(), 1);
        assert_eq!(phenopackets[0], expected_p006);
    }

    #[rstest]
    fn test_collect_phenotypic_features_invalid_linking(
        tc: TableContext,
        mut df_single_patient: DataFrame,
        pf_asthma_no_onset: PhenotypicFeature,
        pf_pneumonia_no_onset: PhenotypicFeature,
        pf_nail_psoriasis_no_onset: PhenotypicFeature,
        pf_runny_nose: PhenotypicFeature,
    ) {
        let mut collector = init_collector();

        let onset_dt_col = Column::new(
            "onset_date".into(),
            [
                AnyValue::String("03.06.1956"),
                AnyValue::Null,
                AnyValue::String("26.04.2005"),
                AnyValue::String("16.02.1952"),
            ],
        );

        df_single_patient.with_column(onset_dt_col).unwrap();

        let mut patient_cdf = ContextualizedDataFrame::new(tc, df_single_patient);

        let onset_dt_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("onset_date".to_string()))
            .with_data_context(Context::OnsetDateTime)
            .with_building_block_id(Some("Block_1".to_string()));

        patient_cdf.add_series_context(onset_dt_sc);

        let phenopacket_id = "cohort2019-P006".to_string();
        collector
            .collect_phenotypic_features(&patient_cdf, &phenopacket_id)
            .unwrap();
        let phenopackets = collector.phenopacket_builder.build();

        let mut expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            ..Default::default()
        };
        expected_p006
            .phenotypic_features
            .push(pf_pneumonia_no_onset);
        expected_p006.phenotypic_features.push(pf_asthma_no_onset);
        expected_p006
            .phenotypic_features
            .push(pf_nail_psoriasis_no_onset);
        expected_p006.phenotypic_features.push(pf_runny_nose);

        assert_eq!(phenopackets.len(), 1);
        assert_eq!(phenopackets[0], expected_p006);
    }

    #[rstest]
    fn test_collect_hpo_in_cells_col(
        pf_pneumonia: PhenotypicFeature,
        pf_asthma_no_onset: PhenotypicFeature,
        pf_nail_psoriasis: PhenotypicFeature,
    ) {
        let mut collector = init_collector();

        let patient_hpo_col = Column::new(
            "phenotypic_features".into(),
            [
                AnyValue::String("Pneumonia"),
                AnyValue::Null,
                AnyValue::String("Asthma"),
                AnyValue::String("Nail psoriasis"),
            ],
        );
        let patient_onset_col = Column::new(
            "onset_age".into(),
            [
                AnyValue::String("P40Y10M05D"),
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::String("P48Y4M21D"),
            ],
        );

        let stringified_onset_col = patient_onset_col.str().unwrap();

        let phenopacket_id = "cohort2019-P006".to_string();
        collector
            .collect_hpo_in_cells_col(&phenopacket_id, &patient_hpo_col, &stringified_onset_col)
            .unwrap();
        let phenopackets = collector.phenopacket_builder.build();

        let mut expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            ..Default::default()
        };
        expected_p006.phenotypic_features.push(pf_pneumonia);
        expected_p006.phenotypic_features.push(pf_asthma_no_onset);
        expected_p006.phenotypic_features.push(pf_nail_psoriasis);

        assert_eq!(phenopackets.len(), 1);
        assert_eq!(phenopackets[0], expected_p006);
    }

    #[rstest]
    fn test_collect_hpo_in_header_col(pf_runny_nose_excluded: PhenotypicFeature) {
        let mut collector = init_collector();

        let patient_hpo_col = Column::new(
            "HP:0031417#(block foo)".into(),
            [AnyValue::Boolean(false), AnyValue::Null],
        );
        let patient_onset_col = Column::from(Series::full_null(
            "null_onset_col".into(),
            2,
            &DataType::String,
        ));

        let stringified_onset_col = patient_onset_col.str().unwrap();

        let phenopacket_id = "cohort2019-P006".to_string();
        collector
            .collect_hpo_in_header_col(&phenopacket_id, &patient_hpo_col, &stringified_onset_col)
            .unwrap();
        let phenopackets = collector.phenopacket_builder.build();

        let mut expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            ..Default::default()
        };
        expected_p006
            .phenotypic_features
            .push(pf_runny_nose_excluded);

        assert_eq!(phenopackets.len(), 1);
        assert_eq!(phenopackets[0], expected_p006);
    }

    #[rstest]
    fn test_collect_individual(tc: TableContext, df_single_patient: DataFrame) {
        let mut collector = init_collector();

        let patient_cdf = ContextualizedDataFrame::new(tc, df_single_patient);

        let phenopacket_id = "cohort2019-P006".to_string();
        let patient_id = "P006".to_string();

        collector
            .collect_individual(&patient_cdf, &phenopacket_id, &patient_id)
            .unwrap();

        let phenopackets = collector.phenopacket_builder.build();

        let indiv = Individual {
            id: "P006".to_string(),
            date_of_birth: Some(TimestampProtobuf {
                seconds: -312595200,
                nanos: 0,
            }),
            sex: Sex::Female as i32,
            vital_status: Some(VitalStatus {
                status: Status::Alive as i32,
                time_of_death: Some(TimeElement {
                    element: Some(Timestamp(TimestampProtobuf {
                        seconds: 980726400,
                        nanos: 0,
                    })),
                }),
                cause_of_death: None,
                survival_time_in_days: 155,
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

    #[rstest]
    fn test_get_hpo_from_col_header() {
        let hpo_col = Column::new("HP:1234567#(block A)".into(), vec![true, true, false]);
        assert_eq!("HP:1234567", Collector::get_hpo_from_col_header(&hpo_col));
        let hpo_col2 = Column::new("HP:1234567".into(), vec![true, true, false]);
        assert_eq!("HP:1234567", Collector::get_hpo_from_col_header(&hpo_col2));
    }
}
