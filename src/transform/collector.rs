use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::error::{CollectorError, DataProcessingError};
use crate::transform::phenopacket_builder::PhenopacketBuilder;
use crate::transform::utils::HpoColMaker;
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
    ) -> Result<Vec<Phenopacket>, CollectorError> {
        for cdf in cdfs {
            let subject_id_cols = cdf
                .filter_columns()
                .where_data_context(Filter::Is(&Context::SubjectId))
                .collect();
            if subject_id_cols.len() > 1 {
                return Err(CollectorError::ExpectedSingleColumn {
                    table_name: cdf.context().name().to_string(),
                    context: Context::SubjectId,
                });
            }

            let subject_id_col = subject_id_cols
                .last()
                .ok_or(DataProcessingError::EmptyFilteringError)?;

            let patient_dfs = cdf
                .data()
                .partition_by(vec![subject_id_col.name().as_str()], true)?;

            for patient_df in patient_dfs.iter() {
                let patient_id = patient_df
                    .column(subject_id_col.name())?
                    .get(0)?
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

    fn collect_individual(
        &mut self,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
        patient_id: &str,
    ) -> Result<(), CollectorError> {
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

        self.phenopacket_builder.upsert_individual(
            phenopacket_id,
            patient_id,
            None,
            date_of_birth.as_deref(),
            None,
            subject_sex.as_deref(),
            None,
            None,
            None,
        )?;

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
                .transpose()?;

            self.phenopacket_builder.upsert_vital_status(
                phenopacket_id,
                status.as_str(),
                time_of_death.as_deref(),
                cause_of_death.as_deref(),
                survival_time_days,
            )?;
        }

        Ok(())
    }

    fn collect_phenotypic_features(
        &mut self,
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

        let null_col = &&Column::from(Series::full_null(
            "null_col".into(),
            patient_cdf.data().height(),
            &DataType::String,
        ));

        for hpo_sc in hpo_scs {
            let sc_id = hpo_sc.get_identifier();
            let hpo_cols = patient_cdf.get_columns(sc_id);

            if hpo_cols.is_empty() {
                warn!(
                    "HPO Series context {hpo_sc:?} found with no associated columns. It has been ignored during collection."
                );
                continue;
            }

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
                DataProcessingError::CastingError {
                    col_name: onset_col.name().to_string(),
                    from: onset_col.dtype().clone(),
                    to: DataType::String,
                }
            })?;
            let stringified_onset_col = onset_col_cast_to_string.str()?;

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
    ) -> Result<(), CollectorError> {
        let stringified_hpo_col = patient_hpo_col.str()?;

        let hpo_onset_pairs = stringified_hpo_col.iter().zip(stringified_onset_col.iter());

        for (hpo, onset) in hpo_onset_pairs {
            if let Some(hpo) = hpo {
                self.phenopacket_builder.upsert_phenotypic_feature(
                    phenopacket_id,
                    hpo,
                    None,
                    None,
                    None,
                    None,
                    onset,
                    None,
                    None,
                )?;
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
    ) -> Result<(), CollectorError> {
        let hpo_id = HpoColMaker::new().decode_column_header(patient_hpo_col).0;

        let boolified_hpo_col = patient_hpo_col.bool()?;

        let bool_onset_pairs = boolified_hpo_col.iter().zip(stringified_onset_col.iter());

        //if the cell bool is null, no phenotype is upserted
        //if the cell bool is true, the phenotype is upserted with excluded = None
        //if the cell bool is false, the phenotype is upserted with excluded = true
        for (observation_status, onset) in bool_onset_pairs {
            if let Some(observation_status) = observation_status {
                let excluded = if observation_status { None } else { Some(true) };

                self.phenopacket_builder.upsert_phenotypic_feature(
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
                    "Non-null Onset {} found for null HPO value in column {} for phenopacket {}",
                    onset,
                    patient_hpo_col.name(),
                    phenopacket_id
                );
            }
        }
        Ok(())
    }

    /// Finds all diseases associated with a patient and gives them to the phenopacket builder
    /// as interpretations.
    fn collect_interpretations(
        &mut self,
        patient_cdf: &ContextualizedDataFrame,
        phenopacket_id: &str,
    ) -> Result<(), CollectorError> {
        let disease_in_cells_scs = patient_cdf
            .filter_series_context()
            .where_header_context(Filter::Is(&Context::None))
            .where_data_context(Filter::Is(&Context::OmimLabelOrId))
            .where_data_context(Filter::Is(&Context::OrphanetLabelOrId))
            .where_data_context(Filter::Is(&Context::MondoLabelOrId))
            .collect();

        for disease_sc in disease_in_cells_scs {
            let sc_id = disease_sc.get_identifier();

            let stringified_disease_cols = patient_cdf
                .get_columns(sc_id)
                .iter()
                .map(|col| col.str().unwrap())
                .collect::<Vec<&StringChunked>>();

            let n_rows = patient_cdf.data().height();

            for row_idx in 0..n_rows {
                for stringified_disease_col in stringified_disease_cols.iter() {
                    let disease = stringified_disease_col.get(row_idx);
                    if let Some(disease) = disease {
                        let (term, _res_ref) = self
                            .phenopacket_builder
                            .query_disease_identifiers(disease)?;
                        let interpretation_id = format!("{}-{}", phenopacket_id, term.id);

                        self.phenopacket_builder.upsert_interpretation(
                            phenopacket_id,
                            interpretation_id.as_str(),
                            disease,
                        )?;

                        self.phenopacket_builder.upsert_disease(
                            phenopacket_id,
                            disease,
                            None,
                            None,
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
    ) -> Result<Option<String>, CollectorError> {
        let cols_of_element_type = patient_cdf
            .filter_columns()
            .where_data_context(Filter::Is(&context))
            .collect();

        if cols_of_element_type.is_empty() {
            return Ok(None);
        }

        let mut unique_values: HashSet<String> = HashSet::new();

        for col in cols_of_element_type {
            let stringified_col =
                col.cast(&DataType::String)
                    .map_err(|_| DataProcessingError::CastingError {
                        col_name: col.name().to_string(),
                        from: col.dtype().clone(),
                        to: DataType::String,
                    })?;
            let stringified_col_str = stringified_col.str()?;
            stringified_col_str.into_iter().for_each(|opt_val| {
                if let Some(val) = opt_val {
                    unique_values.insert(val.to_string());
                }
            });
        }

        if unique_values.len() > 1 {
            Err(CollectorError::ExpectedSingleValue {
                table_name: patient_cdf.context().name().to_string(),
                patient_id: patient_id.to_string(),
                context,
            })
        } else {
            match unique_values.iter().next() {
                Some(unique_val) => Ok(Some(unique_val.clone())),
                None => Ok(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::ontology::ontology_bidict::OntologyBiDict;
    use crate::ontology::traits::HasPrefixId;
    use crate::test_utils::{
        GENO_REF, HPO_REF, MONDO_BIDICT, ONTOLOGY_FACTORY, assert_phenopackets,
    };
    use crate::transform::collector::Collector;
    use crate::transform::phenopacket_builder::PhenopacketBuilder;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::time_element::Element::{Age, Timestamp};
    use phenopackets::schema::v2::core::vital_status::Status;
    use phenopackets::schema::v2::core::{
        Age as age_struct, Diagnosis, Individual, Interpretation, MetaData, OntologyClass,
        PhenotypicFeature, Resource, Sex, TimeElement, VitalStatus,
    };
    use polars::datatypes::{AnyValue, DataType};
    use polars::frame::DataFrame;
    use polars::prelude::{Column, NamedFrom, Series};
    use pretty_assertions::assert_eq;
    use prost_types::Timestamp as TimestampProtobuf;
    use rstest::{fixture, rstest};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn build_test_dicts() -> HashMap<String, Arc<OntologyBiDict>> {
        let hpo_dict = ONTOLOGY_FACTORY
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .build_bidict(&HPO_REF.clone(), None)
            .unwrap();

        let geno_dict = ONTOLOGY_FACTORY
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .build_bidict(&GENO_REF.clone(), None)
            .unwrap();

        HashMap::from_iter(vec![
            (hpo_dict.ontology.prefix_id().to_string(), hpo_dict),
            (
                MONDO_BIDICT.ontology.prefix_id().to_string(),
                MONDO_BIDICT.clone(),
            ),
            (geno_dict.ontology.prefix_id().to_string(), geno_dict),
        ])
    }
    fn build_test_phenopacket_builder() -> PhenopacketBuilder {
        PhenopacketBuilder::new(build_test_dicts())
    }
    fn init_test_collector() -> Collector {
        let phenopacket_builder = build_test_phenopacket_builder();

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

        let diseases_sc = SeriesContext::default()
            .with_identifier(Identifier::Regex("diseases".to_string()))
            .with_data_context(Context::MondoLabelOrId);

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
                diseases_sc,
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
    fn mondo_meta_data_resource() -> Resource {
        Resource {
            id: "mondo".to_string(),
            name: "Mondo Disease Ontology".to_string(),
            url: "http://purl.obolibrary.org/obo/mondo.json".to_string(),
            version: "2025-10-07".to_string(),
            namespace_prefix: "MONDO".to_string(),
            iri_prefix: "http://purl.obolibrary.org/obo/MONDO_$1".to_string(),
        }
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
        let disease_col = Column::new(
            "diseases".into(),
            [
                AnyValue::String("platelet signal processing defect"),
                AnyValue::Null,
                AnyValue::String("MONDO:0008258"), //also platelet signal processing defect
                AnyValue::String("Spondylocostal Dysostosis"),
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
            disease_col,
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
        hp_meta_data_resource: Resource,
    ) {
        let mut collector = init_test_collector();

        let cdf = ContextualizedDataFrame::new(tc, df_multi_patient);

        let collect_result = collector.collect(vec![cdf]);
        let phenopackets = collect_result.unwrap();
        let meta_data = Some(MetaData {
            resources: vec![hp_meta_data_resource.clone()],
            ..Default::default()
        });
        let mut expected_p001 = Phenopacket {
            id: "cohort2019-P001".to_string(),
            subject: Some(Individual {
                id: "P001".to_string(),
                sex: Sex::Male as i32,
                vital_status: Some(VitalStatus {
                    status: Status::UnknownStatus as i32,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta_data: meta_data.clone(),
            ..Default::default()
        };
        let mut expected_p002 = Phenopacket {
            id: "cohort2019-P002".to_string(),
            subject: Some(Individual {
                id: "P002".to_string(),
                sex: Sex::Female as i32,
                vital_status: Some(VitalStatus {
                    status: Status::Alive as i32,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            meta_data: meta_data.clone(),
            ..Default::default()
        };
        let expected_p003 = Phenopacket {
            id: "cohort2019-P003".to_string(),
            subject: Some(Individual {
                id: "P003".to_string(),
                vital_status: Some(VitalStatus {
                    status: Status::Deceased as i32,
                    ..Default::default()
                }),

                ..Default::default()
            }),
            meta_data: Some(MetaData::default()),
            ..Default::default()
        };

        expected_p001.phenotypic_features.push(pf_pneumonia);
        expected_p002.phenotypic_features.push(pf_asthma);
        expected_p002.phenotypic_features.push(pf_nail_psoriasis);
        expected_p002.phenotypic_features.push(pf_macrocephaly);

        assert_eq!(phenopackets.len(), 3);
        for mut phenopacket in phenopackets {
            if phenopacket.id == "cohort2019-P001" {
                assert_phenopackets(&mut phenopacket, &mut expected_p001.clone());
            }
            if phenopacket.id == "cohort2019-P002" {
                assert_phenopackets(&mut phenopacket, &mut expected_p002.clone());
            }
            if phenopacket.id == "cohort2019-P003" {
                assert_phenopackets(&mut phenopacket, &mut expected_p003.clone());
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
        hp_meta_data_resource: Resource,
    ) {
        let mut collector = init_test_collector();

        let patient_cdf = ContextualizedDataFrame::new(tc, df_single_patient);

        let phenopacket_id = "cohort2019-P006".to_string();
        collector
            .collect_phenotypic_features(&patient_cdf, &phenopacket_id)
            .unwrap();
        let mut phenopackets = collector.phenopacket_builder.build();

        let mut expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            meta_data: Some(MetaData {
                resources: vec![hp_meta_data_resource],
                ..Default::default()
            }),
            ..Default::default()
        };
        expected_p006.phenotypic_features.push(pf_pneumonia);
        expected_p006.phenotypic_features.push(pf_asthma);
        expected_p006.phenotypic_features.push(pf_nail_psoriasis);
        expected_p006.phenotypic_features.push(pf_runny_nose);

        assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_p006);
    }

    #[rstest]
    fn test_collect_phenotypic_features_invalid_linking(
        tc: TableContext,
        mut df_single_patient: DataFrame,
        pf_asthma_no_onset: PhenotypicFeature,
        pf_pneumonia_no_onset: PhenotypicFeature,
        pf_nail_psoriasis_no_onset: PhenotypicFeature,
        pf_runny_nose: PhenotypicFeature,
        hp_meta_data_resource: Resource,
    ) {
        let mut collector = init_test_collector();

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

        patient_cdf
            .builder()
            .add_series_context(onset_dt_sc)
            .unwrap()
            .build()
            .unwrap();

        let phenopacket_id = "cohort2019-P006".to_string();
        collector
            .collect_phenotypic_features(&patient_cdf, &phenopacket_id)
            .unwrap();
        let mut phenopackets = collector.phenopacket_builder.build();

        let mut expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            meta_data: Some(MetaData {
                resources: vec![hp_meta_data_resource],
                ..Default::default()
            }),
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
        assert_phenopackets(&mut phenopackets[0], &mut expected_p006);
    }

    #[rstest]
    fn test_collect_hpo_in_cells_col(
        pf_pneumonia: PhenotypicFeature,
        pf_asthma_no_onset: PhenotypicFeature,
        pf_nail_psoriasis: PhenotypicFeature,
        hp_meta_data_resource: Resource,
    ) {
        let mut collector = init_test_collector();

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
            .collect_hpo_in_cells_col(&phenopacket_id, &patient_hpo_col, stringified_onset_col)
            .unwrap();
        let mut phenopackets = collector.phenopacket_builder.build();

        let mut expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            meta_data: Some(MetaData {
                resources: vec![hp_meta_data_resource],
                ..Default::default()
            }),
            ..Default::default()
        };
        expected_p006.phenotypic_features.push(pf_pneumonia);
        expected_p006.phenotypic_features.push(pf_asthma_no_onset);
        expected_p006.phenotypic_features.push(pf_nail_psoriasis);

        assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_p006);
    }

    #[rstest]
    fn test_collect_hpo_in_header_col(
        pf_runny_nose_excluded: PhenotypicFeature,
        hp_meta_data_resource: Resource,
    ) {
        let mut collector = init_test_collector();

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
            .collect_hpo_in_header_col(&phenopacket_id, &patient_hpo_col, stringified_onset_col)
            .unwrap();
        let mut phenopackets = collector.phenopacket_builder.build();

        let mut expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            meta_data: Some(MetaData {
                resources: vec![hp_meta_data_resource],
                ..Default::default()
            }),
            ..Default::default()
        };
        expected_p006
            .phenotypic_features
            .push(pf_runny_nose_excluded);

        assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_p006);
    }

    #[rstest]
    fn test_collect_individual(tc: TableContext, df_single_patient: DataFrame) {
        let mut collector = init_test_collector();

        let patient_cdf = ContextualizedDataFrame::new(tc, df_single_patient);

        let phenopacket_id = "cohort2019-P006".to_string();
        let patient_id = "P006".to_string();

        collector
            .collect_individual(&patient_cdf, &phenopacket_id, &patient_id)
            .unwrap();

        let mut phenopackets = collector.phenopacket_builder.build();

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

        let mut expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            subject: Some(indiv),
            meta_data: Some(MetaData::default()),
            ..Default::default()
        };

        assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_p006);
    }

    #[rstest]
    fn test_collect_interpretations(
        tc: TableContext,
        df_single_patient: DataFrame,
        mondo_meta_data_resource: Resource,
    ) {
        let mut collector = init_test_collector();

        let patient_cdf = ContextualizedDataFrame::new(tc, df_single_patient);

        let phenopacket_id = "cohort2019-P006".to_string();

        collector
            .collect_interpretations(&patient_cdf, &phenopacket_id)
            .unwrap();

        let mut phenopackets = collector.phenopacket_builder.build();

        let platelet_defect_interpretation = Interpretation {
            id: "cohort2019-P006-MONDO:0008258".to_string(),
            progress_status: 4,
            diagnosis: Some(Diagnosis {
                disease: Some(OntologyClass {
                    id: "MONDO:0008258".to_string(),
                    label: "platelet signal processing defect".to_string(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let dysostosis_interpretation = Interpretation {
            id: "cohort2019-P006-MONDO:0000359".to_string(),
            progress_status: 4,
            diagnosis: Some(Diagnosis {
                disease: Some(OntologyClass {
                    id: "MONDO:0000359".to_string(),
                    label: "spondylocostal dysostosis".to_string(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        let mut expected_p006 = Phenopacket {
            id: "cohort2019-P006".to_string(),
            interpretations: vec![platelet_defect_interpretation, dysostosis_interpretation],
            meta_data: Some(MetaData {
                resources: vec![mondo_meta_data_resource],
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(phenopackets.len(), 1);
        assert_phenopackets(&mut phenopackets[0], &mut expected_p006);
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
    }
}
