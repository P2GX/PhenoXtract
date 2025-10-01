#![allow(clippy::too_many_arguments)]
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::CollectionError;
use chrono::{NaiveDate, TimeZone, Utc};
use log::warn;
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::simple::SimpleTerm;
use ontolius::term::{MinimalTerm, Synonymous};
use ontolius::{Identified, TermId};
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::time_element::Element::{Age, Timestamp};
use phenopackets::schema::v2::core::vital_status::Status;
use phenopackets::schema::v2::core::{
    Age as AgeStruct, Individual, OntologyClass, PhenotypicFeature, Sex, TimeElement, VitalStatus,
};
use prost_types::Timestamp as TimestampProtobuf;
use regex::Regex;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug)]
pub struct PhenopacketBuilder {
    subject_to_phenopacket: HashMap<String, Phenopacket>,
    hpo: Arc<FullCsrOntology>,
}

impl PhenopacketBuilder {
    pub fn new(hpo: Arc<FullCsrOntology>) -> PhenopacketBuilder {
        PhenopacketBuilder {
            subject_to_phenopacket: HashMap::default(),
            hpo,
        }
    }
    #[allow(dead_code)]
    pub fn build(&self) -> Vec<Phenopacket> {
        self.subject_to_phenopacket.values().cloned().collect()
    }
    #[allow(dead_code)]
    pub fn build_for_id(&self, #[allow(unused)] id: String) -> Result<Phenopacket, TransformError> {
        Ok(Phenopacket::default())
    }

    #[allow(dead_code)]
    pub fn upsert_individual(
        &mut self,
        phenopacket_id: &str,
        individual_id: &str,
        alternate_ids: Option<&[&str]>,
        date_of_birth: Option<&str>,
        time_at_last_encounter: Option<&str>,
        sex: Option<&str>,
        karyotypic_sex: Option<&str>,
        gender: Option<&str>,
        taxonomy: Option<&str>,
    ) -> Result<(), TransformError> {
        if alternate_ids.is_some() {
            warn!("alternate_ids - not implemented for individual yet");
        }
        if time_at_last_encounter.is_some() {
            warn!("time_at_last_encounter - not implemented for individual yet");
        }
        if karyotypic_sex.is_some() {
            warn!("karyotypic_sex - not implemented for individual yet");
        }
        if gender.is_some() {
            warn!("gender - not implemented for individual yet");
        }
        if taxonomy.is_some() {
            warn!("taxonomy - not implemented for individual yet");
        }

        let phenopacket = self.get_or_create_phenopacket(phenopacket_id);

        let individual = phenopacket.subject.get_or_insert(Individual::default());
        individual.id = individual_id.to_string();

        if let Some(date_of_birth) = date_of_birth {
            individual.date_of_birth = Some(Self::parse_timestamp(date_of_birth)?);
        }

        if let Some(sex) = sex {
            individual.sex = Sex::from_str_name(sex)
                .ok_or_else(|| CollectionError(format!("Could not parse {sex}")))?
                .into();
        }
        Ok(())
    }

    pub fn upsert_vital_status(
        &mut self,
        phenopacket_id: &str,
        status: &str,
        time_of_death: Option<&str>,
        cause_of_death: Option<&str>,
        survival_time_in_days: Option<u32>,
    ) -> Result<(), TransformError> {
        if cause_of_death.is_some() {
            warn!("cause_of_death - not implemented for vital_status yet");
        }

        let status = Status::from_str_name(status).ok_or({
            CollectionError(format!(
                "Could not interpret {status} as status for {phenopacket_id}"
            ))
        })? as i32;

        let time_of_death = match time_of_death {
            Some(tod_string) => Some(Self::parse_time_element(tod_string)?),
            None => None,
        };

        let survival_time_in_days = survival_time_in_days.unwrap_or(0);

        let phenopacket = self.get_or_create_phenopacket(phenopacket_id);
        let individual = phenopacket.subject.get_or_insert(Individual::default());

        individual.vital_status = Some(VitalStatus {
            status,
            time_of_death,
            cause_of_death: None,
            survival_time_in_days,
        });
        Ok(())
    }

    /// Upserts a phenotypic feature within a specific phenopacket.
    ///
    /// This function adds or updates a `PhenotypicFeature` for a given phenopacket,
    /// identified by `phenopacket_id`. If the phenopacket does not exist, it will be
    /// created. If a feature with the same `phenotype` ID already exists within the
    /// phenopacket, this function will update it (upsert).
    ///
    /// # Arguments
    ///
    /// * `phenopacket_id` - A `String` that uniquely identifies the target phenopacket.
    /// * `phenotype` - A string slice (`&str`) representing the ontology term or id for the
    ///   phenotype (e.g., `"HP:0000118" or "Phenotypic abnormality"`).
    /// * `description` - An optional free-text description of the feature.
    /// * `excluded` - An optional boolean indicating if the feature is explicitly absent.
    /// * `severity` - An optional `String` describing the severity of the phenotype.
    /// * `modifiers` - An optional `Vec<String>` of terms that modify the phenotype.
    /// * `on_set` - An optional `TimeElement` representing the onset time of the feature.
    /// * `resolution` - An optional `TimeElement` indicating when the feature resolved.
    /// * `evidence` - An optional `Evidence` struct providing support for the feature.
    ///
    /// # Errors
    ///
    /// This function will return an `Err` if the provided `phenotype` term cannot be
    /// resolved into a valid `HpoTerm`.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful addition or update of the phenotypic feature.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Assuming `handler` is a mutable instance of the struct containing this method.
    /// let phenopacket_id = "patient-1".to_string();
    /// let phenotype_term = "HP:0000118"; // Corresponds to "Phenotypic abnormality"
    ///
    /// match handler.upsert_phenotypic_feature(
    ///     phenopacket_id,
    ///     phenotype_term,
    ///     None, None, None, None, None, None, None
    /// ) {
    ///     Ok(()) => println!("Successfully upserted the phenotypic feature."),
    ///     Err(e) => eprintln!("Error upserting feature: {}", e),
    /// }
    /// ```
    #[allow(dead_code)]
    pub fn upsert_phenotypic_feature(
        &mut self,
        phenopacket_id: &str,
        phenotype: &str,
        description: Option<&str>,
        excluded: Option<bool>,
        severity: Option<&str>,
        modifiers: Option<Vec<&str>>,
        onset: Option<&str>,
        resolution: Option<&str>,
        evidence: Option<&str>,
    ) -> Result<(), TransformError> {
        if excluded.is_some() {
            warn!("is_observed phenotypic feature not implemented yet");
        }
        if severity.is_some() {
            warn!("severity phenotypic feature not implemented yet");
        }
        if modifiers.is_some() {
            warn!("modifiers phenotypic feature not implemented yet");
        }
        if onset.is_some() {
            warn!("onset phenotypic feature is not fully implemented yet");
        }
        if resolution.is_some() {
            warn!("resolution phenotypic feature not implemented yet");
        }
        if evidence.is_some() {
            warn!("evidence phenotypic feature not implemented yet");
        }

        let term = self.raw_to_full_term(phenotype)?;
        let phenopacket = self.get_or_create_phenopacket(phenopacket_id);

        let feature = if let Some(pos) =
            phenopacket.phenotypic_features.iter().position(|feature| {
                if let Some(t) = &feature.r#type {
                    t.id == term.identifier().to_string()
                } else {
                    false
                }
            }) {
            &mut phenopacket.phenotypic_features[pos]
        } else {
            let new_feature = PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: term.identifier().to_string(),
                    label: term.name().to_string(),
                }),
                ..Default::default()
            };
            phenopacket.phenotypic_features.push(new_feature);
            phenopacket.phenotypic_features.last_mut().unwrap()
        };

        if let Some(desc) = description {
            feature.description = desc.to_string();
        }

        if let Some(onset) = onset {
            let onset_te = Self::parse_time_element(onset)?;
            feature.onset = Some(onset_te);
        }

        Ok(())
    }

    // TODO: Add test after MVP
    fn get_or_create_phenopacket(&mut self, phenopacket_id: &str) -> &mut Phenopacket {
        self.subject_to_phenopacket
            .entry(phenopacket_id.to_string())
            .or_insert_with(|| Phenopacket {
                id: phenopacket_id.to_string(),
                ..Default::default()
            })
    }
    // TODO: Add test after MVP
    fn raw_to_full_term(&self, raw_term: &str) -> Result<SimpleTerm, TransformError> {
        let term = TermId::from_str(raw_term)
            .ok()
            .and_then(|term_id| self.hpo.as_ref().term_by_id(&term_id))
            .or_else(|| {
                self.hpo.as_ref().iter_terms().find(|term| {
                    term.is_current()
                        && (term.name().to_lowercase() == raw_term.to_lowercase().trim()
                            || term.synonyms().iter().any(|syn| {
                                syn.name.to_lowercase() == raw_term.to_lowercase().trim()
                            }))
                })
            });
        if term.is_none() {
            return Err(CollectionError(format!(
                "Could not find ontology class for {raw_term}"
            )));
        }
        let term = term.unwrap();
        if term.is_obsolete() {
            return Err(CollectionError(format!(
                "Could only find obsolete term for: {raw_term}"
            )));
        }
        Ok(term.clone())
    }

    fn parse_time_element(te_string: &str) -> Result<TimeElement, TransformError> {
        //try to parse the string as a datetime
        if let Ok(ts) = Self::parse_timestamp(te_string) {
            let datetime_te = TimeElement {
                element: Some(Timestamp(ts)),
            };
            return Ok(datetime_te);
        }

        //if that fails, try to parse the string as a duration
        let iso8601_dur_pattern = r"^P(\d+Y)?(\d+M)?(\d+D)?(T(\d+H)?(\d+M)?(\d+S)?)?$";
        let re = Regex::new(iso8601_dur_pattern).unwrap();
        let is_iso8601_dur = re.is_match(te_string);
        if is_iso8601_dur {
            let age_te = TimeElement {
                element: Some(Age(AgeStruct {
                    iso8601duration: te_string.to_string(),
                })),
            };
            return Ok(age_te);
        }

        //if it could not be parsed return an error
        Err(CollectionError(format!(
            "Could not parse {te_string} as a TimeElement."
        )))
    }

    fn parse_timestamp(ts_string: &str) -> Result<TimestampProtobuf, TransformError> {
        //this will allow either full datetimes e.g. 2005-10-01T12:34:56Z or dates e.g. 2005-10-01
        let dt = ts_string
            .parse::<chrono::DateTime<Utc>>()
            .or_else(|_| {
                NaiveDate::parse_from_str(ts_string, "%Y-%m-%d")
                    .map(|date| Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap()))
            })
            .map_err(|_| {
                CollectionError(format!(
                    "Could not parse {ts_string} as a Protobuf Timestamp."
                ))
            })?;

        let seconds = dt.timestamp();
        let nanos = dt.timestamp_subsec_nanos() as i32;
        Ok(TimestampProtobuf { seconds, nanos })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HPO;
    use phenopackets::schema::v2::core::Age as age_struct;
    use phenopackets::schema::v2::core::time_element::Element::Age;
    use rstest::*;

    #[fixture]
    fn phenopacket_id() -> String {
        "cohort_patient_001".to_string()
    }

    #[fixture]
    fn valid_phenotype() -> String {
        "HP:0001166".to_string()
    }

    #[fixture]
    fn another_phenotype() -> String {
        "Microcephaly".to_string()
    }

    #[fixture]
    fn onset_age() -> Option<&'static str> {
        Some("P48Y4M21D")
    }

    #[fixture]
    fn onset_age_te() -> Option<TimeElement> {
        Some(TimeElement {
            element: Some(Age(age_struct {
                iso8601duration: "P48Y4M21D".to_string(),
            })),
        })
    }

    #[fixture]
    fn onset_timestamp() -> Option<&'static str> {
        Some("2005-10-01T12:34:56Z")
    }

    #[fixture]
    fn onset_timestamp_te() -> Option<TimeElement> {
        Some(TimeElement {
            element: Some(Timestamp(TimestampProtobuf {
                seconds: 1128170096,
                nanos: 0,
            })),
        })
    }

    #[fixture]
    fn another_phenotype() -> String {
        "Microcephaly".to_string()
    }

    #[fixture]
    fn tmp_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    fn construct_builder(tmp_dir: TempDir) -> PhenopacketBuilder {
        let hpo_registry = GithubOntologyRegistry::default_hpo_registry()
            .unwrap()
            .with_registry_path(tmp_dir.path().into());
        let path = hpo_registry.register("latest").unwrap();

        PhenopacketBuilder::new(init_ontolius(path).unwrap())
    }

    #[rstest]
    fn test_upsert_phenotypic_feature_success(
        phenopacket_id: String,
        valid_phenotype: String,
        onset_age: Option<&str>,
        onset_age_te: Option<TimeElement>,
    ) {
        let mut builder = PhenopacketBuilder::new(HPO.clone());
        let result = builder.upsert_phenotypic_feature(
            phenopacket_id.as_str(),
            &valid_phenotype,
            None,
            None,
            None,
            None,
            onset_age,
            None,
            None,
        );

        assert!(result.is_ok());

        assert!(builder.subject_to_phenopacket.contains_key(&phenopacket_id));

        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 1);

        let feature = &phenopacket.phenotypic_features[0];
        assert!(feature.r#type.is_some());

        let ontology_class = feature.r#type.as_ref().unwrap();
        assert_eq!(ontology_class.id, "HP:0001166");
        assert_eq!(ontology_class.label, "Arachnodactyly");

        assert!(feature.onset.is_some());
        let feature_onset = feature.onset.as_ref().unwrap();
        assert_eq!(feature_onset, &onset_age_te.unwrap());
    }

    #[rstest]
    fn test_upsert_phenotypic_feature_invalid_term(phenopacket_id: String) {
        let mut builder = PhenopacketBuilder::new(HPO.clone());

        let result = builder.upsert_phenotypic_feature(
            phenopacket_id.as_str(),
            "invalid_term",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        assert!(result.is_err());
    }

    #[rstest]
    fn test_multiple_phenotypic_features_same_phenopacket(
        phenopacket_id: String,
        valid_phenotype: String,
        another_phenotype: String,
    ) {
        let mut builder = PhenopacketBuilder::new(HPO.clone());

        let result1 = builder.upsert_phenotypic_feature(
            phenopacket_id.as_str(),
            &valid_phenotype,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result1.is_ok());

        let result2 = builder.upsert_phenotypic_feature(
            phenopacket_id.as_str(),
            &another_phenotype,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result2.is_ok());

        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 2);
    }

    #[rstest]
    fn test_different_phenopacket_ids(valid_phenotype: String) {
        let mut builder = PhenopacketBuilder::new(HPO.clone());

        let id1 = "pp_001".to_string();
        let id2 = "pp_002".to_string();

        let result1 = builder.upsert_phenotypic_feature(
            id1.as_str(),
            &valid_phenotype,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result1.is_ok());

        let result2 = builder.upsert_phenotypic_feature(
            id2.as_str(),
            &valid_phenotype,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result2.is_ok());

        assert!(builder.subject_to_phenopacket.contains_key(&id1));
        assert!(builder.subject_to_phenopacket.contains_key(&id2));
        assert_eq!(builder.subject_to_phenopacket.len(), 2);
    }

    #[rstest]
    fn test_update_phenotypic_features(phenopacket_id: String, valid_phenotype: String) {
        let mut builder = PhenopacketBuilder::new(HPO.clone());

        let existing_phenopacket = Phenopacket {
            id: phenopacket_id.clone(),
            subject: None,
            phenotypic_features: vec![PhenotypicFeature {
                description: "".to_string(),
                r#type: Some(OntologyClass {
                    id: "HP:0000001".to_string(),
                    label: "All".to_string(),
                }),
                excluded: false,
                severity: None,
                modifiers: vec![],
                onset: None,
                resolution: None,
                evidence: vec![],
            }],
            measurements: vec![],
            biosamples: vec![],
            interpretations: vec![],
            diseases: vec![],
            medical_actions: vec![],
            files: vec![],
            meta_data: None,
        };
        builder
            .subject_to_phenopacket
            .insert(phenopacket_id.clone(), existing_phenopacket);

        // Add another feature
        let result = builder.upsert_phenotypic_feature(
            phenopacket_id.as_str(),
            &valid_phenotype,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        assert!(result.is_ok());

        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 2);
    }

    #[rstest]
    fn test_update_onset_of_phenotypic_feature(
        phenopacket_id: String,
        onset_age: Option<&str>,
        onset_timestamp: Option<&str>,
        onset_timestamp_te: Option<TimeElement>,
        valid_phenotype: String,
    ) {
        let mut builder = PhenopacketBuilder::new(HPO.clone());

        // Add a feature
        builder
            .upsert_phenotypic_feature(
                phenopacket_id.as_str(),
                &valid_phenotype,
                None,
                None,
                None,
                None,
                onset_age,
                None,
                None,
            )
            .unwrap();

        // Update the same feature
        let result = builder.upsert_phenotypic_feature(
            phenopacket_id.as_str(),
            &valid_phenotype,
            None,
            None,
            None,
            None,
            onset_timestamp,
            None,
            None,
        );

        assert!(result.is_ok());

        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 1);

        let feature = &phenopacket.phenotypic_features[0];
        assert!(feature.r#type.is_some());

        assert!(feature.onset.is_some());
        let feature_onset = feature.onset.as_ref().unwrap();
        assert_eq!(feature_onset, &onset_timestamp_te.unwrap());
    }

    #[rstest]
    fn test_upsert_individual() {
        let mut builder = PhenopacketBuilder::new(HPO.clone());

        let phenopacket_id = "pp_001";
        let individual_id = "individual_001";

        // Test just upserting the individual id
        let result = builder.upsert_individual(
            phenopacket_id,
            individual_id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());

        let phenopacket = builder.subject_to_phenopacket.get(phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();
        assert_eq!(individual.id, individual_id);
        assert_eq!(individual.sex, 0);
        assert_eq!(individual.vital_status, None);

        // Test upserting the other entries
        let result = builder.upsert_individual(
            phenopacket_id,
            individual_id,
            None,
            Some("2001-01-29"),
            None,
            Some("MALE"),
            None,
            None,
            None,
        );
        assert!(result.is_ok());

        let phenopacket = builder.subject_to_phenopacket.get(phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();

        assert_eq!(individual.sex, Sex::Male as i32);
        assert_eq!(
            individual.date_of_birth,
            Some(TimestampProtobuf {
                seconds: 980726400,
                nanos: 0,
            })
        );
    }

    #[rstest]
    fn test_upsert_vital_status(tmp_dir: TempDir) {
        skip_in_ci!();

        let mut builder = construct_builder(tmp_dir);

        let phenopacket_id = "pp_001";

        let result = builder.upsert_vital_status(
            phenopacket_id,
            "ALIVE",
            Some("P81Y5M13D"),
            None,
            Some(322),
        );
        assert!(result.is_ok());

        let phenopacket = builder.subject_to_phenopacket.get(phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();

        assert_eq!(
            individual.vital_status,
            Some(VitalStatus {
                status: 1,
                time_of_death: Some(TimeElement {
                    element: Some(Age(AgeStruct {
                        iso8601duration: "P81Y5M13D".to_string()
                    }))
                }),
                cause_of_death: None,
                survival_time_in_days: 322,
            })
        );
    }

    #[rstest]
    fn test_parse_time_element_duration() {
        let te = PhenopacketBuilder::parse_time_element("P81Y5M13D").unwrap();
        assert_eq!(
            te,
            TimeElement {
                element: Some(Age(AgeStruct {
                    iso8601duration: "P81Y5M13D".to_string()
                }))
            }
        );
    }

    #[rstest]
    fn test_parse_time_element_datetime() {
        let te_date = PhenopacketBuilder::parse_time_element("2001-01-29").unwrap();
        assert_eq!(
            te_date,
            TimeElement {
                element: Some(Timestamp(TimestampProtobuf {
                    seconds: 980726400,
                    nanos: 0,
                })),
            }
        );
        let te_datetime = PhenopacketBuilder::parse_time_element("2015-06-05T09:17:39Z").unwrap();
        assert_eq!(
            te_datetime,
            TimeElement {
                element: Some(Timestamp(TimestampProtobuf {
                    seconds: 1433495859,
                    nanos: 0,
                })),
            }
        );
    }

    #[rstest]
    fn test_parse_time_element_invalid() {
        let result = PhenopacketBuilder::parse_time_element("P81D5M13Y");
        assert!(result.is_err());
        let result = PhenopacketBuilder::parse_time_element("8D5M13Y");
        assert!(result.is_err());
        let result = PhenopacketBuilder::parse_time_element("09:17:39Z");
        assert!(result.is_err());
        let result = PhenopacketBuilder::parse_time_element("2020-20-15T09:17:39Z");
        assert!(result.is_err());
    }

    #[rstest]
    fn test_parse_timestamp() {
        let ts_date = PhenopacketBuilder::parse_timestamp("2001-01-29").unwrap();
        assert_eq!(
            ts_date,
            TimestampProtobuf {
                seconds: 980726400,
                nanos: 0,
            }
        );
        let ts_datetime = PhenopacketBuilder::parse_timestamp("2015-06-05T09:17:39Z").unwrap();
        assert_eq!(
            ts_datetime,
            TimestampProtobuf {
                seconds: 1433495859,
                nanos: 0,
            }
        );
        let result = PhenopacketBuilder::parse_timestamp("09:17:39Z");
        assert!(result.is_err());
        let result = PhenopacketBuilder::parse_timestamp("2020-20-15T09:17:39Z");
        assert!(result.is_err());
    }

    #[rstest]
    fn test_get_or_create_phenopacket() {
        let mut builder = PhenopacketBuilder::new(HPO.clone());
        let phenopacket_id = "pp_001";
        builder.get_or_create_phenopacket(phenopacket_id);
        let pp = builder.get_or_create_phenopacket(phenopacket_id);
        assert_eq!(pp.id, phenopacket_id);
        assert_eq!(builder.subject_to_phenopacket.len(), 1);
    }
}
