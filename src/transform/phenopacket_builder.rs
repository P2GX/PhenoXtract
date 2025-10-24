#![allow(clippy::too_many_arguments)]
use crate::constants::ISO8601_DUR_PATTERN;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::traits::{HasPrefixId, HasVersion};
use crate::ontology::{HGNCClient, OntologyRef};
use crate::transform::cached_resource_resolver::CachedResourceResolver;
use crate::transform::error::PhenopacketBuilderError;
use crate::transform::variant_syntax_parser::VariantParser;
use crate::utils::{try_parse_string_date, try_parse_string_datetime};
use chrono::{TimeZone, Utc};
use log::warn;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::time_element::Element::{Age, Timestamp};
use phenopackets::schema::v2::core::vital_status::Status;
use phenopackets::schema::v2::core::{
    Age as IndividualAge, Individual, OntologyClass, PhenotypicFeature, Sex, TimeElement,
    VitalStatus,
};
use prost_types::Timestamp as TimestampProtobuf;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct PhenopacketBuilder {
    subject_to_phenopacket: HashMap<String, Phenopacket>,
    ontology_bidicts: HashMap<String, Arc<OntologyBiDict>>,
    hgnc_client: HGNCClient,
    resource_resolver: CachedResourceResolver,
    variant_parser: VariantParser,
}

impl PhenopacketBuilder {
    pub fn new(ontology_bidicts: HashMap<String, Arc<OntologyBiDict>>) -> PhenopacketBuilder {
        PhenopacketBuilder {
            ontology_bidicts,
            ..Default::default()
        }
    }

    pub fn build(&self) -> Vec<Phenopacket> {
        let mut phenopackets: Vec<Phenopacket> =
            self.subject_to_phenopacket.values().cloned().collect();
        let now = Utc::now().to_string();

        phenopackets.iter_mut().for_each(|pp| {
            if let Some(metadata) = pp.meta_data.as_mut() {
                metadata.created = Some(
                    Self::try_parse_timestamp(&now)
                        .expect("Failed to parse current timestamp for phenopacket metadata"),
                )
            }
        });

        phenopackets
    }
    #[allow(dead_code)]
    pub fn build_for_id(&self, phenopacket_id: String) -> Option<Phenopacket> {
        self.subject_to_phenopacket.get(&phenopacket_id).cloned()
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
    ) -> Result<(), PhenopacketBuilderError> {
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
            individual.date_of_birth = Some(Self::try_parse_timestamp(date_of_birth)?);
        }

        if let Some(sex) = sex {
            individual.sex = Sex::from_str_name(sex)
                .ok_or_else(|| PhenopacketBuilderError::ParsingError {
                    what: "Sex".to_string(),
                    value: sex.to_string(),
                })?
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
    ) -> Result<(), PhenopacketBuilderError> {
        if cause_of_death.is_some() {
            warn!("cause_of_death - not implemented for vital_status yet");
        }

        let status = Status::from_str_name(status).ok_or(PhenopacketBuilderError::ParsingError {
            what: "vital status".to_string(),
            value: status.to_string(),
        })? as i32;

        let time_of_death = match time_of_death {
            Some(tod_string) => Some(Self::try_parse_time_element(tod_string)?),
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
    /// * `phenotype` - A string slice (`&str`) representing the ontology label or id for the
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
    ) -> Result<(), PhenopacketBuilderError> {
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

        let term = self.query_hpo_identifiers(phenotype)?;

        let phenopacket = self.get_or_create_phenopacket(phenopacket_id);
        let feature = if let Some(pos) =
            phenopacket.phenotypic_features.iter().position(|feature| {
                if let Some(t) = &feature.r#type {
                    t.id == term.id
                } else {
                    false
                }
            }) {
            &mut phenopacket.phenotypic_features[pos]
        } else {
            let new_feature = PhenotypicFeature {
                r#type: Some(term),
                ..Default::default()
            };
            phenopacket.phenotypic_features.push(new_feature);
            phenopacket.phenotypic_features.last_mut().unwrap()
        };

        if let Some(desc) = description {
            feature.description = desc.to_string();
        }

        if let Some(excluded) = excluded {
            feature.excluded = excluded;
        }

        if let Some(onset) = onset {
            let onset_te = Self::try_parse_time_element(onset)?;
            feature.onset = Some(onset_te);
        }
        self.ensure_resource(
            phenopacket_id,
            &self
                .ontology_bidicts
                .get(OntologyRef::HPO_PREFIX)
                .unwrap()
                .ontology
                .clone(),
        );
        Ok(())
    }

    fn get_or_create_phenopacket(&mut self, phenopacket_id: &str) -> &mut Phenopacket {
        self.subject_to_phenopacket
            .entry(phenopacket_id.to_string())
            .or_insert_with(|| Phenopacket {
                id: phenopacket_id.to_string(),
                ..Default::default()
            })
    }
    fn query_hpo_identifiers(
        &self,
        hpo_query: &str,
    ) -> Result<OntologyClass, PhenopacketBuilderError> {
        let hpo_dict = self
            .ontology_bidicts
            .get(OntologyRef::HPO_PREFIX)
            .expect(&format!(
                "No bidirectional ontology for {} in PhenopacketBuilder",
                OntologyRef::HPO_PREFIX
            ));
        hpo_dict
            .get(hpo_query)
            .ok_or_else(|| PhenopacketBuilderError::ParsingError {
                what: "hpo query".to_string(),
                value: hpo_query.to_string(),
            })
            .map(|found| {
                let corresponding_label_or_id = hpo_dict
                    .get(found)
                    .unwrap_or_else(|| panic!("Could not find hpo label or id from {}", found));
                let (label, id) = if hpo_dict.is_primary_label(found) {
                    (found.to_string(), corresponding_label_or_id.to_string())
                } else {
                    (corresponding_label_or_id.to_string(), found.to_string())
                };
                Ok(OntologyClass { id, label })
            })?
    }

    fn try_parse_time_element(te_string: &str) -> Result<TimeElement, PhenopacketBuilderError> {
        //try to parse the string as a datetime
        if let Ok(ts) = Self::try_parse_timestamp(te_string) {
            let datetime_te = TimeElement {
                element: Some(Timestamp(ts)),
            };
            return Ok(datetime_te);
        }

        let re = Regex::new(ISO8601_DUR_PATTERN).unwrap();
        let is_iso8601_dur = re.is_match(te_string);
        if is_iso8601_dur {
            let age_te = TimeElement {
                element: Some(Age(IndividualAge {
                    iso8601duration: te_string.to_string(),
                })),
            };
            return Ok(age_te);
        }

        Err(PhenopacketBuilderError::ParsingError {
            what: "TimeElement".to_string(),
            value: te_string.to_string(),
        })
    }

    fn try_parse_timestamp(ts_string: &str) -> Result<TimestampProtobuf, PhenopacketBuilderError> {
        let utc_dt = try_parse_string_datetime(ts_string)
            .or_else(|| try_parse_string_date(ts_string).and_then(|date| date.and_hms_opt(0, 0, 0)))
            .map(|naive| Utc.from_utc_datetime(&naive))
            .ok_or_else(|| PhenopacketBuilderError::ParsingError {
                what: "Timestamp".to_string(),
                value: ts_string.to_string(),
            })?;

        let seconds = utc_dt.timestamp();
        let nanos = utc_dt.timestamp_subsec_nanos() as i32;
        Ok(TimestampProtobuf { seconds, nanos })
    }

    fn ensure_resource(
        &mut self,
        phenopacket_id: &str,
        resource_id: &(impl HasPrefixId + HasVersion),
    ) {
        let needs_resource = self
            .get_or_create_phenopacket(phenopacket_id)
            .meta_data
            .as_ref()
            .map(|meta_data| {
                !meta_data.resources.iter().any(|resource| {
                    resource.id.to_lowercase() == resource_id.prefix_id().to_lowercase()
                        && resource.version.to_lowercase() == resource.version.to_lowercase()
                })
            })
            .unwrap_or(true);

        if needs_resource {
            let resource = self.resource_resolver.resolve(resource_id).unwrap();

            let phenopacket = self.get_or_create_phenopacket(phenopacket_id);
            phenopacket
                .meta_data
                .get_or_insert_with(Default::default)
                .resources
                .push(resource);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ontology::resource_references::ResourceRef;
    use crate::test_utils::{GENO_REF, HPO_REF, MONDO_REF, ONTOLOGY_FACTORY};
    use phenopackets::schema::v2::core::time_element::Element::Age;
    use phenopackets::schema::v2::core::{Age as age_struct, Resource};
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

    fn build_dicts() -> HashMap<String, Arc<OntologyBiDict>> {
        let hpo_dict = ONTOLOGY_FACTORY
            .lock()
            .unwrap()
            .build_bidict(&HPO_REF.clone(), None)
            .unwrap();
        let mondo_dict = ONTOLOGY_FACTORY
            .lock()
            .unwrap()
            .build_bidict(&MONDO_REF.clone(), None)
            .unwrap();
        let geno_dict = ONTOLOGY_FACTORY
            .lock()
            .unwrap()
            .build_bidict(&GENO_REF.clone(), None)
            .unwrap();

        HashMap::from_iter(vec![
            (hpo_dict.ontology.prefix_id().to_string(), hpo_dict),
            (mondo_dict.ontology.prefix_id().to_string(), mondo_dict),
            (geno_dict.ontology.prefix_id().to_string(), geno_dict),
        ])
    }
    fn build_phenopacket_builder() -> PhenopacketBuilder {
        PhenopacketBuilder::new(build_dicts())
    }
    #[rstest]
    fn test_upsert_phenotypic_feature_success(
        phenopacket_id: String,
        valid_phenotype: String,
        onset_age: Option<&str>,
        onset_age_te: Option<TimeElement>,
    ) {
        let mut builder = build_phenopacket_builder();
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
        let mut builder = build_phenopacket_builder();

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
        let mut builder = build_phenopacket_builder();

        builder
            .upsert_phenotypic_feature(
                phenopacket_id.as_str(),
                &valid_phenotype,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        builder
            .upsert_phenotypic_feature(
                phenopacket_id.as_str(),
                &another_phenotype,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 2);
    }

    #[rstest]
    fn test_different_phenopacket_ids(valid_phenotype: String) {
        let mut builder = build_phenopacket_builder();

        let id1 = "pp_001".to_string();
        let id2 = "pp_002".to_string();

        builder
            .upsert_phenotypic_feature(
                id1.as_str(),
                &valid_phenotype,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        builder
            .upsert_phenotypic_feature(
                id2.as_str(),
                &valid_phenotype,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        assert!(builder.subject_to_phenopacket.contains_key(&id1));
        assert!(builder.subject_to_phenopacket.contains_key(&id2));
        assert_eq!(builder.subject_to_phenopacket.len(), 2);
    }

    #[rstest]
    fn test_update_phenotypic_features(phenopacket_id: String, valid_phenotype: String) {
        let mut builder = build_phenopacket_builder();

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
        builder
            .upsert_phenotypic_feature(
                phenopacket_id.as_str(),
                &valid_phenotype,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

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
        let mut builder = build_phenopacket_builder();

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
        builder
            .upsert_phenotypic_feature(
                phenopacket_id.as_str(),
                &valid_phenotype,
                None,
                None,
                None,
                None,
                onset_timestamp,
                None,
                None,
            )
            .unwrap();

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
        let mut builder = build_phenopacket_builder();

        let phenopacket_id = "pp_001";
        let individual_id = "individual_001";

        // Test just upserting the individual id
        builder
            .upsert_individual(
                phenopacket_id,
                individual_id,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();
        assert_eq!(individual.id, individual_id);
        assert_eq!(individual.sex, 0);
        assert_eq!(individual.vital_status, None);

        // Test upserting the other entries
        builder
            .upsert_individual(
                phenopacket_id,
                individual_id,
                None,
                Some("2001-01-29"),
                None,
                Some("MALE"),
                None,
                None,
                None,
            )
            .unwrap();

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
    fn test_upsert_vital_status() {
        let mut builder = build_phenopacket_builder();

        let phenopacket_id = "pp_001";

        builder
            .upsert_vital_status(phenopacket_id, "ALIVE", Some("P81Y5M13D"), None, Some(322))
            .unwrap();

        let phenopacket = builder.subject_to_phenopacket.get(phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();

        assert_eq!(
            individual.vital_status,
            Some(VitalStatus {
                status: 1,
                time_of_death: Some(TimeElement {
                    element: Some(Age(IndividualAge {
                        iso8601duration: "P81Y5M13D".to_string()
                    }))
                }),
                cause_of_death: None,
                survival_time_in_days: 322,
            })
        );
    }

    #[rstest]
    fn test_query_hpo_identifiers_with_valid_label() {
        let builder = build_phenopacket_builder();

        // Known HPO label from test_utils::HPO_DICT: "Seizure" <-> "HP:0001250"
        let result = builder.query_hpo_identifiers("Seizure").unwrap();

        assert_eq!(result.label, "Seizure");
        assert_eq!(result.id, "HP:0001250");
    }

    #[rstest]
    fn test_query_hpo_identifiers_with_valid_id() {
        let builder = build_phenopacket_builder();

        let result = builder.query_hpo_identifiers("HP:0001250").unwrap();

        assert_eq!(result.label, "Seizure");
        assert_eq!(result.id, "HP:0001250");
    }

    #[rstest]
    fn test_query_hpo_identifiers_invalid_query() {
        let builder = build_phenopacket_builder();

        let result = builder.query_hpo_identifiers("NonexistentTerm");

        assert!(result.is_err());
    }

    #[rstest]
    fn test_parse_time_element_duration() {
        let te = PhenopacketBuilder::try_parse_time_element("P81Y5M13D").unwrap();
        assert_eq!(
            te,
            TimeElement {
                element: Some(Age(IndividualAge {
                    iso8601duration: "P81Y5M13D".to_string()
                }))
            }
        );
    }

    #[rstest]
    fn test_parse_time_element_datetime() {
        let te_date = PhenopacketBuilder::try_parse_time_element("2001-01-29").unwrap();
        assert_eq!(
            te_date,
            TimeElement {
                element: Some(Timestamp(TimestampProtobuf {
                    seconds: 980726400,
                    nanos: 0,
                })),
            }
        );
        let te_datetime =
            PhenopacketBuilder::try_parse_time_element("2015-06-05T09:17:39Z").unwrap();
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
    #[case("P81D5M13Y")]
    #[case("8D5M13Y")]
    #[case("09:17:39Z")]
    #[case("2020-20-15T09:17:39Z")]
    fn test_parse_time_element_invalid(#[case] date_str: &str) {
        let result = PhenopacketBuilder::try_parse_time_element(date_str);
        assert!(result.is_err());
    }

    #[rstest]
    fn test_parse_timestamp() {
        let ts_date = PhenopacketBuilder::try_parse_timestamp("2001-01-29").unwrap();
        assert_eq!(
            ts_date,
            TimestampProtobuf {
                seconds: 980726400,
                nanos: 0,
            }
        );
        let ts_datetime = PhenopacketBuilder::try_parse_timestamp("2015-06-05T09:17:39Z").unwrap();
        assert_eq!(
            ts_datetime,
            TimestampProtobuf {
                seconds: 1433495859,
                nanos: 0,
            }
        );
        let result = PhenopacketBuilder::try_parse_timestamp("09:17:39Z");
        assert!(result.is_err());
        let result = PhenopacketBuilder::try_parse_timestamp("2020-20-15T09:17:39Z");
        assert!(result.is_err());
    }

    #[rstest]
    fn test_get_or_create_phenopacket() {
        let mut builder = build_phenopacket_builder();
        let phenopacket_id = "pp_001";
        builder.get_or_create_phenopacket(phenopacket_id);
        let pp = builder.get_or_create_phenopacket(phenopacket_id);
        assert_eq!(pp.id, phenopacket_id);
        assert_eq!(builder.subject_to_phenopacket.len(), 1);
    }

    #[rstest]
    fn test_ensure_resource() {
        let mut builder = build_phenopacket_builder();
        let pp_id = "test_id".to_string();

        builder.ensure_resource(
            &pp_id,
            &ResourceRef::new("omim".to_string(), "".to_string()),
        );

        let pp = builder.build().first().unwrap().clone();
        let omim_resrouce = pp.meta_data.as_ref().unwrap().resources.first().unwrap();

        let expected_resource = Resource {
            id: "omim".to_string(),
            name: "Online Mendelian Inheritance in Man".to_string(),
            url: "https://omim.org/".to_string(),
            version: "-".to_string(),
            namespace_prefix: "omim".to_string(),
            iri_prefix: "https://omim.org/MIM:$1".to_string(),
        };
        assert_eq!(omim_resrouce, &expected_resource);
    }
}
