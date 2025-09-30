#![allow(clippy::too_many_arguments)]
use crate::transform::error::TransformError;
use anyhow::anyhow;
use log::warn;
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::simple::SimpleTerm;
use ontolius::term::{MinimalTerm, Synonymous};
use ontolius::{Identified, TermId};
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::Evidence;
use phenopackets::schema::v2::core::{
    Individual, OntologyClass, PhenotypicFeature, Sex, TimeElement, VitalStatus,
};
use std::collections::HashMap;
use std::rc::Rc;
use std::str::FromStr;

#[allow(dead_code)]
#[derive(Debug)]
pub struct PhenopacketBuilder {
    subject_to_phenopacket: HashMap<String, Phenopacket>,
    hpo: Rc<FullCsrOntology>,
}

impl PhenopacketBuilder {
    pub fn new(hpo: Rc<FullCsrOntology>) -> PhenopacketBuilder {
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
        time_at_last_encounter: Option<TimeElement>,
        vital_status: Option<VitalStatus>,
        sex: Option<&str>,
        karyotypic_sex: Option<&str>,
        gender: Option<&str>,
        taxonomy: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        if alternate_ids.is_some() {
            warn!("alternate_ids - not implemented for individual yet");
        }
        if date_of_birth.is_some() {
            warn!("date_of_birth - not implemented for individual yet");
        }
        if time_at_last_encounter.is_some() {
            warn!("time_at_last_encounter - not implemented for individual yet");
        }
        if vital_status.is_some() {
            warn!("vital_status - not fully implemented for individual yet");
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

        if let Some(vs) = vital_status {
            individual.vital_status = Some(vs);
        }

        if let Some(sex) = sex {
            individual.sex = Sex::from_str_name(sex)
                .ok_or_else(|| anyhow!("Could not parse {sex}"))?
                .into();
        }
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
        onset: Option<TimeElement>,
        resolution: Option<TimeElement>,
        evidence: Option<Evidence>,
    ) -> Result<(), anyhow::Error> {
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
            feature.onset = Some(onset);
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
    fn raw_to_full_term(&self, raw_term: &str) -> Result<SimpleTerm, anyhow::Error> {
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
            return Err(anyhow!("Could not find ontology class for {raw_term}"));
        }
        let term = term.unwrap();
        if term.is_obsolete() {
            return Err(anyhow!("Could only find obsolete term for: {raw_term}"));
        }
        Ok(term.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
    use crate::ontology::traits::OntologyRegistry;
    use crate::ontology::utils::init_ontolius;
    use crate::skip_in_ci;
    use phenopackets::schema::v1::core::Sex::Male;
    use phenopackets::schema::v2::core::Age as age_struct;
    use phenopackets::schema::v2::core::time_element::Element::Age;
    use rstest::*;
    use tempfile::TempDir;

    #[fixture]
    fn phenopacket_id() -> String {
        "cohort_patient_001".to_string()
    }

    #[fixture]
    fn valid_phenotype() -> String {
        "HP:0001166".to_string()
    }

    #[fixture]
    fn onset_te() -> Option<TimeElement> {
        Some(TimeElement {
            element: Some(Age(age_struct {
                iso8601duration: "P48Y4M21D".to_string(),
            })),
        })
    }

    #[fixture]
    fn onset_te_alt() -> Option<TimeElement> {
        Some(TimeElement {
            element: Some(Age(age_struct {
                iso8601duration: "P12Y5M028D".to_string(),
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
        onset_te: Option<TimeElement>,
        tmp_dir: TempDir,
    ) {
        skip_in_ci!();

        let mut builder = construct_builder(tmp_dir);
        let result = builder.upsert_phenotypic_feature(
            phenopacket_id.as_str(),
            &valid_phenotype,
            None,
            None,
            None,
            None,
            onset_te.clone(),
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
        assert_eq!(feature_onset, &onset_te.unwrap());
    }

    #[rstest]
    fn test_upsert_phenotypic_feature_invalid_term(tmp_dir: TempDir, phenopacket_id: String) {
        skip_in_ci!();

        let mut builder = construct_builder(tmp_dir);

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
        tmp_dir: TempDir,
        phenopacket_id: String,
        valid_phenotype: String,
        another_phenotype: String,
    ) {
        skip_in_ci!();

        let mut builder = construct_builder(tmp_dir);

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
    fn test_different_phenopacket_ids(valid_phenotype: String, tmp_dir: TempDir) {
        skip_in_ci!();

        let mut builder = construct_builder(tmp_dir);

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
    fn test_update_phenotypic_features(
        tmp_dir: TempDir,
        phenopacket_id: String,
        valid_phenotype: String,
    ) {
        skip_in_ci!();

        let mut builder = construct_builder(tmp_dir);

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
        tmp_dir: TempDir,
        phenopacket_id: String,
        onset_te: Option<TimeElement>,
        onset_te_alt: Option<TimeElement>,
        valid_phenotype: String,
    ) {
        skip_in_ci!();

        let mut builder = construct_builder(tmp_dir);

        // Add a feature
        builder
            .upsert_phenotypic_feature(
                phenopacket_id.as_str(),
                &valid_phenotype,
                None,
                None,
                None,
                None,
                onset_te,
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
            onset_te_alt.clone(),
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
        assert_eq!(feature_onset, &onset_te_alt.unwrap());
    }

    //todo to be updated when upsert individual is fully implemented
    #[rstest]
    fn test_upsert_individual(tmp_dir: TempDir) {
        skip_in_ci!();

        let mut builder = construct_builder(tmp_dir);

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
            None,
        );
        assert!(result.is_ok());

        // Test upserting the other entries
        let phenopacket = builder.subject_to_phenopacket.get(phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();
        assert_eq!(individual.id, individual_id);
        assert_eq!(individual.sex, 0);
        assert_eq!(individual.vital_status, None);

        let vs = VitalStatus {
            status: 1,
            ..Default::default()
        };

        let result = builder.upsert_individual(
            phenopacket_id,
            individual_id,
            None,
            None,
            None,
            Some(vs.clone()),
            Some("MALE"),
            None,
            None,
            None,
        );
        assert!(result.is_ok());

        let phenopacket = builder.subject_to_phenopacket.get(phenopacket_id).unwrap();
        let individual = phenopacket.subject.as_ref().unwrap();

        assert_eq!(individual.sex, Sex::Male as i32);
        assert_eq!(individual.vital_status, Some(vs));
    }

    #[rstest]
    fn test_get_or_create_phenopacket(tmp_dir: TempDir) {
        skip_in_ci!();

        let mut builder = construct_builder(tmp_dir);
        let phenopacket_id = "pp_001";
        builder.get_or_create_phenopacket(phenopacket_id);
        let pp = builder.get_or_create_phenopacket(phenopacket_id);
        assert_eq!(pp.id, phenopacket_id);
        assert_eq!(builder.subject_to_phenopacket.len(), 1);
    }
}
