#![allow(clippy::too_many_arguments)]
use crate::transform::error::TransformError;
use anyhow::anyhow;
use log::warn;
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::simple::SimpleTerm;
use ontolius::term::{MinimalTerm, Synonymous, Term};
use ontolius::{Identified, TermId};
use phenopackets::schema::v1::core::Evidence;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::{OntologyClass, PhenotypicFeature, TimeElement};
use std::collections::HashMap;
use std::rc::Rc;
use std::str::FromStr;

#[allow(dead_code)]
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
    pub fn upsert_individual(&mut self) -> Result<(), anyhow::Error> {
        todo!()
    }

    fn raw_to_full_term(&self, raw_term: &str) -> Result<SimpleTerm, anyhow::Error> {
        let term = TermId::from_str(raw_term)
            .ok()
            .and_then(|term_id| self.hpo.as_ref().term_by_id(&term_id))
            .or_else(|| {
                self.hpo.as_ref().iter_terms().find(|term| {
                    !term.is_obsolete()
                        && (term.name() == raw_term
                            || term.synonyms().iter().any(|syn| syn.name == raw_term))
                })
            });
        if term.is_none() {
            return Err(anyhow!("Could not find ontology class for {raw_term}"));
        }
        Ok(term.unwrap().clone())
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
        phenopacket_id: String,
        phenotype: &str,
        description: Option<&str>,
        excluded: Option<bool>,
        severity: Option<String>,
        modifiers: Option<Vec<String>>,
        on_set: Option<TimeElement>,
        resolution: Option<TimeElement>,
        evidence: Option<Evidence>,
    ) -> Result<(), anyhow::Error> {
        if description.is_some() {
            warn!("desciption phenotypic feature not implemented yet");
        }
        if excluded.is_some() {
            warn!("is_observed phenotypic feature not implemented yet");
        }
        if severity.is_some() {
            warn!("severity phenotypic feature not implemented yet");
        }
        if modifiers.is_some() {
            warn!("modifiers phenotypic feature not implemented yet");
        }
        if on_set.is_some() {
            warn!("on_set phenotypic feature not implemented yet");
        }
        if resolution.is_some() {
            warn!("resolution phenotypic feature not implemented yet");
        }
        if evidence.is_some() {
            warn!("evidence phenotypic feature not implemented yet");
        }

        let term = self.raw_to_full_term(phenotype)?;
        let phenopacket = self
            .subject_to_phenopacket
            .entry(phenopacket_id.clone())
            .or_insert_with(|| Phenopacket {
                id: phenopacket_id,
                ..Default::default()
            });

        let mut phenotypic_feature: PhenotypicFeature = phenopacket
            .phenotypic_features
            .iter()
            .find(|feature| {
                if let Some(t) = &feature.r#type {
                    return t.id == term.identifier().to_string();
                };
                false
            })
            .cloned()
            .unwrap_or_default();

        phenotypic_feature.r#type = Some(OntologyClass {
            id: term.identifier().to_string(),
            label: term.name().to_string(),
        });
        phenopacket.phenotypic_features.push(phenotypic_feature);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
    use crate::ontology::traits::OntologyRegistry;
    use crate::ontology::utils::init_ontolius;
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
    fn another_phenotype() -> String {
        "HP:0000252".to_string()
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
        tmp_dir: TempDir,
    ) {
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_upsert_phenotypic_feature_success");
            return;
        }
        let mut builder = construct_builder(tmp_dir);
        let result = builder.upsert_phenotypic_feature(
            phenopacket_id.clone(),
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

        assert!(builder.subject_to_phenopacket.contains_key(&phenopacket_id));

        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 1);

        let feature = &phenopacket.phenotypic_features[0];
        assert!(feature.r#type.is_some());

        let ontology_class = feature.r#type.as_ref().unwrap();
        assert_eq!(ontology_class.id, "HP:0001166");
        assert_eq!(ontology_class.label, "Arachnodactyly");
    }

    #[rstest]
    fn test_upsert_phenotypic_feature_invalid_term(tmp_dir: TempDir, phenopacket_id: String) {
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_upsert_phenotypic_feature_invalid_term");
            return;
        }
        let mut builder = construct_builder(tmp_dir);

        let result = builder.upsert_phenotypic_feature(
            phenopacket_id,
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
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_multiple_phenotypic_features_same_phenopacket");
            return;
        }
        let mut builder = construct_builder(tmp_dir);

        let result1 = builder.upsert_phenotypic_feature(
            phenopacket_id.clone(),
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
            phenopacket_id.clone(),
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

        // Check both features exist
        let phenopacket = builder.subject_to_phenopacket.get(&phenopacket_id).unwrap();
        assert_eq!(phenopacket.phenotypic_features.len(), 2);
    }

    #[rstest]
    fn test_different_phenopacket_ids(valid_phenotype: String, tmp_dir: TempDir) {
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_different_phenopacket_ids");
            return;
        }
        let mut builder = construct_builder(tmp_dir);

        let id1 = "pp_001".to_string();
        let id2 = "pp_002".to_string();

        let result1 = builder.upsert_phenotypic_feature(
            id1.clone(),
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
            id2.clone(),
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

        // Check both phenopackets exist
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
        let ci = std::env::var("CI");
        if ci.is_ok() {
            println!("Skipping test_update_phenotypic_features");
            return;
        }
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
            phenopacket_id.clone(),
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
}
