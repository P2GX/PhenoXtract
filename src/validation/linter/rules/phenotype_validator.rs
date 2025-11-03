use crate::validation::linter::linting_report::{LintReport, LintReportInfo};
use log::debug;
use ontolius::TermId;
use ontolius::ontology::HierarchyQueries;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::PhenotypicFeature;

use crate::validation::linter::enums::{FixAction, LintingViolations};
use crate::validation::linter::traits::RuleCheck;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

pub(crate) struct PhenotypeValidator {
    hpo: Arc<FullCsrOntology>,
    phenotypic_abnormality: TermId,
    clinical_modifiers: TermId,
    onsets: TermId,
    severity: TermId,
}

impl RuleCheck for PhenotypeValidator {
    fn check(&self, phenopacket: &Phenopacket, lint_report: &mut LintReport) {
        let phenotypic_features = phenopacket.phenotypic_features.as_slice();
    }
}
impl PhenotypeValidator {
    pub fn new(hpo: Arc<FullCsrOntology>) -> Self {
        PhenotypeValidator {
            hpo,
            phenotypic_abnormality: TermId::from_str("HP:0000118").unwrap(),
            clinical_modifiers: TermId::from_str("HP:0012823").unwrap(),
            onsets: TermId::from_str("HP:0003674").unwrap(),
            severity: TermId::from_str("HP:0012824").unwrap(),
        }
    }
    // Duplicates Same level  | Action
    // Pure duplicates -> Remove
    // excluded and included -> None
    // As soon as there is an onset, severity or modifiers phenotypes can not be merged otherwise -> Merge

    fn is_mergable_pf(phenotypic_features: &PhenotypicFeature) -> bool {
        phenotypic_features.onset.is_some()
            || phenotypic_features.modifiers.is_empty()
            || phenotypic_features.severity.is_none()
    }

    fn is_empty_pf(phenotypic_features: &PhenotypicFeature) -> bool {
        phenotypic_features.onset.is_none()
            && phenotypic_features.severity.is_none()
            && phenotypic_features.modifiers.is_empty()
            && phenotypic_features.description.is_empty()
            && phenotypic_features.evidence.is_empty()
            && phenotypic_features.resolution.is_none()
    }

    fn find_related_phenotypic_features(
        &self,
        phenotypic_features: &[PhenotypicFeature],
    ) -> HashSet<String> {
        let mut observed: HashSet<TermId> = HashSet::new();
        let mut excluded: HashSet<TermId> = HashSet::new();

        for pf in phenotypic_features {
            if let Some(feature_type) = pf.r#type.clone() {
                let phenotypic_term = TermId::from_str(feature_type.id.as_str()).unwrap();
                if !pf.excluded {
                    observed.insert(phenotypic_term);
                } else {
                    excluded.insert(phenotypic_term);
                }
            }
        }

        // Case 1: Invalidate all ancestors of a family for an observed term
        // Amongst the observed terms, we want to keep the most specific ones.
        // Which means, if we find a term that is more general then another, we deem the more general term invalid.
        let invalid_observed_ancestors = observed
            .iter()
            .flat_map(|phenotypic_term| self.find_ancestors(&observed, phenotypic_term))
            .collect::<HashSet<TermId>>();

        debug!(
            "Found invalid observed ancestors: {:?}",
            invalid_observed_ancestors
        );

        // Case 2: Invalidate excluded terms that share the same family with an observed term and are descendents
        // If there is a more specific excluded term, we should invalidate that as well.
        // In this case we assume that the excluded term is invalid, because a specific ancestor was annotated
        let invalid_excluded_observed_descendents = observed
            .iter()
            .flat_map(|phenotypic_term| self.find_descendents(&excluded, phenotypic_term))
            .collect::<HashSet<TermId>>();

        debug!(
            "Found invalid excluded/observed descendents: {:?}",
            invalid_excluded_observed_descendents
        );

        // Case 3: Invalidate all descendents of a family for an excluded term
        // Because, if you can exclude a general phenotype the specific one can also be excluded.
        let invalid_excluded_descendents = excluded
            .iter()
            .flat_map(|phenotypic_term| self.find_descendents(&excluded, phenotypic_term))
            .collect::<HashSet<TermId>>();

        debug!(
            "Found invalid excluded descendents: {:?}",
            invalid_excluded_descendents
        );

        invalid_observed_ancestors
            .into_iter()
            .chain(invalid_excluded_observed_descendents)
            .chain(invalid_excluded_descendents)
            .collect::<HashSet<TermId>>()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// Finds all ancestor terms of a given scion term within a provided ancestry set.
    ///
    /// This method filters the provided ancestry set to return only those terms that are
    /// ancestors of the specified scion term, excluding the scion term itself from the results.
    /// An ancestor is a term that is higher in the ontology hierarchy and has a path leading
    /// down to the scion term.
    ///
    /// # Arguments
    ///
    /// * `ancestry` - A reference to a HashSet containing TermIds to search within
    /// * `scion` - A reference to the TermId for which to find ancestors
    ///
    /// # Returns
    ///
    /// A HashSet<TermId> containing all terms from the ancestry set that are ancestors
    /// of the scion term. The scion term itself is excluded from the results.
    ///
    /// # Behaviour
    ///
    /// Ancestry:
    ///
    /// Abnormality of the musculoskeletal system ━┓
    /// Abnormal musculoskeletal physiology        ┣━ These will be returned
    /// Limb pain                                 ━┛
    /// Lower limb pain -> Selected as scion
    /// Foot pain
    ///
    /// # Examples
    /// ```ignore
    /// ```rust
    /// let ancestry_set: HashSet<TermId> = [term1, term2, term3, scion_term].iter().cloned().collect();
    /// let ancestors = obj.find_ancestors(&ancestry_set, &scion_term);
    /// // ancestors will contain only those terms from ancestry_set that are ancestors of scion_term
    /// ```
    fn find_ancestors(&self, ancestry: &HashSet<TermId>, scion: &TermId) -> HashSet<TermId> {
        ancestry
            .iter()
            .filter(|term| *term != scion && self.hpo.is_ancestor_of(*term, scion))
            .cloned()
            .collect()
    }

    /// Finds all descendant terms of a given progenitor term within a provided ancestry set.
    ///
    /// This method filters the provided ancestry set to return only those terms that are
    /// descendants of the specified progenitor term, excluding the progenitor term itself
    /// from the results. A descendant is a term that is lower in the ontology hierarchy
    /// and can be reached by following paths down from the progenitor term.
    ///
    /// # Arguments
    ///
    /// * `ancestry` - A reference to a HashSet containing TermIds to search within
    /// * `progenitor` - A reference to the TermId for which to find descendants
    ///
    /// # Returns
    ///
    /// A HashSet<TermId> containing all terms from the ancestry set that are descendants
    /// of the progenitor term. The progenitor term itself is excluded from the results.
    ///
    /// # Behaviour
    ///
    /// Ancestry:
    ///
    /// Abnormality of the musculoskeletal system
    /// Abnormal musculoskeletal physiology -> Selected as progenitor
    /// Limb pain       ━┓
    /// Lower limb pain  ┣━ These will be returned
    /// Foot pain       ━┛
    ///
    /// # Examples
    /// ```ignore
    /// ```rust
    /// let ancestry_set: HashSet<TermId> = [progenitor_term, term1, term2, term3].iter().cloned().collect();
    /// let descendants = obj.find_descendents(&ancestry_set, &progenitor_term);
    /// // descendants will contain only those terms from ancestry_set that are descendants of progenitor_term
    /// ```
    fn find_descendents(&self, ancestry: &HashSet<TermId>, progenitor: &TermId) -> HashSet<TermId> {
        ancestry
            .iter()
            .filter(|term| *term != progenitor && self.hpo.is_descendant_of(*term, progenitor))
            .cloned()
            .collect()
    }

    // TODO: These should add a move operation, if the Ontology class falls into another category.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HPO;
    use phenopackets::schema::v2::core::{OntologyClass, TimeElement};
    use rstest::{fixture, rstest};

    #[fixture]
    fn term_ancestry() -> Vec<TermId> {
        vec![
            "HP:0000448".parse().unwrap(), // scion
            "HP:0005105".parse().unwrap(),
            "HP:0000366".parse().unwrap(),
            "HP:0000271".parse().unwrap(), // progenitor
        ]
    }

    fn construct_validator() -> PhenotypeValidator {
        PhenotypeValidator::new(HPO.clone())
    }

    #[rstest]
    fn test_find_ancestors(term_ancestry: Vec<TermId>) {
        let validator = construct_validator();

        let ancestors = validator.find_ancestors(
            &term_ancestry.iter().cloned().collect(),
            &"HP:0005105".parse().unwrap(),
        );

        assert!(ancestors.contains(&TermId::from_str("HP:0000366").unwrap()));
        assert!(ancestors.contains(&TermId::from_str("HP:0000271").unwrap()));
    }

    #[rstest]
    fn test_find_descendents(term_ancestry: Vec<TermId>) {
        let validator = construct_validator();

        let ancestors = validator.find_descendents(
            &term_ancestry.iter().cloned().collect(),
            &"HP:0005105".parse().unwrap(),
        );

        assert!(ancestors.contains(&TermId::from_str("HP:0000448").unwrap()));
    }

    #[rstest]
    fn test_find_related_phenotypic_features_case_1() {
        let validator = construct_validator();

        let phenotypic_features = vec![
            PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: "HP:0005105".to_string(),
                    label: "Abnormal nasal morphology".to_string(),
                }),
                ..Default::default()
            },
            PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: "HP:0000366".to_string(),
                    label: "Abnormality of the nose".to_string(),
                }),
                ..Default::default()
            },
            PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: "HP:0000448".to_string(),
                    label: "Prominent nose".to_string(),
                }),
                ..Default::default()
            },
        ];

        let invalid_terms = validator.find_related_phenotypic_features(&phenotypic_features);
        assert_eq!(invalid_terms.len(), 2);
        invalid_terms.contains("HP:0005105");
        invalid_terms.contains("HP:0000366");
    }

    #[rstest]
    fn test_find_related_phenotypic_features_case_2() {
        let validator = construct_validator();

        let phenotypic_features = vec![
            PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: "HP:0000608".to_string(),
                    label: "Macular degeneration".to_string(),
                }),
                excluded: true,
                ..Default::default()
            },
            PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: "HP:0001098".to_string(),
                    label: "Abnormal fundus morphology".to_string(),
                }),
                excluded: false,
                ..Default::default()
            },
        ];

        let invalid_terms = validator.find_related_phenotypic_features(&phenotypic_features);
        assert_eq!(invalid_terms.len(), 1);
        if let Some(hpo_id) = invalid_terms.iter().next() {
            assert_eq!(hpo_id, "HP:0000608");
        }
    }

    #[rstest]
    fn test_find_related_phenotypic_features_case_3() {
        let validator = construct_validator();

        let phenotypic_features = vec![
            PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: "HP:0001098".to_string(),
                    label: "Abnormal fundus morphology".to_string(),
                }),
                excluded: true,
                ..Default::default()
            },
            PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: "HP:0000608".to_string(),
                    label: "Macular degeneration".to_string(),
                }),
                excluded: true,
                ..Default::default()
            },
        ];

        let invalid_terms = validator.find_related_phenotypic_features(&phenotypic_features);
        assert_eq!(invalid_terms.len(), 1);
        invalid_terms.contains("HP:0001098");
    }
}
