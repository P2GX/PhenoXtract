use log::debug;
use ontolius::ontology::HierarchyQueries;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::{Identified, TermId};
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::OntologyClass;
use phenopackets::schema::v2::core::PhenotypicFeature;
use std::collections::HashSet;
use std::rc::Rc;
use std::str::FromStr;

struct PhenopacketLinter {
    hpo: Rc<FullCsrOntology>,
}
struct LintReport;
impl PhenopacketLinter {
    pub fn lint(&self, phenopackets: &[Phenopacket], fix: bool) -> LintReport {
        let lint_report = LintReport {};

        for pp in phenopackets {
            let duplicates =
                self.find_duplicate_phenotypic_features(&pp.phenotypic_features.clone());
            let invalid_phenotypic_features =
                self.find_related_phenotypic_features(&pp.phenotypic_features.clone());

            if fix {
                self.fix(pp, duplicates, invalid_phenotypic_features);
            }
        }

        lint_report
    }

    fn fix(
        &self,
        mut phenopacket: &Phenopacket,
        duplicates: HashSet<String>,
        invalid_phenotypic_features: HashSet<String>,
    ) {
        todo!()
    }

    fn find_duplicate_phenotypic_features(
        &self,
        phenotypic_features: &[PhenotypicFeature],
    ) -> HashSet<String> {
        let mut duplicates: HashSet<String> = HashSet::new();
        let mut seen: HashSet<String> = HashSet::new();

        for pf in phenotypic_features {
            if let Some(ontology_class) = pf.r#type.clone() {
                let pf_id = ontology_class.id;
                if seen.contains(pf_id.as_str()) {
                    duplicates.insert(pf_id.clone());
                }
                seen.insert(pf_id);
            }
        }

        duplicates
    }

    fn find_related_phenotypic_features(
        &self,
        phenotypic_features: &[PhenotypicFeature],
    ) -> HashSet<String> {
        let mut observed: HashSet<TermId> = HashSet::new();
        let mut excluded: HashSet<TermId> = HashSet::new();

        for pf in phenotypic_features {
            if let Some(feature_type) = pf.r#type.clone() {
                let term = PhenopacketLinter::ontology_class_to_term_id(&feature_type);
                if !pf.excluded || pf.onset.is_some() || !pf.modifiers.is_empty() {
                    observed.insert(term);
                } else {
                    excluded.insert(term);
                }
            }
        }

        let mut invalid_terms = HashSet::new();

        // Amongst the observed terms, we want to keep the most specific ones.
        // Which means, if we find a term that is more general then another, we deem the more general term invalid.
        // If there is a more specific excluded term, we should invalidate that as well.
        // In this case we assume that the excluded term is invalid,  because a specific ancestor was annotated
        for observed_term in &observed {
            for other_observed in &observed {
                if observed_term != other_observed
                    && self.hpo.is_ancestor_of(observed_term, other_observed)
                {
                    debug!(
                        "Found related terms amongst observed terms {:?} -> {:?}",
                        observed_term.identifier().to_string(),
                        other_observed.identifier().to_string()
                    );
                    invalid_terms.insert(observed_term.clone());
                }
            }
            for excluded_term in &excluded {
                debug!(
                    "Found related terms amongst observed and excluded terms {:?} -> {:?}",
                    observed_term.identifier().to_string(),
                    excluded_term.identifier().to_string()
                );
                if self.hpo.is_ancestor_of(observed_term, excluded_term) {
                    invalid_terms.insert(excluded_term.clone());
                }
            }
        }

        // Amongst the excluded terms, we want to keep the most general.
        // Because, if you can exclude a general phenotype the specific one can also be excluded.
        for excluded_term in &excluded {
            for other in &excluded {
                if excluded_term != other && self.hpo.is_descendant_of(excluded_term, other) {
                    debug!(
                        "Found related terms amongst excluded terms {:?} -> {:?}",
                        excluded_term.identifier().to_string(),
                        other.identifier().to_string()
                    );
                    invalid_terms.insert(other.clone());
                }
            }
        }
        invalid_terms.iter().map(ToString::to_string).collect()
    }

    fn ontology_class_to_term_id(ontology_class: &OntologyClass) -> TermId {
        TermId::from_str(ontology_class.id.as_str()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ontology::github_ontology_registry::GithubOntologyRegistry;
    use crate::ontology::traits::OntologyRegistry;
    use crate::ontology::utils::init_ontolius;
    use crate::skip_in_ci;
    use rstest::*;
    use tempfile::TempDir;

    #[fixture]
    fn tmp_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    fn construct_linter(tmp_dir: TempDir) -> PhenopacketLinter {
        let hpo_registry = GithubOntologyRegistry::default_hpo_registry()
            .unwrap()
            .with_registry_path(tmp_dir.path().into());
        let path = hpo_registry.register("latest").unwrap();

        PhenopacketLinter {
            hpo: init_ontolius(path).unwrap(),
        }
    }

    #[rstest]
    fn test_find_related_phenotypic_features_exclude_general_term(tmp_dir: TempDir) {
        let linter = construct_linter(tmp_dir);

        let phenotypic_features = vec![
            PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: "HP:0000448".to_string(),
                    label: "Prominent nose".to_string(),
                }),
                ..Default::default()
            },
            PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: "HP:0005105".to_string(),
                    label: "Abnormal nasal morphology".to_string(),
                }),
                ..Default::default()
            },
        ];

        let invalid_terms = linter.find_related_phenotypic_features(&phenotypic_features);
        if let Some(hpo_id) = invalid_terms.iter().next() {
            assert_eq!(hpo_id, "HP:0005105");
        }
    }

    #[rstest]
    fn test_find_related_phenotypic_features_exclude_specific_excluded_term(tmp_dir: TempDir) {
        let linter = construct_linter(tmp_dir);

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
                excluded: true,
                ..Default::default()
            },
        ];

        let invalid_terms = linter.find_related_phenotypic_features(&phenotypic_features);
        if let Some(hpo_id) = invalid_terms.iter().next() {
            assert_eq!(hpo_id, "HP:0001098");
        }
    }

    #[rstest]
    fn test_find_related_phenotypic_features_excluded_more_specific(tmp_dir: TempDir) {
        let linter = construct_linter(tmp_dir);

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

        let invalid_terms = linter.find_related_phenotypic_features(&phenotypic_features);
        if let Some(hpo_id) = invalid_terms.iter().next() {
            assert_eq!(hpo_id, "HP:0000608");
        }
    }

    #[rstest]
    fn test_find_duplicate_phenotypic_features(tmp_dir: TempDir) {
        skip_in_ci!();

        let linter = construct_linter(tmp_dir);

        let phenotypic_features = vec![
            PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: "HP:0001098".to_string(),
                    label: "Macular degeneration".to_string(),
                }),
                excluded: true,
                ..Default::default()
            },
            PhenotypicFeature {
                r#type: Some(OntologyClass {
                    id: "HP:0001098".to_string(),
                    label: "Macular degeneration".to_string(),
                }),
                excluded: false,
                ..Default::default()
            },
        ];

        let duplicates = linter.find_duplicate_phenotypic_features(phenotypic_features.as_slice());

        assert_eq!(duplicates.len(), 1);
        assert_eq!(duplicates.iter().next().unwrap(), "HP:0001098");
    }
}
