use crate::validation::linter::enums::LintingViolations;
use crate::validation::linter::linting_report::{LintReport, LintReportInfo};
use crate::validation::linter::rules::utils;
use crate::validation::linter::traits::RuleCheck;
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v2::Phenopacket;
use std::sync::Arc;

/// Validates that observed phenotypic terms don't have redundant observed ancestors.
///
/// This rule implements the linting check `PF007`, which identifies cases where both a
/// specific phenotypic term and its more general ancestor terms are marked as observed
/// in a phenopacket. Best practice in phenotype annotation is to use the most specific
/// terms possible. When a specific phenotype is observed, annotating its general
/// ancestors adds no additional information and should be avoided.
///
/// # Rule Logic
///
/// For each observed phenotypic term in the phenopacket:
/// 1. Determines if the term is a "scion" (has no observed descendants, making it most specific)
/// 2. Finds all ancestor terms that are also explicitly observed
/// 3. Reports an `ObservedAncestor` violation if redundant ancestors are found
///
/// # Example
///
/// If "Ventricular septal defect" (HP:0001629) is marked as observed, then also marking
/// its ancestor "Abnormal heart morphology" (HP:0001627) as observed would be flagged
/// as redundant. The specific term already conveys all the information of the general
/// term, making the ancestor annotation unnecessary.
struct ObservedAncestorRule {
    hpo: Arc<FullCsrOntology>,
}

impl ObservedAncestorRule {
    fn new(hpo: Arc<FullCsrOntology>) -> Self {
        ObservedAncestorRule { hpo }
    }
}

impl RuleCheck for ObservedAncestorRule {
    fn check(&self, phenopacket: &Phenopacket, report: &mut LintReport) {
        let (observed, _) = utils::partition_phenotypic_features(phenopacket);

        // Invalidate all ancestors of a family for an observed term
        // Amongst the observed terms, we want to keep the most specific ones.
        // Which means, if we find a term that is more general then another, we deem the more general term invalid.
        observed.iter().for_each(|phenotypic_term| {
            let is_scion =
                utils::find_descendents(self.hpo.clone(), &observed, phenotypic_term).is_empty();

            if is_scion {
                let ancestor_terms =
                    utils::find_ancestors(self.hpo.clone(), &observed, phenotypic_term);

                if !ancestor_terms.is_empty() {
                    // TODO: Add empty check
                    report.push_info(LintReportInfo::new(
                        LintingViolations::ObservedAncestor {
                            scion: utils::term_to_ontology_class(
                                self.hpo.term_by_id(phenotypic_term).unwrap_or_else(|| {
                                    panic!("Could find term for id: '{}'", phenotypic_term)
                                }),
                            ),
                            ancestors: ancestor_terms
                                .iter()
                                .map(|ancestor| {
                                    utils::term_to_ontology_class(
                                        self.hpo.term_by_id(ancestor).unwrap_or_else(|| {
                                            panic!("Could find term for id: '{}'", ancestor)
                                        }),
                                    )
                                })
                                .collect(),
                        },
                        None,
                    ))
                }
            }
        });
    }

    fn rule_id() -> &'static str
    where
        Self: Sized,
    {
        "PF007"
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::HPO;
    use crate::validation::linter::enums::LintingViolations;
    use crate::validation::linter::linting_report::LintReport;
    use crate::validation::linter::rules::phenotypic_features::observed_ancestor_rule::ObservedAncestorRule;
    use crate::validation::linter::traits::RuleCheck;
    use phenopackets::schema::v2::Phenopacket;
    use phenopackets::schema::v2::core::{OntologyClass, PhenotypicFeature};
    use rstest::rstest;

    #[rstest]
    fn test_find_related_phenotypic_features_case_1() {
        let rule = ObservedAncestorRule::new(HPO.clone());
        let expected_progenitor = OntologyClass {
            id: "HP:0000448".to_string(),
            label: "Prominent nose".to_string(),
        };
        let expected_middle = OntologyClass {
            id: "HP:0005105".to_string(),
            label: "Abnormal nasal morphology".to_string(),
        };
        let expected_ancestor = OntologyClass {
            id: "HP:0000366".to_string(),
            label: "Abnormality of the nose".to_string(),
        };
        let phenopacket = Phenopacket {
            phenotypic_features: vec![
                PhenotypicFeature {
                    r#type: Some(expected_middle.clone()),
                    ..Default::default()
                },
                PhenotypicFeature {
                    r#type: Some(expected_ancestor.clone()),
                    ..Default::default()
                },
                PhenotypicFeature {
                    r#type: Some(expected_progenitor.clone()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let mut report = LintReport::new();
        rule.check(&phenopacket, &mut report);

        let violations = report.into_violations();
        assert_eq!(violations.len(), 1);
        for violation in violations {
            match violation {
                LintingViolations::ObservedAncestor {
                    scion: progenitor,
                    ancestors,
                } => {
                    assert_eq!(progenitor, expected_progenitor);
                    assert_eq!(ancestors.len(), 2);
                    assert!(ancestors.contains(&expected_middle));
                    assert!(ancestors.contains(&expected_ancestor));
                }
                _ => panic!("Unexpected violation: {:?}", violation),
            }
        }
    }
}
