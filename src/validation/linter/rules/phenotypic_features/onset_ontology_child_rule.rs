use crate::validation::linter::enums::LintingViolations;
use crate::validation::linter::linting_report::LintReport;
use crate::validation::linter::traits::RuleCheck;
use ontolius::TermId;
use ontolius::ontology::HierarchyQueries;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::time_element::Element;
use std::str::FromStr;
use std::sync::Arc;
#[derive(Debug)]
/// Validates that phenotypic feature onset terms are descendants of the Onset term.
///
/// This rule implements the linting check `PF003`, which ensures that all onset
/// annotations for phenotypic features use valid HPO onset terms. According to the
/// HPO specification, onset terms must be descendants of "Onset" (HP:0003674).
///
/// # Rule Logic
///
/// For each phenotypic feature in the phenopacket:
/// 1. Checks if the feature has an onset annotation
/// 2. Verifies the onset is specified as an OntologyClass element
/// 3. Validates that the term is a descendant of HP:0003674 (Onset)
/// 4. Reports a `NonOnset` violation if an invalid term is used for onset
///
/// # Example
///
/// Using "Abnormal heart morphology" (HP:0001627) as an onset term would be flagged
/// as invalid because it's a phenotypic abnormality term, not an onset term.
/// Valid onset terms include "Congenital onset" (HP:0003577), "Adult onset" (HP:0003581),
/// or "Childhood onset" (HP:0011463), which are all descendants of HP:0003674.
pub struct OnsetOntologyChildRule {
    hpo: Arc<FullCsrOntology>,
    onsets: TermId,
}

impl OnsetOntologyChildRule {
    fn new(hpo: Arc<FullCsrOntology>) -> Self {
        OnsetOntologyChildRule {
            hpo,
            onsets: TermId::from_str("HP:0003674").unwrap(),
        }
    }
}

impl RuleCheck for OnsetOntologyChildRule {
    fn check(&self, phenopacket: &Phenopacket, report: &mut LintReport) {
        for feature in &phenopacket.phenotypic_features {
            let Some(onset) = &feature.onset else {
                continue;
            };

            let Some(Element::OntologyClass(oc)) = &onset.element else {
                continue;
            };

            let Ok(term_id) = TermId::from_str(&oc.id) else {
                continue;
            };

            if !self.hpo.is_ancestor_of(&term_id, &self.onsets) {
                report.push_violation(LintingViolations::NonOnset(oc.clone()));
            }
        }
    }

    fn rule_id() -> &'static str {
        "PF003"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HPO;
    use phenopackets::schema::v2::core::{OntologyClass, PhenotypicFeature, TimeElement};
    use rstest::rstest;

    #[rstest]
    fn test_find_non_onsets() {
        let rule = OnsetOntologyChildRule::new(HPO.clone());
        let onset = OntologyClass {
            id: "HP:0002197".to_string(),
            label: "Generalized-onset seizure".to_string(),
        };

        let phenopacket = Phenopacket {
            phenotypic_features: vec![PhenotypicFeature {
                onset: Some(TimeElement {
                    element: Some(Element::OntologyClass(onset.clone())),
                }),
                ..Default::default()
            }],

            ..Default::default()
        };

        let mut report = LintReport::new();
        rule.check(&phenopacket, &mut report);

        match report.into_violations().first().unwrap() {
            LintingViolations::NonOnset(feature) => {
                assert_eq!(feature, &onset);
            }
            _ => {
                panic!("Wrong LintingViolation")
            }
        }
    }
}
