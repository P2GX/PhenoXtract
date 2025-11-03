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

    fn rule_id(&self) -> &'static str {
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
