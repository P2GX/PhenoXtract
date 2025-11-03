use crate::validation::linter::enums::LintingViolations;
use crate::validation::linter::linting_report::LintReport;
use crate::validation::linter::traits::RuleCheck;
use ontolius::TermId;
use ontolius::ontology::HierarchyQueries;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v2::Phenopacket;
use std::str::FromStr;
use std::sync::Arc;

pub struct SeverityOntologyChildRule {
    hpo: Arc<FullCsrOntology>,
    severity: TermId,
}

impl SeverityOntologyChildRule {
    pub fn new(hpo: Arc<FullCsrOntology>) -> Self {
        SeverityOntologyChildRule {
            hpo,
            severity: TermId::from_str("HP:0012824").unwrap(),
        }
    }
}

impl RuleCheck for SeverityOntologyChildRule {
    fn check(&self, phenopacket: &Phenopacket, report: &mut LintReport) {
        phenopacket
            .phenotypic_features
            .iter()
            .for_each(|feature_type| {
                if let Some(f) = &feature_type.severity
                    && !self
                        .hpo
                        .is_ancestor_of(&TermId::from_str(&f.id).unwrap(), &self.severity)
                {
                    report.push_violation(LintingViolations::NonSeverity(f.clone()));
                }
            })
    }

    fn rule_id(&self) -> &'static str {
        "PF004"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HPO;
    use phenopackets::schema::v2::core::{OntologyClass, PhenotypicFeature};
    use rstest::rstest;

    #[rstest]
    fn test_find_non_severity() {
        let rule = SeverityOntologyChildRule::new(HPO.clone());

        let severity = OntologyClass {
            id: "HP:0410401".to_string(),
            label: "Worse in evening".to_string(),
        };

        let phenopacket = Phenopacket {
            phenotypic_features: vec![PhenotypicFeature {
                severity: Some(severity.clone()),
                ..Default::default()
            }],

            ..Default::default()
        };

        let mut report = LintReport::new();
        rule.check(&phenopacket, &mut report);
        match report.into_violations().first().unwrap() {
            LintingViolations::NonSeverity(severity) => {
                assert_eq!(severity, severity);
            }
            _ => {
                panic!("Wrong LintingViolation")
            }
        }
    }
}
