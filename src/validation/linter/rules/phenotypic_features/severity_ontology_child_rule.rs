use crate::validation::linter::enums::LintingViolations;
use crate::validation::linter::linting_report::LintReport;
use crate::validation::linter::traits::RuleCheck;
use ontolius::TermId;
use ontolius::ontology::HierarchyQueries;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v2::Phenopacket;
use std::str::FromStr;
use std::sync::Arc;
#[derive(Debug)]
/// Validates that phenotypic feature severity terms are descendants of the Severity term.
///
/// This rule implements the linting check `PF004`, which ensures that all severity
/// annotations for phenotypic features use valid HPO severity terms. According to the
/// HPO specification, severity terms must be descendants of "Severity" (HP:0012824).
///
/// # Rule Logic
///
/// For each phenotypic feature in the phenopacket:
/// 1. Checks if the feature has a severity annotation
/// 2. Verifies that the term is a descendant of HP:0012824 (Severity)
/// 3. Reports a `NonSeverity` violation if an invalid term is used for severity
///
/// # Example
///
/// Using "Seizure" (HP:0001250) as a severity term would be flagged as invalid
/// because it's a phenotypic abnormality term, not a severity term. Valid severity
/// terms include "Severe" (HP:0012828), "Moderate" (HP:0012826), "Mild" (HP:0012825),
/// or "Profound" (HP:0012829), which are all descendants of HP:0012824.
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

    fn rule_id() -> &'static str {
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
