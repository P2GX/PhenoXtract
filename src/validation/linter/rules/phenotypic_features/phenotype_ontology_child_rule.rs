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
/// Validates that phenotypic features are descendants of the Phenotypic abnormality term.
///
/// This rule implements the linting check `PF001`, which ensures that all terms used
/// as phenotypic features belong to the phenotypic abnormality branch of the HPO.
/// According to the phenopacket specification, phenotypic features must be descendants
/// of "Phenotypic abnormality" (HP:0000118).
///
/// # Rule Logic
///
/// For each phenotypic feature in the phenopacket:
/// 1. Checks if the feature has an ontology class specified
/// 2. Verifies that the term is a descendant of HP:0000118 (Phenotypic abnormality)
/// 3. Reports a `NonPhenotypicFeature` violation if an invalid term is used
///
/// # Example
///
/// Using "Clinical modifier" (HP:0012823) or "Onset" (HP:0003674) as a phenotypic
/// feature would be flagged as invalid because these terms are not descendants of
/// HP:0000118. Valid phenotypic features include terms like "Seizure" (HP:0001250)
/// or "Intellectual disability" (HP:0001249), which are part of the phenotypic
/// abnormality hierarchy.
pub struct PhenotypeOntologyChildRule {
    hpo: Arc<FullCsrOntology>,
    phenotypic_abnormality: TermId,
}

impl RuleCheck for PhenotypeOntologyChildRule {
    fn check(&self, phenopacket: &Phenopacket, report: &mut LintReport) {
        phenopacket
            .phenotypic_features
            .iter()
            .for_each(|feature_type| {
                if let Some(f) = &feature_type.r#type
                    && !self.hpo.is_ancestor_of(
                        &TermId::from_str(&f.id).unwrap(),
                        &self.phenotypic_abnormality,
                    )
                {
                    report.push_violation(LintingViolations::NonPhenotypicFeature(f.clone()));
                }
            })
    }

    fn rule_id() -> &'static str {
        "PF001"
    }
}
impl PhenotypeOntologyChildRule {
    pub fn new(hpo: Arc<FullCsrOntology>) -> Self {
        PhenotypeOntologyChildRule {
            hpo,
            phenotypic_abnormality: TermId::from_str("HP:0000118").unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HPO;
    use phenopackets::schema::v2::core::{OntologyClass, PhenotypicFeature};
    use rstest::rstest;

    #[rstest]
    fn test_find_non_phenotypic_abnormalities() {
        let rule = PhenotypeOntologyChildRule::new(HPO.clone());

        let pf = OntologyClass {
            id: "HP:0410401".to_string(),
            label: "Worse in evening".to_string(),
        };

        let phenopacket = Phenopacket {
            phenotypic_features: vec![PhenotypicFeature {
                r#type: Some(pf.clone()),
                ..Default::default()
            }],

            ..Default::default()
        };

        let mut report = LintReport::new();
        rule.check(&phenopacket, &mut report);

        match report.into_violations().first().unwrap() {
            LintingViolations::NonPhenotypicFeature(feature) => {
                assert_eq!(feature, &pf);
            }
            _ => {
                panic!("Wrong LintingViolation")
            }
        }
    }
}
