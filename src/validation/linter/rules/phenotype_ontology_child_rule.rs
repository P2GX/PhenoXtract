use crate::validation::linter::enums::LintingViolations;
use crate::validation::linter::linting_report::LintReport;
use crate::validation::linter::traits::RuleCheck;
use ontolius::TermId;
use ontolius::ontology::HierarchyQueries;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v2::Phenopacket;

use std::str::FromStr;
use std::sync::Arc;

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

    fn rule_id(&self) -> &'static str {
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
    use phenopackets::schema::v2::core::{OntologyClass, PhenotypicFeature, TimeElement};
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
