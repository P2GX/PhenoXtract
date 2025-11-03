use crate::validation::linter::enums::LintingViolations;
use crate::validation::linter::linting_report::LintReport;
use crate::validation::linter::rules::phenotype_validator::PhenotypeValidator;
use crate::validation::linter::traits::RuleCheck;
use ontolius::TermId;
use ontolius::ontology::HierarchyQueries;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::PhenotypicFeature;
use std::str::FromStr;
use std::sync::Arc;

pub struct ModifierOntologyChildRule {
    hpo: Arc<FullCsrOntology>,
    clinical_modifiers: TermId,
}

impl ModifierOntologyChildRule {
    fn new(hpo: Arc<FullCsrOntology>) -> Self {
        ModifierOntologyChildRule {
            hpo,
            clinical_modifiers: TermId::from_str("HP:0012823").unwrap(),
        }
    }
}

impl RuleCheck for ModifierOntologyChildRule {
    fn check(&self, phenopacket: &Phenopacket, report: &mut LintReport) {
        phenopacket
            .phenotypic_features
            .iter()
            .for_each(|feature_type| {
                feature_type.modifiers.iter().for_each(|modi| {
                    if !self.hpo.is_ancestor_of(
                        &TermId::from_str(&modi.id).unwrap(),
                        &self.clinical_modifiers,
                    ) {
                        report.push_violation(LintingViolations::NonModifier(modi.clone()));
                    }
                })
            })
    }

    fn rule_id(&self) -> &'static str {
        "PF002"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HPO;
    use phenopackets::schema::v2::core::OntologyClass;
    use rstest::rstest;

    #[rstest]
    fn test_find_non_modifiers() {
        let rule = ModifierOntologyChildRule::new(HPO.clone());

        let modifier = OntologyClass {
            id: "HP:0002197".to_string(),
            label: "Generalized-onset seizure".to_string(),
        };

        let phenopacket = Phenopacket {
            phenotypic_features: vec![PhenotypicFeature {
                modifiers: vec![modifier.clone()],
                ..Default::default()
            }],

            ..Default::default()
        };

        let mut report = LintReport::new();
        rule.check(&phenopacket, &mut report);

        match report.into_violations().first().unwrap() {
            LintingViolations::NonModifier(feature) => {
                assert_eq!(feature, &modifier);
            }
            _ => {
                panic!("Wrong LintingViolation")
            }
        }
    }
}
