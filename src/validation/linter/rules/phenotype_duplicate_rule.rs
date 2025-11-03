use crate::validation::linter::enums::{FixAction, LintingViolations};
use crate::validation::linter::linting_report::{LintReport, LintReportInfo};
use crate::validation::linter::traits::RuleCheck;
use log::debug;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::{OntologyClass, PhenotypicFeature};
use std::collections::HashMap;
use std::sync::Arc;

pub struct PhenotypeDuplicateRule {
    hpo: Arc<FullCsrOntology>,
}

impl PhenotypeDuplicateRule {
    pub fn new(hpo: Arc<FullCsrOntology>) -> Self {
        Self { hpo }
    }

    fn filter_by_duplicate_ontology_classes(
        &self,
        phenotypic_features: &[PhenotypicFeature],
    ) -> HashMap<String, Vec<PhenotypicFeature>> {
        let mut duplicates: HashMap<String, Vec<PhenotypicFeature>> = HashMap::new();
        let mut seen: Vec<&OntologyClass> = Vec::new();

        for pf in phenotypic_features {
            if let Some(ref ont_class) = pf.r#type {
                if seen.contains(&ont_class) {
                    duplicates
                        .entry(ont_class.id.to_string())
                        .or_default()
                        .push(pf.clone());
                }
                seen.push(ont_class);
            }
        }

        debug!("Duplicate phenotypic features: {:?}", duplicates);
        duplicates
    }
}
impl RuleCheck for PhenotypeDuplicateRule {
    fn check(&self, phenopacket: &Phenopacket, report: &mut LintReport) {
        let duplicate_features =
            self.filter_by_duplicate_ontology_classes(phenopacket.phenotypic_features.as_slice());

        for mut dup_pfs in duplicate_features.values().cloned() {
            // Find pure duplicates
            let mut seen = Vec::new();
            let mut indices_to_remove = Vec::new();

            for (index, pf) in dup_pfs.iter().enumerate() {
                if seen.contains(&pf) {
                    report.push_info(LintReportInfo::new(
                        LintingViolations::DuplicatePhenotype(Box::new(pf.clone())),
                        Some(FixAction::Remove),
                    ));
                    indices_to_remove.push(index);
                } else {
                    seen.push(pf);
                }
            }
            for &i in indices_to_remove.iter().rev() {
                dup_pfs.remove(i);
            }
        }
    }

    fn rule_id(&self) -> &'static str {
        "PF006"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HPO;
    use rstest::rstest;

    #[rstest]
    fn test_find_duplicate_phenotypic_features() {
        let rule = PhenotypeDuplicateRule::new(HPO.clone());

        let phenopacket = Phenopacket {
            phenotypic_features: vec![
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
            ],
            ..Default::default()
        };

        let duplicates =
            rule.filter_by_duplicate_ontology_classes(phenopacket.phenotypic_features.as_slice());

        assert_eq!(duplicates.len(), 1);
        assert_eq!(
            duplicates
                .values()
                .next()
                .unwrap()
                .first()
                .unwrap()
                .r#type
                .clone()
                .unwrap()
                .id,
            "HP:0001098"
        );
    }
}
