use crate::validation::linter::enums::LintingViolations;
use crate::validation::linter::linting_report::{LintReport, LintReportInfo};
use crate::validation::linter::rules::utils;
use crate::validation::linter::traits::RuleCheck;
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v2::Phenopacket;
use std::sync::Arc;

#[derive(Debug)]
/// Validates that excluded phenotypic terms don't contradict observed ancestor terms.
///
/// This rule implements the linting check `PF009`, which identifies cases where a
/// phenotypic term is marked as observed, but more specific descendant terms are
/// marked as excluded. This represents a logical inconsistency: if a general phenotype
/// is present, its specific variants cannot be simultaneously excluded.
///
/// # Rule Logic
///
/// For each observed phenotypic term in the phenopacket:
/// 1. Determines if the term is a "progenitor" (has no excluded descendants)
/// 2. Finds all descendant terms that are explicitly excluded
/// 3. Reports an `ExcludedDescendents` violation if contradictory descendants are found
///
/// # Example
///
/// If "Abnormal heart morphology" (HP:0001627) is marked as observed (present), then
/// marking its descendant "Ventricular septal defect" (HP:0001629) as excluded would
/// be flagged as contradictory. The presence of the general heart abnormality implies
/// that specific heart defects cannot be categorically ruled out.
struct ObservedAncestorWithExcludedDescendantsRule {
    hpo: Arc<FullCsrOntology>,
}

impl ObservedAncestorWithExcludedDescendantsRule {
    pub fn new(hpo: Arc<FullCsrOntology>) -> Self {
        Self { hpo }
    }
}

impl RuleCheck for ObservedAncestorWithExcludedDescendantsRule {
    fn check(&self, phenopacket: &Phenopacket, report: &mut LintReport) {
        let (observed, excluded) = utils::partition_phenotypic_features(phenopacket);

        // Invalidate excluded terms that share the same family with an observed term and are descendents
        // If there is a more specific excluded term, we should invalidate that as well.
        // In this case we assume that the excluded term is invalid, because a specific ancestor was annotated
        observed.iter().for_each(|phenotypic_term| {
            let is_progenitor =
                utils::find_descendents(self.hpo.clone(), &excluded, phenotypic_term).is_empty();

            if is_progenitor {
                let child_terms =
                    utils::find_descendents(self.hpo.clone(), &excluded, phenotypic_term);
                if !child_terms.is_empty() {
                    // TODO: Add empty check
                    report.push_info(LintReportInfo::new(
                        LintingViolations::ExcludedDescendents {
                            progenitor: utils::term_to_ontology_class(
                                self.hpo.term_by_id(phenotypic_term).unwrap_or_else(|| {
                                    panic!("Could find term for id: '{}'", phenotypic_term)
                                }),
                            ),
                            descendents: child_terms
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
        "PF009"
    }
}
