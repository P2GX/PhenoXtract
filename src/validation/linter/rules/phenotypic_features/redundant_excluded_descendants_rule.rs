use crate::validation::linter::enums::LintingViolations;
use crate::validation::linter::linting_report::{LintReport, LintReportInfo};
use crate::validation::linter::rules::utils;
use crate::validation::linter::traits::RuleCheck;

use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v2::Phenopacket;
use std::sync::Arc;

/// Validates that excluded phenotypic terms don't have redundant excluded descendants.
///
/// This rule implements the linting check `PF008`, which identifies cases where both an
/// ancestor term and its descendant terms are marked as excluded in a phenopacket. When
/// a general phenotype is excluded, all of its more specific descendants are implicitly
/// excluded as well, making their explicit exclusion redundant.
///
/// # Rule Logic
///
/// For each excluded phenotypic term in the phenopacket:
/// 1. Determines if the term is a "progenitor" (has no excluded ancestors itself)
/// 2. Finds all descendant terms that are also explicitly excluded
/// 3. Reports a `ExcludedDescendents` violation if redundant descendants are found
///
/// # Example
///
/// If "Abnormal heart morphology" (HP:0001627) is excluded, then excluding its
/// descendant "Ventricular septal defect" (HP:0001629) would be flagged as redundant,
/// since the more general exclusion already covers all specific heart defects.
#[derive(Debug)]
struct RedundantExcludedDescendantsRule {
    hpo: Arc<FullCsrOntology>,
}

impl RedundantExcludedDescendantsRule {
    pub fn new(hpo: Arc<FullCsrOntology>) -> Self {
        Self { hpo }
    }
}

impl RuleCheck for RedundantExcludedDescendantsRule {
    fn check(&self, phenopacket: &Phenopacket, report: &mut LintReport) {
        let (_, excluded) = utils::partition_phenotypic_features(phenopacket);

        // Case 3: Invalidate all descendents of a family for an excluded term
        // Because, if you can exclude a general phenotype the specific one can also be excluded.
        excluded.iter().for_each(|phenotypic_term| {
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
        "PF008"
    }
}
