#![allow(dead_code)]
#![allow(unused)]
use crate::validation::linter::curie_validator::CurieValidator;
use crate::validation::linter::error::LintingError;
use crate::validation::linter::interpretation_validator::InterpretationValidator;
use crate::validation::linter::linting_report::{LintReport, LintingViolations};
use crate::validation::linter::phenotype_validator::PhenotypeValidator;
use log::debug;
use ontolius::ontology::HierarchyQueries;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::{Identified, TermId};
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::PhenotypicFeature;
use phenopackets::schema::v2::core::time_element::Element;
use phenopackets::schema::v2::core::{OntologyClass, TimeElement};
use regex::Regex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

struct PhenopacketLinter {
    phenotype_validator: PhenotypeValidator,
}

impl PhenopacketLinter {
    pub fn new(hpo: Arc<FullCsrOntology>) -> PhenopacketLinter {
        PhenopacketLinter {
            phenotype_validator: PhenotypeValidator::new(hpo),
        }
    }

    pub fn lint(&mut self, phenopacket: &mut Phenopacket, fix: bool) -> LintReport {
        let mut report = LintReport::new();

        CurieValidator::validate(phenopacket, &mut report);
        self.phenotype_validator.validate(phenopacket, &mut report);
        InterpretationValidator::validate(phenopacket, &mut report);

        if fix && report.has_violations() {
            let fix_res = self.fix(phenopacket, &report);
        }

        report
    }

    fn fix(&self, phenopacket: &mut Phenopacket, report: &LintReport) -> Result<(), LintingError> {
        let mut seen = HashSet::new();
        phenopacket.phenotypic_features.retain(|feature| {
            if let Some(f) = &feature.r#type {
                seen.insert(f.id.clone())
            } else {
                true
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ontology::traits::OntologyRegistry;
    use crate::test_utils::HPO;
    use rstest::*;
    use tempfile::TempDir;

    #[fixture]
    fn term_ancestry() -> Vec<TermId> {
        vec![
            "HP:0000448".parse().unwrap(), // scion
            "HP:0005105".parse().unwrap(),
            "HP:0000366".parse().unwrap(),
            "HP:0000271".parse().unwrap(), // progenitor
        ]
    }

    fn construct_linter() -> PhenopacketLinter {
        PhenopacketLinter::new(HPO.clone())
    }
}
