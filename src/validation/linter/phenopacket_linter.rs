#![allow(dead_code)]
#![allow(unused)]
use crate::validation::linter::error::LintingError;
use crate::validation::linter::linting_report::LintReport;
use crate::validation::linter::rules::curie_format_rule::CurieFormatRule;
use crate::validation::linter::rules::phenotype_validator::PhenotypeValidator;
use crate::validation::linter::traits::{Lint, RuleCheck};
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
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

struct PhenopacketLinter {
    rules: Vec<Box<dyn RuleCheck>>,
}

impl Lint<Phenopacket> for PhenopacketLinter {
    fn lint(&mut self, phenopacket: Phenopacket, fix: bool) -> LintReport {
        let mut phenopacket = phenopacket.clone();
        let mut report = LintReport::new();

        for rule in &self.rules {
            rule.check(&mut phenopacket, &mut report);
        }

        if fix && report.has_violations() {
            let fix_res = self.fix(&mut phenopacket, &report);
            report.fixed_phenopacket = Some(phenopacket)
        }

        report
    }
}

impl Lint<PathBuf> for PhenopacketLinter {
    fn lint(&mut self, path: PathBuf, fix: bool) -> LintReport {
        let content = std::fs::read_to_string(path).expect("Failed to read file");
        let mut phenopacket: Phenopacket =
            serde_json::from_str(&content).expect("Failed to parse phenopacket");
        self.lint(phenopacket, fix)
    }
}

impl Lint<&[u8]> for PhenopacketLinter {
    fn lint(&mut self, bytes: &[u8], fix: bool) -> LintReport {
        let mut phenopacket: Phenopacket =
            serde_json::from_slice(bytes).expect("Failed to parse phenopacket");
        self.lint(phenopacket, fix)
    }
}

impl PhenopacketLinter {
    pub fn new(rules: Vec<Box<dyn RuleCheck>>) -> PhenopacketLinter {
        PhenopacketLinter { rules }
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
}
