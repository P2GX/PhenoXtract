use crate::validation::linter::linting_report::LintReport;
use phenopackets::schema::v2::Phenopacket;
use serde::{Deserialize, Serialize};

pub trait ValidatePhenopacket {
    fn validate(&self, phenopacket: &Phenopacket, report: &mut LintReport);
}

pub(crate) trait Lint<T> {
    fn lint(&mut self, input: T, fix: bool) -> LintReport;
}
