use crate::validation::linter::linting_report::LintReport;
use phenopackets::schema::v2::Phenopacket;

pub trait RuleCheck {
    fn check(&self, phenopacket: &Phenopacket, report: &mut LintReport);
    fn rule_id(&self) -> &'static str;
}

pub(crate) trait Lint<T> {
    fn lint(&mut self, input: T, fix: bool) -> LintReport;
}
