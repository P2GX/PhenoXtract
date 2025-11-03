use crate::validation::linter::enums::{FixAction, LintingViolations};
use phenopackets::schema::v2::Phenopacket;

#[derive(Clone, Debug)]
pub(crate) struct LintReportInfo {
    violation: LintingViolations,
    fix: Option<FixAction>,
}

impl LintReportInfo {
    pub(crate) fn new(violation: LintingViolations, fix: Option<FixAction>) -> Self {
        Self { violation, fix }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct LintReport {
    pub fixed_phenopacket: Option<Phenopacket>,
    pub report_info: Vec<LintReportInfo>,
}

impl LintReport {
    pub fn new() -> LintReport {
        LintReport {
            fixed_phenopacket: None,
            report_info: Vec::new(),
        }
    }

    pub fn into_violations(self) -> Vec<LintingViolations> {
        self.report_info
            .iter()
            .map(|i| i.violation.clone())
            .collect()
    }

    pub fn push_violation(&mut self, violation: LintingViolations) {
        self.report_info.push(LintReportInfo::new(violation, None));
    }

    pub fn push_info(&mut self, info: LintReportInfo) {
        self.report_info.push(info);
    }

    pub fn has_violations(&self) -> bool {
        !self.report_info.is_empty()
    }
}
