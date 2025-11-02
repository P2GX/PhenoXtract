use phenopackets::schema::v2::core::OntologyClass;

#[derive(Clone, Debug)]
pub enum LintingViolations {
    NonModifier(OntologyClass),
    NonPhenotypicFeature(OntologyClass),
    NonOnset(OntologyClass),
    NonSeverity(OntologyClass),
    NotACurieID(String),
}

#[derive(Clone, Debug)]
pub(crate) struct LintReport {
    report_info: Vec<LintingViolations>,
}

impl LintReport {
    pub fn new() -> LintReport {
        LintReport {
            report_info: Vec::new(),
        }
    }

    pub fn save() {
        todo!("Implement saving the report as a json")
    }

    pub fn into_violations(self) -> Vec<LintingViolations> {
        self.report_info
    }

    pub fn insert_violation(&mut self, violation: LintingViolations) {
        self.report_info.push(violation);
    }

    pub fn has_violations(&self) -> bool {
        !self.report_info.is_empty()
    }
}
