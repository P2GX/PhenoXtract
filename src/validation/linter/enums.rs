use phenopackets::schema::v2::core::OntologyClass;

#[derive(Clone, Debug)]
pub enum LintingViolations {
    NonModifier(OntologyClass),
    NonPhenotypicFeature(OntologyClass),
    NonOnset(OntologyClass),
    NonSeverity(OntologyClass),
    NotACurieID(OntologyClass),
    DiseaseConsistency(OntologyClass),
}

#[derive(Clone, Debug)]
pub enum FixAction {
    Merge,
    Remove,
    Add,
}
