use ontolius::TermId;
use phenopackets::schema::v2::core::OntologyClass;
use phenopackets::schema::v2::core::PhenotypicFeature;

#[derive(Clone, Debug)]
pub enum LintingViolations {
    NonModifier(OntologyClass),
    NonPhenotypicFeature(OntologyClass),
    NonOnset(OntologyClass),
    NonSeverity(OntologyClass),
    NotACurieID(OntologyClass),
    DiseaseConsistency(OntologyClass),
    DuplicatePhenotype(Box<PhenotypicFeature>),
    ObservedAncestor {
        scion: OntologyClass,
        ancestors: Vec<OntologyClass>,
    },
    ExcludedDescendents {
        progenitor: OntologyClass,
        descendents: Vec<OntologyClass>,
    },
}

#[derive(Clone, Debug)]
pub enum FixAction {
    Add {
        at: String,
        value: String,
    },
    Remove {
        at: String,
    },
    ///TODO: Unfolds to remove and add
    Move {
        from: String,
        to: String,
    },
    ///TODO: Converts to add
    Duplicate {
        from: String,
        to: String,
    },
}
