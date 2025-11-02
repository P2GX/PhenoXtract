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
}

/// Represents possible fixing actions that can be applied to phenopackets.
///
/// This enum defines the set of operations available for correcting or modifying phenopacket data.
///
/// # Variants
///
/// * `Merge` - Combines multiple elements into a single element.
///   Used to consolidate duplicate or related data.
///
/// * `Move` - Relocates an element from one position to another within
///   the phenopacket.
///
/// * `Remove` - Deletes an element from the phenopacket. Applied when
///   elements are invalid, redundant, or don't conform to the expected format.
///
/// * `Duplicate` - Creates a copy of an existing element. Used when
///   the same data needs to appear in multiple locations within the phenopacket.
#[derive(Clone, Debug)]
pub enum FixAction {
    Merge,
    Move,
    Remove,
    Duplicate,
}
