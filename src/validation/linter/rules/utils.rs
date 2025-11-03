use ontolius::ontology::HierarchyQueries;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::MinimalTerm;
use ontolius::term::simple::SimpleTerm;
use ontolius::{Identified, TermId};
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::{OntologyClass, PhenotypicFeature};
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

fn is_mergable_pf(phenotypic_features: &PhenotypicFeature) -> bool {
    phenotypic_features.onset.is_some()
        || phenotypic_features.modifiers.is_empty()
        || phenotypic_features.severity.is_none()
}

fn is_empty_pf(phenotypic_features: &PhenotypicFeature) -> bool {
    phenotypic_features.onset.is_none()
        && phenotypic_features.severity.is_none()
        && phenotypic_features.modifiers.is_empty()
        && phenotypic_features.description.is_empty()
        && phenotypic_features.evidence.is_empty()
        && phenotypic_features.resolution.is_none()
}

/// Finds all ancestor terms of a given scion term within a provided ancestry set.
///
/// This method filters the provided ancestry set to return only those terms that are
/// ancestors of the specified scion term, excluding the scion term itself from the results.
/// An ancestor is a term that is higher in the ontology hierarchy and has a path leading
/// down to the scion term.
///
/// # Arguments
///
/// * `ancestry` - A reference to a HashSet containing TermIds to search within
/// * `scion` - A reference to the TermId for which to find ancestors
///
/// # Returns
///
/// A HashSet<TermId> containing all terms from the ancestry set that are ancestors
/// of the scion term. The scion term itself is excluded from the results.
///
/// # Behaviour
///
/// Ancestry:
///
/// Abnormality of the musculoskeletal system ━┓
/// Abnormal musculoskeletal physiology        ┣━ These will be returned
/// Limb pain                                 ━┛
/// Lower limb pain -> Selected as scion
/// Foot pain
///
/// # Examples
/// ```ignore
/// ```rust
/// let ancestry_set: HashSet<TermId> = [term1, term2, term3, scion_term].iter().cloned().collect();
/// let ancestors = obj.find_ancestors(&ancestry_set, &scion_term);
/// // ancestors will contain only those terms from ancestry_set that are ancestors of scion_term
/// ```
pub(crate) fn find_ancestors(
    hpo: Arc<FullCsrOntology>,
    ancestry: &HashSet<TermId>,
    scion: &TermId,
) -> HashSet<TermId> {
    ancestry
        .iter()
        .filter(|term| *term != scion && hpo.is_ancestor_of(*term, scion))
        .cloned()
        .collect()
}

/// Finds all descendant terms of a given progenitor term within a provided ancestry set.
///
/// This method filters the provided ancestry set to return only those terms that are
/// descendants of the specified progenitor term, excluding the progenitor term itself
/// from the results. A descendant is a term that is lower in the ontology hierarchy
/// and can be reached by following paths down from the progenitor term.
///
/// # Arguments
///
/// * `ancestry` - A reference to a HashSet containing TermIds to search within
/// * `progenitor` - A reference to the TermId for which to find descendants
///
/// # Returns
///
/// A HashSet<TermId> containing all terms from the ancestry set that are descendants
/// of the progenitor term. The progenitor term itself is excluded from the results.
///
/// # Behaviour
///
/// Ancestry:
///
/// Abnormality of the musculoskeletal system
/// Abnormal musculoskeletal physiology -> Selected as progenitor
/// Limb pain       ━┓
/// Lower limb pain  ┣━ These will be returned
/// Foot pain       ━┛
///
/// # Examples
/// ```ignore
/// ```rust
/// let ancestry_set: HashSet<TermId> = [progenitor_term, term1, term2, term3].iter().cloned().collect();
/// let descendants = obj.find_descendents(&ancestry_set, &progenitor_term);
/// // descendants will contain only those terms from ancestry_set that are descendants of progenitor_term
/// ```
pub(crate) fn find_descendents(
    hpo: Arc<FullCsrOntology>,
    ancestry: &HashSet<TermId>,
    progenitor: &TermId,
) -> HashSet<TermId> {
    ancestry
        .iter()
        .filter(|term| *term != progenitor && hpo.is_descendant_of(*term, progenitor))
        .cloned()
        .collect()
}

pub(crate) fn partition_phenotypic_features(
    phenopacket: &Phenopacket,
) -> (HashSet<TermId>, HashSet<TermId>) {
    let mut observed = HashSet::new();
    let mut excluded = HashSet::new();

    for pf in &phenopacket.phenotypic_features {
        if let Some(feature_type) = &pf.r#type {
            if let Ok(term_id) = TermId::from_str(&feature_type.id) {
                if pf.excluded {
                    excluded.insert(term_id);
                } else {
                    observed.insert(term_id);
                }
            }
        }
    }

    (observed, excluded)
}

// Duplicates Same level  | Action
// Pure duplicates -> Remove
// excluded and included -> None
// As soon as there is an onset, severity or modifiers phenotypes can not be merged otherwise -> Merge

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HPO;
    use rstest::{fixture, rstest};
    use std::str::FromStr;

    #[fixture]
    fn term_ancestry() -> Vec<TermId> {
        vec![
            "HP:0000448".parse().unwrap(), // scion
            "HP:0005105".parse().unwrap(),
            "HP:0000366".parse().unwrap(),
            "HP:0000271".parse().unwrap(), // progenitor
        ]
    }

    #[rstest]
    fn test_find_ancestors(term_ancestry: Vec<TermId>) {
        let ancestors = find_ancestors(
            HPO.clone(),
            &term_ancestry.iter().cloned().collect(),
            &"HP:0005105".parse().unwrap(),
        );

        assert!(ancestors.contains(&TermId::from_str("HP:0000366").unwrap()));
        assert!(ancestors.contains(&TermId::from_str("HP:0000271").unwrap()));
    }

    #[rstest]
    fn test_find_descendents(term_ancestry: Vec<TermId>) {
        let ancestors = find_descendents(
            HPO.clone(),
            &term_ancestry.iter().cloned().collect(),
            &"HP:0005105".parse().unwrap(),
        );

        assert!(ancestors.contains(&TermId::from_str("HP:0000448").unwrap()));
    }
}

pub(crate) fn term_to_ontology_class(term: &SimpleTerm) -> OntologyClass {
    OntologyClass {
        id: term.identifier().to_string(),
        label: term.name().to_string(),
    }
}
