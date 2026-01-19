use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::resource_references::ResourceRef;
use crate::ontology::traits::HasPrefixId;
use phenopackets::schema::v2::core::OntologyClass;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct BiDictLibrary {
    name: String,
    bidicts: Vec<Arc<OntologyBiDict>>,
}

impl BiDictLibrary {
    pub fn new(name: &str, bidicts: Vec<Arc<OntologyBiDict>>) -> Self {
        BiDictLibrary {
            name: name.to_string(),
            bidicts,
        }
    }

    pub fn empty_with_name(name: &str) -> Self {
        BiDictLibrary {
            name: name.to_string(),
            bidicts: vec![],
        }
    }

    pub fn add_bidict(&mut self, bidict: Arc<OntologyBiDict>) {
        self.bidicts.push(bidict);
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_bidicts(&self) -> &Vec<Arc<OntologyBiDict>> {
        &self.bidicts
    }

    pub fn get_bidict_prefixes(&self) -> Vec<&str> {
        self.bidicts
            .iter()
            .map(|bidict| bidict.ontology.prefix_id())
            .collect::<Vec<&str>>()
    }

    pub(crate) fn query_bidicts(&self, query: &str) -> Option<(OntologyClass, ResourceRef)> {
        for bidict in self.bidicts.iter() {
            if let Some(term) = bidict.get(query) {
                let corresponding_label_or_id = bidict.get(term).unwrap_or_else(|| {
                    panic!(
                        "Bidirectional dictionary '{}' inconsistency: missing reverse mapping",
                        bidict.ontology.clone().into_inner()
                    )
                });

                let (label, id) = if bidict.is_primary_label(term) {
                    (term, corresponding_label_or_id)
                } else {
                    (corresponding_label_or_id, term)
                };

                return Some((
                    OntologyClass {
                        id: id.to_string(),
                        label: label.to_string(),
                    },
                    bidict.ontology.clone().into_inner(),
                ));
            }
        }

        None
    }
}

impl PartialEq for BiDictLibrary {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.bidicts == other.bidicts
    }
}

#[cfg(test)]
mod tests {
    use crate::test_suite::component_building::{
        build_test_hpo_bidict_library, build_test_mondo_bidict_library,
        build_test_phenopacket_builder,
    };
    use crate::test_suite::phenopacket_component_generation::default_phenotype_oc;
    use pretty_assertions::assert_eq;
    use rstest::*;

    #[rstest]
    fn test_query_bidicts_with_valid_label() {
        let phenotype = default_phenotype_oc();
        let result = build_test_hpo_bidict_library()
            .query_bidicts(&phenotype.label)
            .unwrap();

        assert_eq!(result.0.label, phenotype.label);
        assert_eq!(result.0.id, phenotype.id);
    }

    #[rstest]
    fn test_query_bidicts_with_valid_id() {
        let phenotype = default_phenotype_oc();
        let result = build_test_hpo_bidict_library()
            .query_bidicts(&phenotype.id)
            .unwrap();

        assert_eq!(result.0.label, phenotype.label);
        assert_eq!(result.0.id, phenotype.id);
    }

    #[rstest]
    fn test_query_bidicts_invalid_query() {
        let result = build_test_mondo_bidict_library().query_bidicts("NonexistentTerm");

        assert!(result.is_none());
    }
}
