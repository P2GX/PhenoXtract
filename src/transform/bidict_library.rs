use crate::ontology::resource_references::ResourceRef;
use crate::ontology::traits::{BiDict, HasPrefixId};
use crate::utils::is_curie;
use phenopackets::schema::v2::core::OntologyClass;

#[derive(Debug, Default)]
pub struct BiDictLibrary {
    name: String,
    bidicts: Vec<Box<dyn BiDict>>,
}

impl BiDictLibrary {
    pub fn new(name: &str, bidicts: Vec<Box<dyn BiDict>>) -> Self {
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

    pub fn add_bidict(&mut self, bidict: Box<dyn BiDict>) {
        self.bidicts.push(bidict);
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_bidicts(&self) -> &Vec<Box<dyn BiDict>> {
        &self.bidicts
    }

    pub(crate) fn query_bidicts(&self, query: &str) -> Option<(OntologyClass, ResourceRef)> {
        for bidict in self.bidicts.iter() {
            if is_curie(query, Some(bidict.reference().prefix_id()), None) {
                if let Ok(label) = bidict.get_label(query) {
                    return Some((
                        OntologyClass {
                            id: query.to_string(),
                            label: label.to_string(),
                        },
                        bidict.reference().clone(),
                    ));
                }
            } else if let Ok(id) = bidict.get_id(query) {
                return Some((
                    OntologyClass {
                        id: id.to_string(),
                        label: query.to_string(),
                    },
                    bidict.reference().clone(),
                ));
            }
        }

        None
    }
}

impl PartialEq for BiDictLibrary {
    fn eq(&self, other: &Self) -> bool {
        let bi_dict_refs = self
            .bidicts
            .iter()
            .map(|bi| bi.reference())
            .collect::<Vec<_>>();
        let bi_dict_refs_other = self
            .bidicts
            .iter()
            .map(|bi| bi.reference())
            .collect::<Vec<_>>();

        self.name == other.name && bi_dict_refs == bi_dict_refs_other
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::loinc_client::LoincClient;
    use crate::test_suite::component_building::{
        build_test_hpo_bidict_library, build_test_mondo_bidict_library,
    };
    use crate::test_suite::phenopacket_component_generation::{
        default_phenotype_oc, default_qual_loinc,
    };
    use dotenvy::dotenv;
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
    fn test_query_bidicts_not_a_curie_fail() {
        dotenv().ok();
        let bidict_lib = BiDictLibrary::new("LOINC", vec![Box::new(LoincClient::default())]);

        let loinc_id = default_qual_loinc()
            .id
            .clone() // clone the String so we own it
            .split_once(':')
            .unwrap()
            .1
            .to_string();

        let result = bidict_lib.query_bidicts(loinc_id.as_str());
        assert!(result.is_none());
    }

    #[rstest]
    fn test_query_bidicts_invalid_query() {
        let result = build_test_mondo_bidict_library().query_bidicts("NonexistentTerm");

        assert!(result.is_none());
    }

    #[rstest]
    fn test_query_bidicts_on_empty_library() {
        let library = BiDictLibrary::empty_with_name("EmptyLib");
        let result = library.query_bidicts("AnyQuery");

        assert!(result.is_none());
    }

    #[rstest]
    fn test_query_bidicts_returns_correct_resource_ref() {
        let phenotype = default_phenotype_oc();
        let library = build_test_hpo_bidict_library();

        let expected_ref = library.get_bidicts()[0].reference();

        let result = library.query_bidicts(&phenotype.label).unwrap();

        assert_eq!(&result.1, expected_ref);
    }
}
