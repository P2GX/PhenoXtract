use crate::ontology::resource_references::OntologyRef;
use ontolius::Identified;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::ontology::{MetadataAware, OntologyTerms};
use ontolius::term::{MinimalTerm, Synonymous};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct OntologyBiDict {
    pub ontology: OntologyRef,
    label_to_id: HashMap<String, String>,
    synonym_to_id: HashMap<String, String>,
    id_to_label: HashMap<String, String>,
}

impl OntologyBiDict {
    pub(crate) fn new(
        ontology: OntologyRef,
        label_to_id: HashMap<String, String>,
        synonym_to_id: HashMap<String, String>,
        id_to_label: HashMap<String, String>,
    ) -> OntologyBiDict {
        OntologyBiDict {
            ontology,
            label_to_id,
            synonym_to_id,
            id_to_label,
        }
    }

    /// Performs a case-insensitive search for an Ontology label, synonym, or ID.
    ///
    /// This method provides a unified interface to query the dictionary. It checks for
    /// a match in the following order:
    /// 1.  Official label -> Ontology ID
    /// 2.  Synonym name -> Ontology ID
    /// 3.  Ontology ID -> Official label
    ///
    /// The search is case-insensitive.
    ///
    /// # Parameters
    ///
    /// * `key`: A string slice representing the label name, synonym, or Ontology ID to look up.
    ///
    /// # Returns
    ///
    /// * `Some(&str)` containing the corresponding ID or label name if a match is found.
    /// * `None` if the input string does not match any known label, synonym, or ID.
    pub fn get(&self, key: &str) -> Option<&str> {
        let normalized_key = Self::normalize_key(key);

        if let Some(identifier) = self.label_to_id.get(&normalized_key) {
            return Some(identifier);
        }
        if let Some(identifier) = self.synonym_to_id.get(&normalized_key) {
            return Some(identifier);
        }
        if let Some(label) = self.id_to_label.get(&normalized_key) {
            return Some(label);
        }
        None
    }

    pub fn is_primary_label(&self, key: &str) -> bool {
        self.label_to_id.contains_key(&Self::normalize_key(key))
    }
    pub fn is_synonym(&self, key: &str) -> bool {
        self.synonym_to_id.contains_key(&Self::normalize_key(key))
    }
    pub fn is_id(&self, key: &str) -> bool {
        self.id_to_label.contains_key(&Self::normalize_key(key))
    }

    fn normalize_key(key: &str) -> String {
        key.trim().to_lowercase()
    }

    pub fn from_ontology(ontology: Arc<FullCsrOntology>, ontology_prefix: &str) -> Self {
        let map_size = ontology.len();
        let mut label_to_id: HashMap<String, String> = HashMap::with_capacity(map_size);
        let mut synonym_to_id: HashMap<String, String> = HashMap::with_capacity(map_size);
        let mut id_to_label: HashMap<String, String> = HashMap::with_capacity(map_size);

        for term in ontology.iter_terms() {
            if term.is_current() {
                label_to_id.insert(term.name().to_lowercase(), term.identifier().to_string());
                term.synonyms().iter().for_each(|syn| {
                    synonym_to_id.insert(syn.name.to_lowercase(), term.identifier().to_string());
                });
                id_to_label.insert(
                    term.identifier().to_string().to_lowercase(),
                    term.name().to_string(),
                );
            }
        }

        let ont_ref = OntologyRef::from(ontology_prefix).with_version(ontology.version());

        OntologyBiDict::new(ont_ref, label_to_id, synonym_to_id, id_to_label)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HPO;
    use rstest::rstest;

    #[rstest]
    fn test_hpo_bidict_get() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), OntologyRef::HPO_PREFIX);

        assert_eq!(hpo_dict.get("HP:0000256"), Some("Macrocephaly"));
    }

    #[rstest]
    fn test_hpo_bidict_get_id_by_label() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), OntologyRef::HPO_PREFIX);
        assert_eq!(hpo_dict.get("Macrocephaly"), Some("HP:0000256"));
    }

    #[rstest]
    fn test_hpo_bidict_get_id_by_synonym() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), OntologyRef::HPO_PREFIX);
        assert_eq!(hpo_dict.get("Big head"), Some("HP:0000256"));
    }

    #[rstest]
    fn test_hpo_bidict_chaining() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), OntologyRef::HPO_PREFIX);
        let hpo_id = hpo_dict.get("Big head");
        assert_eq!(hpo_dict.get(hpo_id.unwrap()), Some("Macrocephaly"));
    }
}
