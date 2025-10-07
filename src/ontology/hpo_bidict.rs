use ontolius::Identified;
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::{MinimalTerm, Synonymous};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct HPOBiDict {
    label_to_id: HashMap<String, String>,
    synonym_to_id: HashMap<String, String>,
    id_to_label: HashMap<String, String>,
}

impl HPOBiDict {
    /// Creates a new `HPOBiDict` by processing an HPO ontology.
    ///
    /// This constructor iterates through all labels and synonyms in the provided
    /// `FullCsrOntology`, populating the internal lookup maps. All keys (label names,
    /// synonyms, and IDs) are stored in lowercase to enable case-insensitive searching.
    ///
    /// This operation can be computationally intensive, as it builds several hashmaps
    /// from the entire ontology. It is intended to be called once during initialization.
    ///
    /// # Parameters
    ///
    /// * `hpo`: An `Arc<FullCsrOntology>` which serves as the data source for
    ///   creating the bidirectional mappings.
    ///
    /// # Returns
    ///
    /// A new, fully populated `HPOBiDict` instance.
    pub fn new(hpo: Arc<FullCsrOntology>) -> Self {
        let mut label_to_id: HashMap<String, String> = HashMap::new();
        let mut synonym_to_id: HashMap<String, String> = HashMap::new();
        let mut id_to_label: HashMap<String, String> = HashMap::new();

        hpo.iter_terms().for_each(|term| {
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
        });

        HPOBiDict {
            label_to_id,
            synonym_to_id,
            id_to_label,
        }
    }
    /// Performs a case-insensitive search for an HPO label, synonym, or ID.
    ///
    /// This method provides a unified interface to query the dictionary. It checks for
    /// a match in the following order:
    /// 1.  Official label -> HPO ID
    /// 2.  Synonym name -> HPO ID
    /// 3.  HPO ID -> Official label
    ///
    /// The search is case-insensitive.
    ///
    /// # Parameters
    ///
    /// * `key`: A string slice representing the label name, synonym, or HPO ID to look up.
    ///
    /// # Returns
    ///
    /// * `Some(&str)` containing the corresponding ID or label name if a match is found.
    /// * `None` if the input string does not match any known label, synonym, or ID.
    pub fn get(&self, key: &str) -> Option<&str> {
        let lowered = key.trim().to_lowercase();

        if let Some(identifier) = self.label_to_id.get(&lowered) {
            return Some(identifier);
        }
        if let Some(identifier) = self.synonym_to_id.get(&lowered) {
            return Some(identifier);
        }
        if let Some(label) = self.id_to_label.get(&lowered) {
            return Some(label);
        }
        None
    }

    pub fn is_primary_label(&self, key: &str) -> bool {
        self.label_to_id.contains_key(&key.trim().to_lowercase())
    }
    pub fn is_synonym(&self, key: &str) -> bool {
        self.synonym_to_id.contains_key(&key.trim().to_lowercase())
    }
    pub fn is_id(&self, key: &str) -> bool {
        self.id_to_label.contains_key(&key.trim().to_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HPO;
    use rstest::rstest;

    #[rstest]
    fn test_hpo_bidict_get() {
        let hpo_dict = HPOBiDict::new(HPO.clone());

        assert_eq!(hpo_dict.get("HP:0000256"), Some("Macrocephaly"));
    }

    #[rstest]
    fn test_hpo_bidict_get_id_by_label() {
        let hpo_dict = HPOBiDict::new(HPO.clone());
        assert_eq!(hpo_dict.get("Macrocephaly"), Some("HP:0000256"));
    }

    #[rstest]
    fn test_hpo_bidict_get_id_by_synonym() {
        let hpo_dict = HPOBiDict::new(HPO.clone());
        assert_eq!(hpo_dict.get("Big head"), Some("HP:0000256"));
    }

    #[rstest]
    fn test_hpo_bidict_chaining() {
        let hpo_dict = HPOBiDict::new(HPO.clone());
        let hpo_id = hpo_dict.get("Big head");
        assert_eq!(hpo_dict.get(hpo_id.unwrap()), Some("Macrocephaly"));
    }
}
