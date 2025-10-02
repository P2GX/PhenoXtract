use ontolius::Identified;
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::{MinimalTerm, Synonymous};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct HPOBiDict {
    term_to_id: HashMap<String, String>,
    synonym_to_id: HashMap<String, String>,
    id_to_term: HashMap<String, String>,
}

impl HPOBiDict {
    /// Creates a new `HPOBiDict` by processing an HPO ontology.
    ///
    /// This constructor iterates through all terms and synonyms in the provided
    /// `FullCsrOntology`, populating the internal lookup maps. All keys (term names,
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
        let mut term_to_id: HashMap<String, String> = HashMap::new();
        let mut synonym_to_id: HashMap<String, String> = HashMap::new();
        let mut id_to_term: HashMap<String, String> = HashMap::new();

        hpo.iter_terms().for_each(|term| {
            term_to_id.insert(term.name().to_lowercase(), term.identifier().to_string());
            term.synonyms().iter().for_each(|syn| {
                synonym_to_id.insert(syn.name.to_lowercase(), term.identifier().to_string());
            });
            id_to_term.insert(
                term.identifier().to_string().to_lowercase(),
                term.name().to_string(),
            );
        });

        HPOBiDict {
            term_to_id,
            synonym_to_id,
            id_to_term,
        }
    }
    /// Performs a case-insensitive search for an HPO term, synonym, or ID.
    ///
    /// This method provides a unified interface to query the dictionary. It checks for
    /// a match in the following order:
    /// 1.  Official term name -> HPO ID
    /// 2.  Synonym name -> HPO ID
    /// 3.  HPO ID -> Official term name
    ///
    /// The search is case-insensitive.
    ///
    /// # Parameters
    ///
    /// * `key`: A string slice representing the term name, synonym, or HPO ID to look up.
    ///
    /// # Returns
    ///
    /// * `Some(&str)` containing the corresponding ID or term name if a match is found.
    /// * `None` if the input string does not match any known term, synonym, or ID.
    pub fn get(&self, key: &str) -> Option<&str> {
        let lowered = key.trim().to_lowercase();

        if let Some(primary_term) = self.term_to_id.get(&lowered) {
            return Some(primary_term);
        }
        if let Some(synonym) = self.synonym_to_id.get(&lowered) {
            return Some(synonym);
        }
        if let Some(id_to_term) = self.id_to_term.get(&lowered) {
            return Some(id_to_term);
        }
        None
    }

    pub fn is_primary_term(&self, term: &str) -> bool {
        self.term_to_id.contains_key(&term.to_lowercase())
    }
    pub fn is_synonym(&self, term: &str) -> bool {
        self.synonym_to_id.contains_key(&term.to_lowercase())
    }
    pub fn is_id(&self, term: &str) -> bool {
        self.id_to_term.contains_key(&term.to_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::HPO;
    use rstest::rstest;

    #[rstest]
    fn test_hpo_bidict_get_term() {
        let hpo_dict = HPOBiDict::new(HPO.clone());

        assert_eq!(hpo_dict.get("HP:0000256"), Some("Macrocephaly"));
    }

    #[rstest]
    fn test_hpo_bidict_get_id_by_term() {
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
