use ontolius::Identified;
use ontolius::ontology::OntologyTerms;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::term::{MinimalTerm, Synonymous};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct HPOBiDict {
    term_to_id: HashMap<String, String>,
    synonym_to_id: HashMap<String, String>,
    id_to_term: HashMap<String, String>,
}

impl HPOBiDict {
    pub(crate) fn new(hpo: Arc<FullCsrOntology>) -> Self {
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

    pub fn get(&self, term: &str) -> Option<&str> {
        let lowered = term.to_lowercase();

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
        assert_eq!(hpo_dict.get("HP:0000256"), Some("Macrocephaly"));
    }
}
