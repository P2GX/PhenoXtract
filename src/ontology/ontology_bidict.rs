use crate::ontology::error::BiDictError;
use crate::ontology::resource_references::OntologyRef;
use crate::ontology::traits::BIDict;
use ontolius::Identified;
use ontolius::ontology::csr::FullCsrOntology;
use ontolius::ontology::{MetadataAware, OntologyTerms};
use ontolius::term::{MinimalTerm, Synonymous};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default, PartialEq)]
pub struct OntologyBiDict {
    pub ontology: OntologyRef,
    label_to_id: HashMap<String, String>,
    synonym_to_id: HashMap<String, String>,
    id_to_label: HashMap<String, String>,
}

impl BIDict for OntologyBiDict {
    fn get(&self, id_or_label: &str) -> Result<&str, BiDictError> {
        let normalized_key = Self::normalize_key(id_or_label);

        if let Some(identifier) = self.label_to_id.get(&normalized_key) {
            return Ok(identifier);
        }
        if let Some(identifier) = self.synonym_to_id.get(&normalized_key) {
            return Ok(identifier);
        }
        if let Some(label) = self.id_to_label.get(&normalized_key) {
            return Ok(label);
        }
        Err(BiDictError::NotFound(normalized_key))
    }

    fn get_label(&self, id: &str) -> Result<&str, BiDictError> {
        let normalized_key = Self::normalize_key(id);

        if let Some(label) = self.id_to_label.get(&normalized_key) {
            return Ok(label);
        }
        Err(BiDictError::NotFound(normalized_key.to_string()))
    }

    fn get_id(&self, term: &str) -> Result<&str, BiDictError> {
        let normalized_key = Self::normalize_key(term);

        if let Some(identifier) = self.label_to_id.get(&normalized_key) {
            return Ok(identifier);
        }
        if let Some(identifier) = self.synonym_to_id.get(&normalized_key) {
            return Ok(identifier);
        }
        Err(BiDictError::NotFound(normalized_key))
    }
}

impl OntologyBiDict {
    pub(crate) fn new(
        ontology: OntologyRef,
        label_to_id: HashMap<String, String>,
        synonym_to_id: HashMap<String, String>,
        id_to_label: HashMap<String, String>,
    ) -> OntologyBiDict {
        let label_to_id_lower: HashMap<String, String> = label_to_id
            .into_iter()
            .map(|(key, value)| (key.to_lowercase(), value))
            .collect();

        let synonym_to_id_lower: HashMap<String, String> = synonym_to_id
            .into_iter()
            .map(|(key, value)| (key.to_lowercase(), value))
            .collect();

        let id_to_label_lower: HashMap<String, String> = id_to_label
            .into_iter()
            .map(|(key, value)| (key.to_lowercase(), value))
            .collect();

        OntologyBiDict {
            ontology,
            label_to_id: label_to_id_lower,
            synonym_to_id: synonym_to_id_lower,
            id_to_label: id_to_label_lower,
        }
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
            let prefix = term.identifier().prefix().to_string().to_lowercase();
            if term.is_current() && prefix == ontology_prefix.to_lowercase() {
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
    use crate::test_suite::ontology_mocking::HPO;
    use rstest::rstest;

    #[rstest]
    fn test_hpo_bidict_get() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), OntologyRef::HPO_PREFIX);

        assert_eq!(hpo_dict.get("HP:0000639").unwrap(), "Nystagmus".to_string());
    }

    #[rstest]
    fn test_hpo_bidict_get_id_by_label() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), OntologyRef::HPO_PREFIX);
        assert_eq!(hpo_dict.get("Nystagmus").unwrap(), "HP:0000639".to_string());
    }

    #[rstest]
    fn test_hpo_bidict_get_id_by_synonym() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), OntologyRef::HPO_PREFIX);
        assert_eq!(
            hpo_dict.get("contact with nickel").unwrap(),
            "HP:4000120".to_string()
        );
    }

    #[rstest]
    fn test_hpo_bidict_chaining() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), OntologyRef::HPO_PREFIX);
        let hpo_id = hpo_dict.get("contact with nickel").unwrap();
        assert_eq!(
            hpo_dict.get(&hpo_id).unwrap(),
            "Triggered by nickel".to_string()
        );
    }
}
