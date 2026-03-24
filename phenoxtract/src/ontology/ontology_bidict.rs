use crate::ontology::error::BiDictError;
use crate::ontology::resource_references::ResourceRef;
use crate::ontology::traits::{BiDict, HasPrefixId, OntologyLike};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default, PartialEq)]
pub struct OntologyBiDict {
    ontology: ResourceRef,
    label_to_id: HashMap<String, String>,
    synonym_to_id: HashMap<String, String>,
    id_to_label: HashMap<String, String>,
}

impl BiDict for OntologyBiDict {
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

    fn reference(&self) -> &ResourceRef {
        &self.ontology
    }
}

impl BiDict for Arc<OntologyBiDict> {
    fn get(&self, id_or_label: &str) -> Result<&str, BiDictError> {
        self.as_ref().get(id_or_label)
    }

    fn get_label(&self, id: &str) -> Result<&str, BiDictError> {
        self.as_ref().get_label(id)
    }

    fn get_id(&self, term: &str) -> Result<&str, BiDictError> {
        self.as_ref().get_id(term)
    }

    fn reference(&self) -> &ResourceRef {
        self.as_ref().reference()
    }
}

impl OntologyBiDict {
    pub(crate) fn new(
        ontology: ResourceRef,
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

    pub fn from_ontology(ontology: Arc<dyn OntologyLike>, ontology_ref: &ResourceRef) -> Self {
        let ontology_prefix = ontology_ref.prefix_id().to_string();
        let map_size = ontology.ontology_len(ontology_prefix.clone());
        let mut label_to_id: HashMap<String, String> = HashMap::with_capacity(map_size);
        let mut synonym_to_id: HashMap<String, String> = HashMap::with_capacity(map_size);
        let mut id_to_label: HashMap<String, String> = HashMap::with_capacity(map_size);

        for term in ontology.iter_ontology_terms(ontology_prefix) {
            label_to_id.insert(term.label().to_lowercase(), term.ontology_id().to_string());
            term.iter_synonyms().for_each(|syn| {
                synonym_to_id.insert(
                    syn.syn_name().to_lowercase(),
                    term.ontology_id().to_string(),
                );
            });
            id_to_label.insert(
                term.ontology_id().to_string().to_lowercase(),
                term.label().to_string(),
            );
        }

        OntologyBiDict::new(
            ontology_ref.clone(),
            label_to_id,
            synonym_to_id,
            id_to_label,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ontology::CachedOntologyFactory;
    use crate::ontology::traits::HasVersion;
    use crate::test_suite::mocks::MockOntologyRegistry;
    use crate::test_suite::ontology_mocking::HPO;
    use crate::test_suite::phenopacket_component_generation::default_unit_oc;
    use crate::test_suite::resource_references::{HPO_REF, UO_REF};
    use ontology_registry::{FileType, OntologyRegistration, Version};
    use rstest::rstest;
    use std::io::BufReader;

    #[rstest]
    fn test_hpo_bidict_get() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), &HPO_REF);

        assert_eq!(hpo_dict.get("HP:0000639").unwrap(), "Nystagmus".to_string());
    }

    #[rstest]
    fn test_uo_obodoc_bidict_get() {
        let registry = MockOntologyRegistry::default();
        let ontology_path = registry
            .register(
                UO_REF.prefix_id().to_string(),
                Version::Declared(UO_REF.version().to_string()),
                FileType::Obo,
            )
            .unwrap();
        let mut reader = BufReader::new(ontology_path);
        let uo_obodoc = Arc::new(fastobo::from_reader(&mut reader).unwrap());
        let pato_dict = OntologyBiDict::from_ontology(uo_obodoc, &UO_REF);

        assert_eq!(
            pato_dict.get(&default_unit_oc().id).unwrap(),
            default_unit_oc().label
        );
    }

    #[rstest]
    fn test_hpo_bidict_get_id_by_label() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), &HPO_REF);
        assert_eq!(hpo_dict.get("Nystagmus").unwrap(), "HP:0000639".to_string());
    }

    #[rstest]
    fn test_hpo_bidict_get_id_by_synonym() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), &HPO_REF);
        assert_eq!(
            hpo_dict.get("contact with nickel").unwrap(),
            "HP:4000120".to_string()
        );
    }

    #[rstest]
    fn test_hpo_bidict_chaining() {
        let hpo_dict = OntologyBiDict::from_ontology(HPO.clone(), &HPO_REF);
        let hpo_id = hpo_dict.get("contact with nickel").unwrap();
        assert_eq!(
            hpo_dict.get(hpo_id).unwrap(),
            "Triggered by nickel".to_string()
        );
    }
}
