use crate::ontology::CachedOntologyFactory;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::test_suite::resource_references::{HPO_REF, MONDO_REF, PATO_REF, UO_REF};
use crate::test_suite::utils::test_ontology_path;
use once_cell::sync::Lazy;
use ontolius::ontology::csr::FullCsrOntology;
use ontology_registry::enums::{FileType, Version};
use ontology_registry::error::OntologyRegistryError;
use ontology_registry::traits::OntologyRegistry;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub(crate) static ONTOLOGY_FACTORY: Lazy<Arc<Mutex<CachedOntologyFactory>>> = Lazy::new(|| {
    Arc::new(Mutex::new(CachedOntologyFactory::new(Box::new(
        MockOntologyRegistry::default(),
    ))))
});
pub(crate) static MONDO_BIDICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    let mock_mondo_label_to_id: HashMap<String, String> = HashMap::from_iter([
        (
            "platelet signal processing defect".to_string(),
            "MONDO:0008258".to_string(),
        ),
        (
            "heart defects-limb shortening syndrome".to_string(),
            "MONDO:0008917".to_string(),
        ),
        (
            "macular degeneration, age-related, 3".to_string(),
            "MONDO:0012145".to_string(),
        ),
        (
            "spondylocostal dysostosis".to_string(),
            "MONDO:0000359".to_string(),
        ),
        (
            "inflammatory diarrhea".to_string(),
            "MONDO:0000252".to_string(),
        ),
    ]);

    let mock_mondo_id_to_label: HashMap<String, String> = mock_mondo_label_to_id
        .iter()
        .map(|(label, id)| (id.to_string(), label.to_string()))
        .collect();

    Arc::new(OntologyBiDict::new(
        MONDO_REF.clone(),
        mock_mondo_label_to_id,
        HashMap::new(),
        mock_mondo_id_to_label,
    ))
});
pub(crate) static HPO: Lazy<Arc<FullCsrOntology>> = Lazy::new(|| {
    ONTOLOGY_FACTORY
        .lock()
        .unwrap()
        .build_ontology(&HPO_REF, None)
        .unwrap()
});
pub(crate) static HPO_DICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    ONTOLOGY_FACTORY
        .lock()
        .unwrap()
        .build_bidict(&HPO_REF.clone(), None)
        .unwrap()
});

pub(crate) static UO_DICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    ONTOLOGY_FACTORY
        .lock()
        .unwrap()
        .build_bidict(&UO_REF.clone(), None)
        .unwrap()
});

pub(crate) static PATO_DICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| {
    ONTOLOGY_FACTORY
        .lock()
        .unwrap()
        .build_bidict(&PATO_REF.clone(), None)
        .unwrap()
});

#[derive(Debug)]
pub(crate) struct MockOntologyRegistry {
    registry_path: PathBuf,
}

impl Default for MockOntologyRegistry {
    fn default() -> Self {
        Self {
            registry_path: test_ontology_path(),
        }
    }
}

impl OntologyRegistry<PathBuf> for MockOntologyRegistry {
    fn register(
        &self,
        ontology_id: &str,
        version: &Version,
        file_type: &FileType,
    ) -> Result<PathBuf, OntologyRegistryError> {
        if version.to_string() == Version::Latest.to_string() {
            let entries =
                fs::read_dir(self.registry_path.clone()).expect("Failed to read registry path");

            for entry in entries {
                let entry = entry.expect("Failed to read entry");
                let path = entry.path();
                let file_name = path
                    .file_name()
                    .expect("No, filename")
                    .to_str()
                    .expect("Conversion error");

                let found_ontology_id = file_name
                    .split("_")
                    .last()
                    .unwrap()
                    .split(".")
                    .next()
                    .unwrap()
                    .to_string();
                if found_ontology_id == ontology_id {
                    return Ok(path);
                }
            }
        }

        let file_name = format!("{version}_{ontology_id}{}", file_type.as_file_ending());
        let file_path = self.registry_path.join(file_name);

        if !file_path.exists() {
            return Err(OntologyRegistryError::UnableToRegister {
                reason: format!(
                    "Ontology not found at {}, when mocking OntologyRegistry",
                    file_path.to_str().unwrap()
                ),
            });
        }

        Ok(file_path)
    }

    #[allow(unused)]
    fn unregister(
        &self,
        ontology_id: &str,
        version: &Version,
        file_type: &FileType,
    ) -> Result<(), OntologyRegistryError> {
        todo!()
    }

    #[allow(unused)]
    fn get(&self, ontology_id: &str, version: &Version, file_type: &FileType) -> Option<PathBuf> {
        todo!()
    }
    #[allow(unused)]
    fn list(&self) -> Vec<String> {
        todo!()
    }
}
