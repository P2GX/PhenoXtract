#![allow(unused)]
use crate::ontology::CachedOntologyFactory;
use crate::ontology::enums::OntologyRef;
use crate::ontology::ontology_bidict::OntologyBiDict;
use once_cell::sync::Lazy;
use ontolius::ontology::csr::FullCsrOntology;
use std::sync::{Arc, Mutex};

pub(crate) static ONTOLOGY_FACTORY: Lazy<Arc<Mutex<CachedOntologyFactory>>> =
    Lazy::new(|| Arc::new(Mutex::new(CachedOntologyFactory::default())));

pub(crate) static HPO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::Hpo(Some("2025-09-01".to_string())));
pub(crate) static GENO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::Mondo(Some("2025-10-07".to_string())));
pub(crate) static MONDO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::Geno(Some("2025-07-25".to_string())));

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

#[macro_export]
macro_rules! skip_in_ci {
    ($test_name:expr) => {
        if std::env::var("CI").is_ok() {
            println!("Skipping {} in CI environment", $test_name);
            return;
        }
    };
    () => {
        if std::env::var("CI").is_ok() {
            println!("Skipping {} in CI environment", module_path!());
            return;
        }
    };
}
