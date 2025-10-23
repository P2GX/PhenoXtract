#![allow(unused)]
use crate::ontology::CachedOntologyFactory;
use crate::ontology::enums::OntologyRef;
use crate::ontology::ontology_bidict::OntologyBiDict;
use once_cell::sync::Lazy;
use ontolius::ontology::csr::FullCsrOntology;
use std::sync::Arc;

pub(crate) static HPO: Lazy<Arc<FullCsrOntology>> = Lazy::new(|| {
    let mut factory = CachedOntologyFactory::default();
    factory
        .build_ontology(&OntologyRef::Hpo(Some("2025-09-01".to_string())), None)
        .unwrap()
});

pub(crate) static HPO_DICT: Lazy<Arc<OntologyBiDict>> = Lazy::new(|| Arc::new(HPO.clone().into()));

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
