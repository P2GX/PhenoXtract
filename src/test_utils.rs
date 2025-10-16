#![allow(unused)]
use crate::ontology::ObolibraryOntologyRegistry;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::traits::OntologyRegistry;
use crate::ontology::utils::init_ontolius;
use once_cell::sync::Lazy;
use ontolius::ontology::csr::FullCsrOntology;
use std::sync::Arc;

pub(crate) static HPO: Lazy<Arc<FullCsrOntology>> = Lazy::new(|| {
    let hpo_registry = ObolibraryOntologyRegistry::default_hpo_registry().unwrap();
    let path = hpo_registry.register("2025-09-01").unwrap();
    init_ontolius(path).unwrap()
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
