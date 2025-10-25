#![allow(unused)]

use crate::ontology::CachedOntologyFactory;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::resource_references::OntologyRef;
use once_cell::sync::Lazy;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v1::core::Individual;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::{
    OntologyClass, PhenotypicFeature, Resource, TimeElement, Update,
};
use prost_types::Timestamp;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

pub(crate) static ONTOLOGY_FACTORY: Lazy<Arc<Mutex<CachedOntologyFactory>>> =
    Lazy::new(|| Arc::new(Mutex::new(CachedOntologyFactory::default())));

pub(crate) static HPO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::hp(Some("2025-09-01".to_string())));
pub(crate) static GENO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::mondo(Some("2025-10-07".to_string())));
pub(crate) static MONDO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::geno(Some("2025-07-25".to_string())));
use validator::ValidateRequired;

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
