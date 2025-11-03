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
use pretty_assertions::assert_eq;
use prost_types::Timestamp;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

pub(crate) static ONTOLOGY_FACTORY: Lazy<Arc<Mutex<CachedOntologyFactory>>> =
    Lazy::new(|| Arc::new(Mutex::new(CachedOntologyFactory::default())));

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
pub(crate) static HPO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::hp_with_version("2025-09-01"));
pub(crate) static GENO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::geno_with_version("2025-07-25"));
pub(crate) static MONDO_REF: Lazy<OntologyRef> =
    Lazy::new(|| OntologyRef::mondo_with_version("2025-10-07"));
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

pub(crate) fn assert_phenopackets(actual: &mut Phenopacket, expected: &mut Phenopacket) {
    if let Some(meta) = &mut actual.meta_data {
        meta.created = None;
    }
    if let Some(meta) = &mut expected.meta_data {
        meta.created = None;
    }
    assert_eq!(actual, expected);
}
