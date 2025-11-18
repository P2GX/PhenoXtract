#![allow(unused)]

use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::resource_references::OntologyRef;
use crate::ontology::{CachedOntologyFactory, HGNCClient};
use once_cell::sync::Lazy;
use ontolius::ontology::csr::FullCsrOntology;
use phenopackets::schema::v1::core::Individual;
use phenopackets::schema::v2::Phenopacket;
use phenopackets::schema::v2::core::{
    OntologyClass, PhenotypicFeature, Resource, TimeElement, Update,
};
use pretty_assertions::assert_eq;
use ratelimit::Ratelimiter;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

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
use crate::ontology::traits::HasPrefixId;
use crate::transform::PhenopacketBuilder;
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

pub(crate) static DATA_SOURCES_CONFIG: &[u8] = br#"
data_sources:
  - type: "csv"
    source: "./data/example.csv"
    separator: ","
    context:
      name: "TestTable"
      context:
        - identifier: "patient_id"
          header_context: subject_id
          data_context: hpo_label_or_id
          fill_missing: "Zollinger-Ellison syndrome"
          alias_map:
            hash_map:
              "null": "Primary peritoneal carcinoma"
              "M": "Male"
              "102": "High quantity"
              "169.5": "Very high quantity"
              "true": "smoker"
            output_dtype: String
          building_block_id: "block_1"
    extraction_config:
      name: "Sheet1"
      has_headers: true
      patients_are_rows: true

  - type: "excel"
    source: "./data/example.excel"
    contexts:
      - name: "Sheet1"
        context:
          - identifier: "lab_result_.*"
            header_context: subject_id
            data_context: hpo_label_or_id
            fill_missing: "Zollinger-Ellison syndrome"
            alias_map:
              hash_map:
                "neoplasma": "4"
                "height": "1.85"
              output_dtype: Float64
      - name: "Sheet2"
        context:
          - identifier:
              - "Col_1"
              - "Col_2"
              - "Col_3"
            header_context: subject_id
            data_context: hpo_label_or_id
            fill_missing: "Zollinger-Ellison syndrome"
            alias_map:
              hash_map:
                "smoker": "true"
              output_dtype: Boolean
    extraction_configs:
      - name: "Sheet1"
        has_headers: true
        patients_are_rows: true
      - name: "Sheet2"
        has_headers: true
        patients_are_rows: true
"#;

pub(crate) static PIPELINE_CONFIG: &[u8] = br#"
pipeline:
  transform_strategies:
    - "alias_map"
    - "multi_hpo_col_expansion"
  loader: "file_system"
  meta_data:
    created_by: Rouven Reuter
    submitted_by: Magnus Knut Hansen
    cohort_name: "Arkham Asylum 2025"
    hp_ref:
      version: "2025-09-01"
      prefix_id: "hp"
"#;

/// Alternative: Get the combined config as bytes
pub(crate) fn get_full_config_bytes() -> Vec<u8> {
    let data_sources =
        std::str::from_utf8(DATA_SOURCES_CONFIG).expect("Invalid UTF-8 in DATA_SOURCES_CONFIG");
    let pipeline = std::str::from_utf8(PIPELINE_CONFIG).expect("Invalid UTF-8 in PIPELINE_CONFIG");

    format!("{}\n{}", data_sources.trim(), pipeline.trim()).into_bytes()
}

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

fn build_test_dicts() -> HashMap<String, Arc<OntologyBiDict>> {
    let hpo_dict = ONTOLOGY_FACTORY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .build_bidict(&HPO_REF.clone(), None)
        .unwrap();

    let geno_dict = ONTOLOGY_FACTORY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .build_bidict(&GENO_REF.clone(), None)
        .unwrap();

    HashMap::from_iter(vec![
        (hpo_dict.ontology.prefix_id().to_string(), hpo_dict),
        (
            MONDO_BIDICT.ontology.prefix_id().to_string(),
            MONDO_BIDICT.clone(),
        ),
        (geno_dict.ontology.prefix_id().to_string(), geno_dict),
    ])
}

pub(crate) fn build_hgnc_test_client(temp_dir: &Path) -> HGNCClient {
    println!("Building HGNC test client at {}", temp_dir.display());
    let rate_limiter = Ratelimiter::builder(10, Duration::from_secs(1))
        .max_tokens(10)
        .build()
        .expect("Building rate limiter failed");

    HGNCClient::new(
        rate_limiter,
        temp_dir.to_path_buf().join("hgnc_test_cache"),
        "https://rest.genenames.org/".to_string(),
    )
    .unwrap()
}

pub(crate) fn build_test_phenopacket_builder(temp_dir: &Path) -> PhenopacketBuilder {
    let hgnc_client = build_hgnc_test_client(temp_dir);
    PhenopacketBuilder::new(build_test_dicts(), hgnc_client)
}
