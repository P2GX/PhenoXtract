use crate::config::MetaData;
use crate::ontology::loinc_client::LoincClient;
use crate::test_suite::config::PIPELINE_CONFIG;
use crate::test_suite::ontology_mocking::{MONDO_BIDICT, ONTOLOGY_FACTORY, PATO_DICT, UO_DICT};
use crate::test_suite::resource_references::HPO_REF;
use crate::transform::PhenopacketBuilder;
use crate::transform::bidict_library::BiDictLibrary;
use config::{Config, File, FileFormat};
use dotenvy::dotenv;
use pivot::hgnc::{CachedHGNCClient, HGNCClient};
use pivot::hgvs::{CachedHGVSClient, HGVSClient};
use std::path::Path;

pub(crate) fn build_test_hpo_bidict_library() -> BiDictLibrary {
    let hpo_bidict = ONTOLOGY_FACTORY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .build_bidict(&HPO_REF.clone(), None)
        .unwrap();

    BiDictLibrary::new("HPO", vec![Box::new(hpo_bidict)])
}

pub(crate) fn build_test_mondo_bidict_library() -> BiDictLibrary {
    BiDictLibrary::new("MONDO", vec![Box::new(MONDO_BIDICT.clone())])
}

pub(crate) fn build_test_loinc_bidict_library() -> BiDictLibrary {
    BiDictLibrary::new("LOINC", vec![Box::new(LoincClient::default())])
}

pub(crate) fn build_test_uo_bidict_library() -> BiDictLibrary {
    BiDictLibrary::new("UO", vec![Box::new(UO_DICT.clone())])
}

pub(crate) fn build_test_pato_bidict_library() -> BiDictLibrary {
    BiDictLibrary::new("PATO", vec![Box::new(PATO_DICT.clone())])
}

pub(crate) fn build_hgnc_test_client(temp_dir: &Path) -> CachedHGNCClient {
    CachedHGNCClient::new(temp_dir.join("test_hgnc_cache"), HGNCClient::default()).unwrap()
}

pub(crate) fn build_hgvs_test_client(temp_dir: &Path) -> CachedHGVSClient {
    CachedHGVSClient::new(temp_dir.join("test_hgvs_cache"), HGVSClient::default()).unwrap()
}

pub fn build_test_phenopacket_builder(temp_dir: &Path) -> PhenopacketBuilder {
    let hgnc_client = build_hgnc_test_client(temp_dir);
    let hgvs_client = build_hgvs_test_client(temp_dir);

    dotenv().ok();

    PhenopacketBuilder::new(
        phenopacket_builder_metadata().into(),
        Box::new(hgnc_client),
        Box::new(hgvs_client),
        build_test_hpo_bidict_library(),
        build_test_mondo_bidict_library(),
        build_test_uo_bidict_library(),
        build_test_loinc_bidict_library(),
        build_test_pato_bidict_library(),
    )
}

pub(crate) fn phenopacket_builder_metadata() -> MetaData {
    let yaml_str = std::str::from_utf8(PIPELINE_CONFIG)
        .expect("FATAL: PIPELINE_CONFIG contains invalid UTF-8");

    let config = Config::builder()
        .add_source(File::from_str(yaml_str, FileFormat::Yaml))
        .build()
        .expect("FATAL: Failed to parse configuration");

    config
        .get::<MetaData>("pipeline_config.meta_data")
        .expect("FATAL: Missing or invalid 'pipeline_config.meta_data' section")
}
