use crate::ontology::loinc_client::LoincClient;
use crate::test_suite::ontology_mocking::{HPO_DICT, MONDO_BIDICT, PATO_DICT, UO_DICT};
use crate::test_suite::phenopacket_component_generation::default_meta_data;
use crate::transform::PhenopacketBuilder;
use crate::transform::bidict_library::BiDictLibrary;
use dotenvy::dotenv;
use pivot::hgnc::{CachedHGNCClient, HGNCClient};
use pivot::hgvs::{CachedHGVSClient, HGVSClient};
use std::path::Path;

pub(crate) fn build_test_hpo_bidict_library() -> BiDictLibrary {
    BiDictLibrary::new("HPO", vec![Box::new(HPO_DICT.clone())])
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
        default_meta_data().into(),
        Box::new(hgnc_client),
        Box::new(hgvs_client),
        build_test_hpo_bidict_library(),
        build_test_mondo_bidict_library(),
        build_test_uo_bidict_library(),
        build_test_loinc_bidict_library(),
        build_test_pato_bidict_library(),
    )
}
