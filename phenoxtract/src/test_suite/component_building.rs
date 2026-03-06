use crate::ontology::loinc_client::LoincClient;
use crate::test_suite::ontology_mocking::{
    HPO_DICT, MAXO_BIDICT, MONDO_BIDICT, NCIT_BIDICT, PATO_DICT, UBERON_BIDICT, UO_DICT,
};
use crate::test_suite::phenopacket_component_generation::default_meta_data;
use crate::transform::PhenopacketBuilder;
use crate::transform::bidict_library::BiDictLibrary;
use crate::transform::transform_context::TransformContext;
use dotenvy::dotenv;
use pivot::hgnc::{CachedHGNCClient, HGNCClient};
use pivot::hgvs::{CachedHGVSClient, HGVSClient};
use std::path::Path;
use std::sync::Arc;

pub(crate) fn build_test_hpo_bidict_library() -> BiDictLibrary {
    BiDictLibrary::new("HPO", vec![Box::new(HPO_DICT.clone())])
}

pub(crate) fn build_test_mondo_bidict_library() -> BiDictLibrary {
    BiDictLibrary::new("MONDO", vec![Box::new(MONDO_BIDICT.clone())])
}

pub(crate) fn build_hgnc_test_client(temp_dir: &Path) -> CachedHGNCClient {
    CachedHGNCClient::new(temp_dir.join("test_hgnc_cache"), HGNCClient::default()).unwrap()
}

pub(crate) fn build_hgvs_test_client(temp_dir: &Path) -> CachedHGVSClient {
    CachedHGVSClient::new(temp_dir.join("test_hgvs_cache"), HGVSClient::default()).unwrap()
}

pub(crate) fn default_builder_context(temp_dir: &Path) -> TransformContext {
    let hgnc_client = build_hgnc_test_client(temp_dir);
    let hgvs_client = build_hgvs_test_client(temp_dir);

    let mut builder = TransformContext::builder(
        default_meta_data().into(),
        Arc::new(hgnc_client),
        Arc::new(hgvs_client),
    );

    builder.add_hpo_bidict(Box::new(HPO_DICT.clone()));
    builder.add_disease_bidict(Box::new(MONDO_BIDICT.clone()));
    builder.add_unit_bidict(Box::new(UO_DICT.clone()));
    builder.add_assay_bidict(Box::new(LoincClient::default()));
    builder.add_anatomy_bidict(Box::new(UBERON_BIDICT.clone()));
    builder.add_treatment_attributes_bidict(Box::new(NCIT_BIDICT.clone()));
    builder.add_qualitative_measurement_bidict(Box::new(PATO_DICT.clone()));
    builder.add_procedure_bidict(Box::new(MAXO_BIDICT.clone()));

    builder.build()
}

pub fn build_test_phenopacket_builder(temp_dir: &Path) -> PhenopacketBuilder {
    dotenv().ok();
    PhenopacketBuilder::new(default_builder_context(temp_dir))
}
