use crate::ontology::loinc_client::LoincClient;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::traits::HasPrefixId;
use crate::test_suite::ontology_mocking::{MONDO_BIDICT, ONTOLOGY_FACTORY};
use crate::test_suite::resource_references::HPO_REF;
use crate::transform::PhenopacketBuilder;
use dotenvy::dotenv;
use pivot::hgnc::{CachedHGNCClient, HGNCClient};
use pivot::hgvs::{CachedHGVSClient, HGVSClient};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

fn build_test_dicts() -> HashMap<String, Arc<OntologyBiDict>> {
    let hpo_dict = ONTOLOGY_FACTORY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .build_bidict(&HPO_REF.clone(), None)
        .unwrap();

    HashMap::from_iter(vec![
        (hpo_dict.ontology.prefix_id().to_string(), hpo_dict),
        (
            MONDO_BIDICT.ontology.prefix_id().to_string(),
            MONDO_BIDICT.clone(),
        ),
    ])
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
        build_test_dicts(),
        Box::new(hgnc_client),
        Box::new(hgvs_client),
        Some(LoincClient::default()),
    )
}
