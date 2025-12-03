use crate::ontology::HGNCClient;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::ontology::traits::HasPrefixId;
use crate::test_suite::ontology_mocking::{MONDO_BIDICT, ONTOLOGY_FACTORY};
use crate::test_suite::resource_references::{GENO_REF, HPO_REF};
use crate::transform::PhenopacketBuilder;
use ratelimit::Ratelimiter;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

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

pub fn build_test_phenopacket_builder(temp_dir: &Path) -> PhenopacketBuilder {
    let hgnc_client = build_hgnc_test_client(temp_dir);
    PhenopacketBuilder::new(build_test_dicts(), hgnc_client)
}
