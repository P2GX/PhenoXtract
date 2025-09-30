#![allow(unused)]
use crate::ontology::GithubOntologyRegistry;
use crate::ontology::traits::OntologyRegistry;
use crate::ontology::utils::init_ontolius;
use once_cell::sync::Lazy;
use ontolius::ontology::csr::FullCsrOntology;
use std::sync::Arc;

pub(crate) static HPO: Lazy<Arc<FullCsrOntology>> = Lazy::new(|| {
    let hpo_registry = GithubOntologyRegistry::default_hpo_registry().unwrap();
    let path = hpo_registry.register("v2025-09-01").unwrap();
    init_ontolius(path).unwrap()
});
