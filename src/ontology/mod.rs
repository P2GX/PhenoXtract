pub(crate) mod bio_registry_client;
pub mod error;
pub(crate) use bio_registry_client::BioRegistryClient;
pub(crate) mod ontology_bidict;

pub mod ontology_factory;
pub mod resource_references;
pub use ontology_factory::CachedOntologyFactory;
pub use resource_references::OntologyRef;
pub mod loinc_client;
pub mod traits;
