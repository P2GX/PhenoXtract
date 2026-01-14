use crate::ontology::error::RegistryError;
use std::path::PathBuf;

pub trait OntologyRegistry {
    fn register(&mut self, version: &str) -> Result<PathBuf, RegistryError>;
    fn deregister(&mut self, version: &str) -> Result<(), RegistryError>;
    fn get_location(&mut self, version: &str) -> Option<PathBuf>;
}

pub trait HasPrefixId {
    fn prefix_id(&self) -> &str;
}

pub trait HasVersion {
    fn version(&self) -> &str;
}

// TODO: Implement for BIDicts
pub trait BIDict {
    fn get(&self, id_or_label: &str) -> Option<String>;
    fn get_term(&self, id: &str) -> Option<String>;
    fn get_id(&self, term: &str) -> Option<String>;
}
