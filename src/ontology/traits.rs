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
