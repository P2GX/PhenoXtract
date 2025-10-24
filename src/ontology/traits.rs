use crate::ontology::error::RegistryError;
use std::path::PathBuf;

pub trait OntologyRegistry {
    #[allow(dead_code)]
    fn register(&mut self, version: &str) -> Result<PathBuf, RegistryError>;
    #[allow(dead_code)]
    fn deregister(&mut self, version: &str) -> Result<(), RegistryError>;
    #[allow(dead_code)]
    fn get_location(&mut self, version: &str) -> Option<PathBuf>;
}

pub trait HasPrefixId {
    fn prefix_id(&self) -> &str;
}

pub trait HasVersion {
    fn version(&self) -> &str;
}
