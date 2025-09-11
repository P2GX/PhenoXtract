use crate::ontology::error::RegistryError;
use std::path::PathBuf;

pub(crate) trait OntologyRegistry {
    #[allow(dead_code)]
    fn register(&self, version: &str) -> Result<PathBuf, RegistryError>;
    #[allow(dead_code)]
    fn deregister(&self, version: &str) -> Result<bool, RegistryError>;
    #[allow(dead_code)]
    fn get_location(&self, version: &str) -> Result<PathBuf, RegistryError>;
}
