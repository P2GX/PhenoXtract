use std::path::PathBuf;

pub(crate) trait OntologyRegistry {
    fn register(&self, version: &str) -> Result<PathBuf, anyhow::Error>;
    fn deregister(&self, version: &str) -> Result<bool, anyhow::Error>;
    fn get_location(&self, version: &str) -> Result<PathBuf, anyhow::Error>;
}
