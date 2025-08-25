use crate::load::error::LoadError;
use crate::load::traits::Loadable;
use phenopackets::schema::v2::Phenopacket;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct FileSystemLoader {
    #[allow(unused)]
    pub out_path: PathBuf,
}

impl Loadable for FileSystemLoader {
    // Rename input withoug _, when implementing
    fn load(&self, _phenopacket: &Phenopacket) -> Result<(), LoadError> {
        Ok(())
    }
}
