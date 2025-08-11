use crate::load::loader_module::Loadable;
use crate::transform::phenopacket::Phenopacket;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct FileSystemLoader {
    _out_path: PathBuf,
}

impl Loadable for FileSystemLoader {
    // Rename input withoug _, when implementing
    fn load(&self, _phenopacket: &Phenopacket) -> Result<(), anyhow::Error> {
        Ok(())
    }
}
