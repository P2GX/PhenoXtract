use crate::load::loader_module::Loadable;
use crate::transform::phenopacket::Phenopacket;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct FileSystemLoader {
    out_path: PathBuf,
}

impl Loadable for FileSystemLoader {
    fn load(&self, phenopacket: &Phenopacket) -> Result<(), anyhow::Error> {
        Ok(())
    }
}
