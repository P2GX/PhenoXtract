use crate::load::file_system_loader::FileSystemLoader;
use crate::transform::phenopacket::Phenopacket;
use serde::Deserialize;

pub trait Loadable {
    /// A trait to implement saving Phenopackets to a file system.
    fn load(&self, phenopacket: &Phenopacket) -> Result<(), anyhow::Error>;
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Loader {
    #[allow(unused)]
    FileSystem(FileSystemLoader),
}
