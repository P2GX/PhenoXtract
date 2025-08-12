use crate::load::file_system_loader::FileSystemLoader;
use phenopackets::schema::v2::Phenopacket;

use serde::Deserialize;

pub trait Loadable {
    /// A trait to implement saving Phenopackets to a file system.
    fn load(&self, phenopacket: &Phenopacket) -> Result<(), anyhow::Error>;
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
#[allow(dead_code)]
enum Loader {
    #[allow(unused)]
    FileSystem(FileSystemLoader),
}
