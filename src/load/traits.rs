use crate::config::meta_data::MetaData;
use crate::load::error::LoadError;
use phenopackets::schema::v2::Phenopacket;

pub trait Loadable {
    /// A trait to implement saving Phenopackets to a file system.
    fn load(&self, phenopacket: &[Phenopacket]) -> Result<(), LoadError>;
}
