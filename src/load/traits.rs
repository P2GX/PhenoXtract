use crate::load::error::LoadError;
use phenopackets::schema::v2::Phenopacket;
use std::fmt::Debug;

pub trait Loadable: Debug {
    /// A trait to implement saving Phenopackets to a file system.
    fn load(&self, phenopackets: &[Phenopacket]) -> Result<(), LoadError>;
}
