use crate::ontology::error::{BiDictError};
use crate::ontology::resource_references::ResourceRef;
use std::fmt::Debug;

pub trait HasPrefixId {
    fn prefix_id(&self) -> &str;
}

pub trait HasVersion {
    fn version(&self) -> &str;
}

pub trait BiDict: Debug {
    fn get(&self, id_or_label: &str) -> Result<&str, BiDictError>;
    fn get_label(&self, id: &str) -> Result<&str, BiDictError>;
    fn get_id(&self, term: &str) -> Result<&str, BiDictError>;

    fn reference(&self) -> &ResourceRef;
}
