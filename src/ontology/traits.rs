pub trait HasPrefixId {
    fn prefix_id(&self) -> &str;
}

pub trait HasVersion {
    fn version(&self) -> &str;
}

// TODO: Implement for BIDicts
pub trait BIDict {
    fn get(&self, id_or_label: &str) -> Option<String>;
    fn get_label(&self, id: &str) -> Option<String>;
    fn get_id(&self, term: &str) -> Option<String>;
}
