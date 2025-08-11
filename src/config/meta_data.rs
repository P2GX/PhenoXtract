use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MetaData {
    #[allow(unused)]
    created_by: Option<String>,
    // If not set, default will be the name of this crate
    #[allow(unused)]
    submitted_by: String,
}
