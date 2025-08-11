use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MetaData {
    created_by: Option<String>,
    // If not set, default will be the name of this crate
    submitted_by: String,
}
