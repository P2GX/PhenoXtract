use serde::Deserialize;

/// Hold all shared meta data for the phenopackets produced by the pipeline
#[derive(Debug, Deserialize)]
pub struct MetaData {
    #[allow(unused)]
    // When not set should be defaulted to app name and version
    created_by: Option<String>,
    #[allow(unused)]
    submitted_by: String,
}
