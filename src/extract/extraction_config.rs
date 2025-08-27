use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum PatientOrientation {
    PatientsAreRows,
    PatientsAreColumns,
}

#[derive(Debug, Validate, Deserialize, Serialize, Clone, PartialEq)]
pub struct ExtractionConfig {
    #[allow(unused)]
    pub name: String,
    pub has_headers: bool,
    pub patient_orientation: PatientOrientation,
}

impl ExtractionConfig {
    #[allow(dead_code)]
    pub(crate) fn new(
        name: String,
        has_headers: bool,
        patient_orientation: PatientOrientation,
    ) -> Self {
        ExtractionConfig {
            name,
            has_headers,
            patient_orientation,
        }
    }
}
