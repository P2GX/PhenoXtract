use serde::{Deserialize, Serialize};
use validator::Validate;

///Given tabular data, this struct provides the necessary information
///so that the extract function knows how to convert the data into a Polars data frame.
#[derive(Debug, Validate, Deserialize, Serialize, Clone, PartialEq)]
pub struct ExtractionConfig {
    #[allow(unused)]
    ///If the data source contains multiple tables (a.k.a sheets) then this should
    ///be identical to the name of the relevant table/sheet.
    pub name: String,
    ///If true, the top row of the data, or the first column of the data,
    ///consists of column/row headers.
    pub has_headers: bool,
    ///If true, each row of the data corresponds to a single patient.
    ///If false, each column of the data corresponds to a single patient.
    pub patients_are_rows: bool,
}

impl ExtractionConfig {
    #[allow(dead_code)]
    pub(crate) fn new(name: String, has_headers: bool, patient_orientation: bool) -> Self {
        ExtractionConfig {
            name,
            has_headers,
            patients_are_rows: patient_orientation,
        }
    }
}
