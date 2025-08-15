use crate::config::table_context::TableContext;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::extractable::Extractable;
use crate::validation::data_source_validation::validate_unique_sheet_names;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use validator::Validate;

/// Defines a CSV file as a data source.
#[derive(Debug, Deserialize)]
pub struct CSVDataSource {
    /// The file path to the CSV source.
    #[allow(unused)]
    pub source: PathBuf,
    /// The character used to separate fields in the CSV file (e.g., ',').
    #[allow(unused)]
    pub separator: Option<String>,
    /// The context describing how to interpret the single table within the CSV.
    #[allow(unused)]
    pub table: TableContext,
}

/// Defines an Excel workbook as a data source.
#[derive(Debug, Validate, Deserialize, Serialize)]
pub struct ExcelDatasource {
    /// The file path to the Excel workbook.
    #[allow(unused)]
    source: PathBuf,
    /// A list of contexts, one for each sheet to be processed from the workbook.
    #[allow(unused)]
    #[validate(custom(function = "validate_unique_sheet_names"))]
    sheets: Vec<TableContext>,
}

/// An enumeration of all supported data source types.
///
/// This enum uses a `tag` to differentiate between source types (e.g., "csv", "excel")
/// when deserializing from a configuration file.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum DataSource {
    Csv(CSVDataSource),
    Excel(ExcelDatasource),
}

impl Extractable for DataSource {
    fn extract(&self) -> Result<Vec<ContextualizedDataFrame>, anyhow::Error> {
        match self {
            // Rename input without _, when implementing
            DataSource::Csv(_csv_source) => {
                todo!("CSV extraction is not yet implemented.")
            }
            DataSource::Excel(_excel_source) => {
                todo!("Excel extraction is not yet implemented.")
            }
        }
    }
}
