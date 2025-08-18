use crate::config::table_context::TableContext;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::extractable::Extractable;
use crate::validation::data_source_validation::validate_unique_sheet_names;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use validator::Validate;

pub trait HasSource {
    type Source;
    #[allow(dead_code)]
    fn source(&self) -> &Self::Source;
    #[allow(dead_code)]
    fn set_source(&mut self, source: &Self::Source);
}

/// Defines a CSV file as a data source.
#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct CSVDataSource {
    /// The file path to the CSV source.
    #[allow(unused)]
    pub source: PathBuf,
    /// The character used to separate fields in the CSV file (e.g., ',').
    #[allow(unused)]
    separator: Option<String>,
    /// The context describing how to interpret the single table within the CSV.
    #[allow(unused)]
    table: TableContext,
}

impl CSVDataSource {
    #[allow(dead_code)]
    pub fn new(source: PathBuf, separator: Option<String>, table: TableContext) -> Self {
        Self {
            source,
            separator,
            table,
        }
    }
}

impl HasSource for CSVDataSource {
    type Source = PathBuf;

    fn source(&self) -> &Self::Source {
        &self.source
    }

    fn set_source(&mut self, source: &Self::Source) {
        self.source = source.clone()
    }
}

/// Defines an Excel workbook as a data source.
#[derive(Debug, Validate, Deserialize, Serialize, Clone)]
pub struct ExcelDatasource {
    /// The file path to the Excel workbook.
    #[allow(unused)]
    pub source: PathBuf,
    /// A list of contexts, one for each sheet to be processed from the workbook.
    #[allow(unused)]
    #[validate(custom(function = "validate_unique_sheet_names"))]
    sheets: Vec<TableContext>,
}

impl ExcelDatasource {
    #[allow(dead_code)]
    pub(crate) fn new(source: PathBuf, sheets: Vec<TableContext>) -> Self {
        ExcelDatasource { source, sheets }
    }
}

impl HasSource for ExcelDatasource {
    type Source = PathBuf;

    fn source(&self) -> &Self::Source {
        &self.source
    }

    fn set_source(&mut self, source: &Self::Source) {
        self.source = source.clone()
    }
}

/// An enumeration of all supported data source types.
///
/// This enum uses a `tag` to differentiate between source types (e.g., "csv", "excel")
/// when deserializing from a configuration file.
#[derive(Debug, Deserialize, Clone, Serialize)]
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
