use crate::config::table_context::TableContext;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::extractable::Extractable;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashSet;
use std::path::PathBuf;
use validator::{Validate, ValidationError};

/// Defines a CSV file as a data source.
#[derive(Debug, Deserialize)]
pub struct CSVDataSource {
    /// The file path to the CSV source.
    #[allow(unused)]
    source: PathBuf,
    /// The character used to separate fields in the CSV file (e.g., ',').
    #[allow(unused)]
    separator: Option<String>,
    /// The context describing how to interpret the single table within the CSV.
    #[allow(unused)]
    table: TableContext,
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

fn validate_unique_sheet_names(sheets: &[TableContext]) -> Result<(), ValidationError> {
    let mut seen_names = HashSet::new();

    let duplicates: Vec<String> = sheets
        .iter()
        .filter_map(|s| {
            if !seen_names.insert(&s.name) {
                Some(s.name.clone())
            } else {
                None
            }
        })
        .collect();

    if duplicates.is_empty() {
        Ok(())
    } else {
        let mut error = ValidationError::new("unique");
        error.add_param(Cow::from("duplicates"), &duplicates);
        Err(error.with_message(Cow::Owned("Duplicate sheet name configured.".to_string())))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_validate_unique_sheet_names() {
        let table_context = vec![
            TableContext {
                name: "phenotypes".to_string(),
                columns: None,
                rows: None,
            },
            TableContext {
                name: "genotypes".to_string(),
                columns: None,
                rows: None,
            },
        ];
        let validation = validate_unique_sheet_names(&table_context);
        assert!(validation.is_ok());
    }

    #[rstest]
    fn test_validate_unique_sheet_names_error() {
        let table_context = vec![
            TableContext {
                name: "phenotypes".to_string(),
                columns: None,
                rows: None,
            },
            TableContext {
                name: "phenotypes".to_string(),
                columns: None,
                rows: None,
            },
        ];
        let validation = validate_unique_sheet_names(&table_context);
        assert!(validation.is_err());
    }
}
