use crate::config::table_context::TableContext;

use crate::extract::traits::HasSource;
use crate::validation::data_source_validation::validate_unique_sheet_names;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use validator::Validate;

/// Defines an Excel workbook as a data source.
#[derive(Debug, Validate, Deserialize, Serialize, Clone)]
pub struct ExcelDatasource {
    /// The file path to the Excel workbook.
    #[allow(unused)]
    pub source: PathBuf,
    /// A list of contexts, one for each sheet to be processed from the workbook.
    #[allow(unused)]
    #[validate(custom(function = "validate_unique_sheet_names"))]
    pub contexts: Vec<TableContext>,
    //todo do we need to add the default thing that's in the csv_data_source struct?
}

impl ExcelDatasource {
    #[allow(dead_code)]
    pub(crate) fn new(source: PathBuf, contexts: Vec<TableContext>) -> Self {
        ExcelDatasource { source, contexts }
    }
}

impl HasSource for ExcelDatasource {
    type Source = PathBuf;

    fn source(&self) -> &Self::Source {
        &self.source
    }

    fn with_source(mut self, source: &Self::Source) -> Self {
        self.source = source.clone();
        self
    }
}
