use crate::config::table_context::TableContext;

use crate::extract::extraction_config::ExtractionConfig;
use crate::extract::traits::HasSource;
use crate::validation::data_source_validation::{
    validate_extraction_config_links, validate_extraction_config_unique_ids,
    validate_unique_sheet_names,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use validator::Validate;

/// Defines an Excel workbook as a data source.
#[derive(Debug, Validate, Deserialize, Serialize, Clone, PartialEq)]
#[validate(schema(
    function = "validate_extraction_config_links",
    skip_on_field_errors = false
))]
pub struct ExcelDataSource {
    /// The file path to the Excel workbook.
    pub(crate) source: PathBuf,
    /// A list of contexts, one for each sheet to be processed from the workbook.
    #[validate(custom(function = "validate_unique_sheet_names"))]
    pub(crate) contexts: Vec<TableContext>,

    /// One extraction config for every worksheet from the workbook that will be extracted.
    #[validate(custom(function = "validate_extraction_config_unique_ids"))]
    pub(crate) extraction_configs: Vec<ExtractionConfig>,
}

impl ExcelDataSource {
    pub fn new(
        source: PathBuf,
        contexts: Vec<TableContext>,
        extraction_configs: Vec<ExtractionConfig>,
    ) -> Self {
        ExcelDataSource {
            source,
            contexts,
            extraction_configs,
        }
    }
}

impl HasSource for ExcelDataSource {
    type Source = PathBuf;

    fn source(&self) -> &Self::Source {
        &self.source
    }

    fn with_source(mut self, source: &Self::Source) -> Self {
        self.source = source.clone();
        self
    }
}
