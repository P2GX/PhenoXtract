use crate::config::table_context::TableContext;
use crate::extract::extraction_config::ExtractionConfig;
use crate::extract::traits::HasSource;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Defines a CSV file as a data source.
#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct CSVDataSource {
    /// The file path to the CSV source.
    pub source: PathBuf,
    /// The character used to separate fields in the CSV file (e.g., ',').
    pub separator: Option<char>,
    /// The context describing how to interpret the resulting DataFrame.
    pub context: TableContext,
    /// This configures how we extract the DataFrame.
    pub extraction_config: ExtractionConfig,
}

impl CSVDataSource {
    #[allow(dead_code)]
    pub fn new(
        source: PathBuf,
        separator: Option<char>,
        table: TableContext,
        extraction_config: ExtractionConfig,
    ) -> Self {
        Self {
            source,
            separator,
            context: table,
            extraction_config,
        }
    }
}

impl HasSource for CSVDataSource {
    type Source = PathBuf;

    fn source(&self) -> &Self::Source {
        &self.source
    }

    fn with_source(mut self, source: &Self::Source) -> Self {
        self.source = source.clone();
        self
    }
}
