use crate::config::table_context::TableContext;
use crate::extract::traits::HasSource;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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

    fn with_source(mut self, source: &Self::Source) -> Self {
        self.source = source.clone();
        self
    }
}
