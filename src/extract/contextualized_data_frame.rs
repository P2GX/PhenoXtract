use crate::config::table_context::{Identifier, TableContext};
use polars::prelude::{Column, DataFrame};
use regex::Regex;
use validator::Validate;

/// A structure that combines a `DataFrame` with its corresponding `TableContext`.
///
/// This allows for processing the data within the `DataFrame` according to the
/// rules and semantic information defined in the context.
#[derive(Debug, PartialEq, Clone, Validate, Default)]
pub struct ContextualizedDataFrame {
    #[allow(unused)]
    context: TableContext,
    #[allow(unused)]
    pub data: DataFrame,
}

impl ContextualizedDataFrame {
    pub fn new(context: TableContext, data: DataFrame) -> Self {
        ContextualizedDataFrame { context, data }
    }

    #[allow(unused)]
    pub fn context(&self) -> &TableContext {
        &self.context
    }

    #[allow(unused)]
    pub fn context_mut(&mut self) -> &mut TableContext {
        &mut self.context
    }

    ///If the identifier is a regex, first a check will be done to see if there is an exact match as a string. If there is not, then
    /// the string will be considered as a regex. If it is an invalid regex, then an empty vector will be returned.
    /// if it is a valid regex, then a regex search will be performed. Any columns that are found will be returned in the vector.
    /// If the identifier is a multi, then all columns whose names match one of the strings in the vector will be returned
    #[allow(unused)]
    pub fn get_cols_from_identifier(&self, identifier: Identifier) -> Vec<&Column> {

        let cols = self.data.get_columns();

        match identifier {
            Identifier::Regex(regex) => {
                match cols.iter().find(|col|col.name()==&&regex) {
                    Some(col) => vec![col],
                    None => {
                        let regex_pattern_result = Regex::new(&regex);
                        match regex_pattern_result {
                            Err(_) => {vec![]},
                            Ok(regex_pattern) => {
                                cols.iter().filter(|col|regex_pattern.is_match(col.name())).collect::<Vec<&Column>>()
                            },
                        }
                    },
                }
            },
            Identifier::Multi(ids) => {
                cols.iter().filter(|col|ids.contains(&col.name().to_string())).collect::<Vec<&Column>>()
            },
        }

    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::Identifier::Regex;
    use crate::config::table_context::{SeriesContext, TableContext};
    use crate::extract::extraction_config::ExtractionConfig;
    use polars::df;
    use polars::prelude::DataFrame;
    use polars::prelude::TimeUnit;
    use rstest::{fixture, rstest};
    use rust_xlsxwriter::{ColNum, ExcelDateTime, Format, IntoCustomDateTime, RowNum, Workbook};
    use std::f64;
    use std::fmt::Write;
    use std::fs::File;
    use std::io::Write as StdWrite;
    use tempfile::TempDir;



}