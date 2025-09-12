use crate::config::table_context::{Identifier, TableContext};
use log::debug;
use polars::prelude::{Column, DataFrame};
use regex::{Regex, escape};
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

    fn regex_match_column(&self, regex: &Regex) -> Vec<&Column> {
        self.data
            .get_columns()
            .iter()
            .filter(|col| regex.is_match(col.name()))
            .collect::<Vec<&Column>>()
    }

    /// Retrieves a collection of columns (`&Column`) from the dataset based on the given identifier.
    ///
    /// # Parameters
    /// - `id`: An `Identifier` specifying which columns to retrieve. This can be:
    ///     - `Identifier::Regex(pattern)`: Selects all columns whose names match the given regex pattern.
    ///       The function will attempt both an escaped and unescaped version of the pattern.
    ///     - `Identifier::Multi(vec)`: Selects columns whose names are explicitly listed in the vector.
    ///
    /// # Returns
    /// A `Vec<&Column>` containing references to the matching columns.
    /// If no columns match the identifier, the returned vector will be empty.
    ///
    /// # Examples
    /// ```ignore
    /// let series = dataframe.get_series(&Identifier::Regex(r"^user_.*".into()));
    /// let specific_series = dataframe.get_series(&Identifier::Multi(vec!["age".into(), "score".into()]));
    /// ```
    ///
    /// # Notes
    /// - The function internally uses `regex_match_column` to handle regex matching.
    /// - When using a regex, both the escaped and original patterns are tried to maximize matches.
    #[allow(unused)]
    pub fn get_column(&self, id: &Identifier) -> Vec<&Column> {
        match id {
            Identifier::Regex(pattern) => {
                let mut found_columns = vec![];
                if let Ok(escape_regex) = Regex::new(escape(pattern).as_str()) {
                    found_columns = self.regex_match_column(&escape_regex);
                }
                if let Ok(regex) = Regex::new(pattern.as_str()) {
                    found_columns = self.regex_match_column(&regex);
                }
                debug!(
                    "Found columns {:?} using regex {}",
                    found_columns
                        .iter()
                        .map(|col| col.name().as_str())
                        .collect::<Vec<&str>>(),
                    pattern
                );
                found_columns
            }
            Identifier::Multi(multi) => {
                let found_columns = multi
                    .iter()
                    .filter_map(|col_name| self.data.column(col_name).ok())
                    .collect::<Vec<&Column>>();

                debug!(
                    "Found columns {:?} using multi identifiers {}",
                    found_columns
                        .iter()
                        .map(|col| col.name().as_str())
                        .collect::<Vec<&str>>(),
                    multi.join(", ")
                );
                found_columns
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use polars::prelude::*;
    use regex::Regex;
    use rstest::rstest;

    fn sample_df() -> DataFrame {
        df!(
        "name" => &["Alice", "Bob", "Charlie"],
        "age" => &[25, 30, 40],
        "location" => &["NY", "SF", "LA"]
        )
        .unwrap()
    }

    #[rstest]
    fn test_regex_match_column_found() {
        let df = sample_df();
        let ctx = TableContext::default();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let regex = Regex::new("^a.*").unwrap();
        let cols = cdf.regex_match_column(&regex);

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name(), "age");
    }

    #[rstest]
    fn test_regex_match_column_found_partial_matches() {
        let df = sample_df();
        let ctx = TableContext::default();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let regex = Regex::new("a.*").unwrap();
        let cols = cdf.regex_match_column(&regex);

        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name(), "name");
        assert_eq!(cols[1].name(), "age");
        assert_eq!(cols[2].name(), "location");
    }

    #[rstest]
    fn test_regex_match_column_none() {
        let df = sample_df();
        let ctx = TableContext::default();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let regex = Regex::new("does_not_exist").unwrap();
        let cols = cdf.regex_match_column(&regex);

        assert!(cols.is_empty());
    }

    #[rstest]
    fn test_get_column_regex_escape() {
        let df = sample_df();
        let ctx = TableContext::default();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let id = Identifier::Regex("age".to_string());
        let cols = cdf.get_column(&id);

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name(), "age");
    }

    #[rstest]
    fn test_get_column_regex_raw() {
        let df = sample_df();
        let ctx = TableContext::default();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let id = Identifier::Regex("^[a,n]{1,2}[a-z]*".to_string());
        let cols = cdf.get_column(&id);

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name(), "name");
        assert_eq!(cols[1].name(), "age");
    }

    #[rstest]
    fn test_get_column_multi() {
        let df = sample_df();
        let ctx = TableContext::default();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let id = Identifier::Multi(vec!["name".to_string(), "age".to_string()]);
        let cols = cdf.get_column(&id);

        let col_names: Vec<&str> = cols.iter().map(|c| c.name().as_str()).collect();
        assert_eq!(col_names, vec!["name", "age"]);
    }
}
