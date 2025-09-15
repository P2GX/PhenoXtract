use crate::config::table_context::{Identifier, SeriesContext, TableContext};
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use log::debug;
use polars::prelude::{Column, DataFrame, NamedFrom, Series};
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

    #[allow(unused)]
    pub fn get_series_contexts(&self) -> &Vec<SeriesContext> {
        &self.context.context
    }

    #[allow(unused)]
    pub fn data_mut(&mut self) -> &mut DataFrame {
        &mut self.data
    }

    fn regex_match_column(&self, regex: &Regex) -> Vec<&Column> {
        self.data
            .get_columns()
            .iter()
            .filter(|col| regex.is_match(col.name()))
            .collect::<Vec<&Column>>()
    }

    /// Retrieves columns from the dataset based on the given identifier(s).
    ///
    /// # Parameters
    /// - `id`: An `Identifier` specifying which columns to retrieve. This can be:
    ///     - `Identifier::Regex(pattern)`: Uses a regular expression to match column names.
    ///       It first tries an escaped version of the regex pattern and falls back to the raw pattern
    ///       if no columns are found.
    ///     - `Identifier::Multi(multi)`: A collection of column names to retrieve explicitly.
    ///
    /// # Returns
    /// A `Vec<&Column>` containing references to the columns that match the given identifier(s).
    /// If no columns match, an empty vector is returned.
    ///
    /// # Behavior
    /// - When using a regex, columns are matched against the column names in the dataset.
    /// - When using multiple identifiers, only the columns that exist in the dataset are returned.
    ///
    /// # Examples
    /// ```ignore
    /// let cols = dataset.get_column(&Identifier::Regex("user.*".into()));
    /// let specific_cols = dataset.get_column(&Identifier::Multi(vec!["id", "name"]));
    /// ```
    #[allow(unused)]
    pub fn get_columns(&self, id: &Identifier) -> Vec<&Column> {
        match id {
            Identifier::Regex(pattern) => {
                let mut found_columns = vec![];
                if let Ok(escape_regex) = Regex::new(escape(pattern).as_str()) {
                    found_columns = self.regex_match_column(&escape_regex);
                }
                if let Ok(regex) = Regex::new(pattern.as_str())
                    && found_columns.is_empty()
                {
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
                let found_columns = self
                    .data
                    .get_columns()
                    .iter()
                    .filter(|col| multi.contains(&col.name().to_string()))
                    .collect::<Vec<&Column>>();

                debug!(
                    "Found columns {:?} using multi identifiers {:?}",
                    found_columns
                        .iter()
                        .map(|col| col.name().as_str())
                        .collect::<Vec<&str>>(),
                    multi
                );
                found_columns
            }
        }
    }

    #[allow(unused)]
    ///The column col_name will be replaced with the data inside the vector transformed_vec
    pub fn replace_column<'a, T, Phantom: ?Sized>(
        &'a mut self,
        transformed_vec: Vec<T>,
        col_name: &str,
        table_name: &str,
    ) -> Result<&'a mut DataFrame, TransformError>
    where
        Series: NamedFrom<Vec<T>, Phantom>,
    {
        let transformed_series = Series::new(col_name.into(), transformed_vec);
        self.data_mut()
            .replace(col_name, transformed_series)
            .map_err(|_e| {
                StrategyError(
                    format!(
                        "Could not insert transformed column {col_name} into table {table_name}."
                    )
                    .to_string(),
                )
            })
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
        "user.name" => &["Alice", "Bob", "Charlie"],
        "age" => &[25, 30, 40],
        "location (some stuff)" => &["NY", "SF", "LA"]
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
        assert_eq!(cols[0].name(), "user.name");
        assert_eq!(cols[1].name(), "age");
        assert_eq!(cols[2].name(), "location (some stuff)");
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

        let id = Identifier::Regex("location (some stuff)".to_string());
        let cols = cdf.get_columns(&id);

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name(), "location (some stuff)");
    }

    #[rstest]
    fn test_get_column_regex_raw() {
        let df = sample_df();
        let ctx = TableContext::default();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let id = Identifier::Regex("^[a,u]{1}[a-z.]*".to_string());
        let cols = cdf.get_columns(&id);

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name(), "user.name");
        assert_eq!(cols[1].name(), "age");
    }

    #[rstest]
    fn test_get_column_multi() {
        let df = sample_df();
        let ctx = TableContext::default();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let id = Identifier::Multi(vec!["user.name".to_string(), "age".to_string()]);
        let cols = cdf.get_columns(&id);

        let col_names: Vec<&str> = cols.iter().map(|c| c.name().as_str()).collect();
        assert_eq!(col_names, vec!["user.name", "age"]);
    }

    #[rstest]
    fn test_replace_column() {
        let df = sample_df();
        let ctx = TableContext::default();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        let transformed_vec = vec![1001, 1002, 1003];
        cdf.replace_column(transformed_vec, "user.name", "table_name")
            .unwrap();

        let expected_df = df!(
        "user.name" => &[1001,1002,1003],
        "age" => &[25, 30, 40],
        "location (some stuff)" => &["NY", "SF", "LA"]
        )
        .unwrap();
        assert_eq!(cdf.data, expected_df);
    }
}
