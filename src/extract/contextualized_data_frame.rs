use crate::config::table_context::{Identifier, TableContext};
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
        let matched_column_names: Vec<String> = self
            .data
            .get_column_names_str()
            .iter()
            .filter_map(|&name| {
                if regex.is_match(name) {
                    Some(name.to_string())
                } else {
                    None
                }
            })
            .collect();

        matched_column_names
            .iter()
            .map(|col_names| {
                self.data
                    .column(col_names)
                    .expect("Expected valid column name")
            })
            .collect()
    }
    #[allow(unused)]
    pub fn get_series(&self, id: &Identifier) -> Vec<&Column> {
        match id {
            Identifier::Regex(regex) => {
                if let Ok(escape_regex) = Regex::new(escape(regex).as_str()) {
                    let found_columns = self.regex_match_column(&escape_regex);
                    if !found_columns.is_empty() {
                        return found_columns;
                    }
                }
                if let Ok(re) = Regex::new(regex.as_str()) {
                    let pattern = String::from(r"\A(?:") + re.as_str() + r")\z";
                    let full_match_regex = Regex::new(&pattern)
                        .unwrap_or_else(|_| panic!("Failed to compile regex {}", pattern));
                    return self.regex_match_column(&full_match_regex);
                }
                vec![]
            }
            Identifier::Multi(multi) => multi
                .iter()
                .filter_map(|col_name| self.data.column(col_name).ok())
                .collect::<Vec<&Column>>(),
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
    fn test_get_series_regex_escape() {
        let df = sample_df();
        let ctx = TableContext::default();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let id = Identifier::Regex("age".to_string());
        let cols = cdf.get_series(&id);

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name(), "age");
    }

    #[rstest]
    #[case::basic("[a,n]{1,2}[a-z]*")]
    #[case::anchored("^[a,n]{1,2}[a-z]*$")]
    #[case::anchored_absolut(r"\A[a,n]{1,2}[a-z]*\z")]
    fn test_get_series_regex_raw(#[case] pattern: &str) {
        let df = sample_df();
        let ctx = TableContext::default();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let id = Identifier::Regex(pattern.to_string());
        let cols = cdf.get_series(&id);

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name(), "name");
        assert_eq!(cols[1].name(), "age");
    }

    #[rstest]
    fn test_get_series_multi() {
        let df = sample_df();
        let ctx = TableContext::default();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let id = Identifier::Multi(vec!["name".to_string(), "age".to_string()]);
        let cols = cdf.get_series(&id);

        let col_names: Vec<&str> = cols.iter().map(|c| c.name().as_str()).collect();
        assert_eq!(col_names, vec!["name", "age"]);
    }
}
