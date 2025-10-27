use crate::config::table_context::{Identifier, SeriesContext, TableContext};
use crate::extract::contextualized_dataframe_filters::{ColumnFilter, SeriesContextFilter};
use crate::validation::contextualised_dataframe_validation::{
    validate_one_context_per_column, validate_single_subject_id_column,
};
use log::{debug, warn};
use ordermap::OrderSet;
use polars::prelude::{Column, DataFrame};
use regex::{Regex, escape};
use validator::Validate;

/// A structure that combines a `DataFrame` with its corresponding `TableContext`.
///
/// This allows for processing the data within the `DataFrame` according to the
/// rules and semantic information defined in the context.
#[derive(Clone, Validate, Default, Debug, PartialEq)]
#[validate(schema(function = "validate_one_context_per_column",))]
#[validate(schema(function = "validate_single_subject_id_column",))]
pub struct ContextualizedDataFrame {
    #[allow(unused)]
    context: TableContext,
    #[allow(unused)]
    data: DataFrame,
}

impl ContextualizedDataFrame {
    pub fn new(context: TableContext, data: DataFrame) -> Self {
        ContextualizedDataFrame { context, data }
    }

    #[allow(unused)]
    pub fn context(&self) -> &TableContext {
        &self.context
    }

    pub fn context_mut(&mut self) -> &mut TableContext {
        &mut self.context
    }

    #[allow(unused)]
    pub fn series_contexts(&self) -> &Vec<SeriesContext> {
        self.context.context()
    }

    #[allow(unused)]
    pub fn series_contexts_mut(&self) -> &Vec<SeriesContext> {
        self.context.context()
    }

    pub fn data(&self) -> &DataFrame {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut DataFrame {
        &mut self.data
    }

    pub fn into_data(self) -> DataFrame {
        self.data
    }

    pub fn set_data(&mut self, data: DataFrame) {
        self.data = data;
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
                if found_columns.is_empty() {
                    warn!("No columns found for regex {}", pattern);
                }
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
                if found_columns.is_empty() {
                    warn!("No columns found for multi identifiers {:?}", multi);
                }
                found_columns
            }
        }
    }

    pub fn filter_series_context(&'_ self) -> SeriesContextFilter<'_> {
        SeriesContextFilter::new(self.context.context())
    }

    pub fn filter_columns(&'_ self) -> ColumnFilter<'_> {
        ColumnFilter::new(self)
    }

    pub fn get_building_block_ids(&self) -> OrderSet<&str> {
        self.context()
            .context()
            .iter()
            .filter_map(|sc| sc.get_building_block_id())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::Context;
    use polars::prelude::*;
    use regex::Regex;
    use rstest::{fixture, rstest};

    #[fixture]
    fn sample_df() -> DataFrame {
        df!(
        "user.name" => &["Alice", "Bob", "Charlie"],
        "different" => &["Al", "Bobby", "Chaz"],
        "age" => &[25, 30, 40],
        "location (some stuff)" => &["NY", "SF", "LA"],
        "bronchitis" => &["Observed", "Not observed", "Observed"],
        "overweight" => &["Not observed", "Not observed", "Observed"],
        )
        .unwrap()
    }

    #[fixture]
    fn sample_ctx() -> TableContext {
        TableContext::new(
            "table".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Multi(vec![
                        "user.name".to_string(),
                        "different".to_string(),
                    ]))
                    .with_data_context(Context::SubjectId)
                    .with_building_block_id(Some("block_1".to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("age".to_string()))
                    .with_data_context(Context::SubjectAge)
                    .with_building_block_id(Some("block_1".to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("bronchitis".to_string()))
                    .with_header_context(Context::HpoLabelOrId)
                    .with_data_context(Context::ObservationStatus)
                    .with_building_block_id(Some("block_1".to_string())),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("overweight".to_string()))
                    .with_header_context(Context::HpoLabelOrId)
                    .with_data_context(Context::ObservationStatus),
            ],
        )
    }

    #[rstest]
    fn test_regex_match_column_found() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let regex = Regex::new("^a.*").unwrap();
        let cols = cdf.regex_match_column(&regex);

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name(), "age");
    }

    #[rstest]
    fn test_regex_match_column_found_partial_matches() {
        let df = sample_df();
        let ctx = sample_ctx();
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
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let regex = Regex::new("does_not_exist").unwrap();
        let cols = cdf.regex_match_column(&regex);

        assert!(cols.is_empty());
    }

    #[rstest]
    fn test_get_column_regex_escape() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let id = Identifier::Regex("location (some stuff)".to_string());
        let cols = cdf.get_columns(&id);

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name(), "location (some stuff)");
    }

    #[rstest]
    fn test_get_column_regex_raw() {
        let df = sample_df();
        let ctx = sample_ctx();
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
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df);

        let id = Identifier::Multi(vec!["user.name".to_string(), "age".to_string()]);
        let cols = cdf.get_columns(&id);

        let col_names: Vec<&str> = cols.iter().map(|c| c.name().as_str()).collect();
        assert_eq!(col_names, vec!["user.name", "age"]);
    }

    #[rstest]
    fn test_get_building_block_ids() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df);
        let mut expected_bb_ids = OrderSet::new();
        expected_bb_ids.insert("block_1");

        assert_eq!(cdf.get_building_block_ids(), expected_bb_ids);
    }
}
