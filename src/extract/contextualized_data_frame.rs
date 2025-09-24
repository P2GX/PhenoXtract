use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use log::{debug, warn};
use polars::prelude::{Column, DataFrame, DataType, NamedFrom, Series};
use regex::{Regex, escape};
use validator::Validate;

/// A structure that combines a `DataFrame` with its corresponding `TableContext`.
///
/// This allows for processing the data within the `DataFrame` according to the
/// rules and semantic information defined in the context.
#[derive(Clone, Validate, Default)]
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
    pub fn get_sc_from_id(&self, id: &Identifier) -> Option<&SeriesContext> {
        self.context.context.iter().find(|sc| &sc.identifier == id)
    }

    //todo test after MVP
    pub fn check_contexts_have_data_type(
        &self,
        data_context: Context,
        desired_dtype: DataType,
    ) -> bool {
        let columns = self.get_cols_with_data_context(data_context.clone());
        let contexts_have_desired_dtype = columns.iter().all(|col| col.dtype() == &desired_dtype);

        if !contexts_have_desired_dtype {
            warn!(
                "Not all columns with {} data context have {} type in table {}.",
                data_context,
                desired_dtype,
                self.context().name
            );
        }
        contexts_have_desired_dtype
    }

    #[allow(unused)]
    ///The column col_name will be replaced with the data inside the vector transformed_vec
    pub fn replace_column<T, Phantom: ?Sized>(
        &mut self,
        transformed_vec: Vec<T>,
        col_name: &str,
    ) -> Result<&mut ContextualizedDataFrame, TransformError>
    where
        Series: NamedFrom<Vec<T>, Phantom>,
    {
        let table_name = self.context.name.clone();
        let transformed_series = Series::new(col_name.into(), transformed_vec);
        let transform_result = self
            .data_mut()
            .replace(col_name, transformed_series)
            .map_err(|_e| {
                StrategyError(
                    format!(
                        "Could not insert transformed column {col_name} into table {table_name}."
                    )
                    .to_string(),
                )
            });
        match transform_result {
            Ok(df) => Ok(self),
            Err(e) => Err(e),
        }
    }

    #[allow(dead_code)]
    pub fn get_cols_with_data_context(&self, data_context: Context) -> Vec<&Column> {
        self.context()
            .context
            .iter()
            .filter_map(|sc| {
                if sc.get_data_context() == data_context {
                    Some(self.get_columns(&sc.identifier))
                } else {
                    None
                }
            })
            .flatten()
            .collect::<Vec<&Column>>()
    }

    #[allow(unused)]
    pub fn get_cols_with_header_context(&self, header_context: Context) -> Vec<&Column> {
        self.context()
            .context
            .iter()
            .filter_map(|sc| {
                if sc.get_header_context() == header_context {
                    Some(self.get_columns(&sc.identifier))
                } else {
                    None
                }
            })
            .flatten()
            .collect::<Vec<&Column>>()
    }

    #[allow(unused)]
    pub fn get_scs_with_data_context(&self, data_context: Context) -> Vec<&SeriesContext> {
        self.context
            .context
            .iter()
            .filter(|sc| sc.get_data_context() == data_context)
            .collect()
    }

    //todo test after MVP
    #[allow(unused)]
    pub fn get_linked_cols_with_data_context(
        &self,
        sc: &SeriesContext,
        data_context: Context,
    ) -> Vec<&Column> {
        let linked_scs = sc
            .linked_to
            .iter()
            .filter_map(|id| self.get_sc_from_id(id))
            .collect::<Vec<&SeriesContext>>();
        let linked_scs_filtered = linked_scs
            .iter()
            .filter(|linked_sc| linked_sc.get_data_context() == data_context)
            .collect::<Vec<&&SeriesContext>>();
        linked_scs_filtered
            .iter()
            .flat_map(|linked_sc| self.get_columns(&linked_sc.identifier))
            .collect::<Vec<&Column>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        TableContext {
            name: "table".to_string(),
            context: vec![
                SeriesContext::new(
                    Identifier::Multi(vec!["user.name".to_string(), "different".to_string()]),
                    Context::None,
                    Context::SubjectId,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("age".to_string()),
                    Context::None,
                    Context::SubjectAge,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("bronchitis".to_string()),
                    Context::HpoLabel,
                    Context::ObservationStatus,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("overweight".to_string()),
                    Context::HpoLabel,
                    Context::ObservationStatus,
                    None,
                    None,
                    vec![],
                ),
            ],
        }
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
    fn test_replace_column() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        let transformed_vec = vec![1001, 1002, 1003];
        cdf.replace_column(transformed_vec, "user.name").unwrap();

        let expected_df = df!(
        "user.name" => &[1001,1002,1003],
        "different" => &["Al", "Bobby", "Chaz"],
        "age" => &[25, 30, 40],
        "location (some stuff)" => &["NY", "SF", "LA"],
        "bronchitis" => &["Observed", "Not observed", "Observed"],
        "overweight" => &["Not observed", "Not observed", "Observed"],
        )
        .unwrap();
        assert_eq!(cdf.data, expected_df);
    }

    #[rstest]
    fn test_get_cols_with_data_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df);
        assert_eq!(
            cdf.get_cols_with_data_context(Context::SubjectId),
            vec![
                cdf.data.column("user.name").unwrap(),
                cdf.data.column("different").unwrap()
            ]
        );
        assert_eq!(
            cdf.get_cols_with_data_context(Context::SubjectAge),
            vec![cdf.data.column("age").unwrap()]
        );
    }

    #[rstest]
    fn test_get_cols_with_header_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df);
        assert_eq!(
            cdf.get_cols_with_header_context(Context::HpoLabel),
            vec![
                cdf.data.column("bronchitis").unwrap(),
                cdf.data.column("overweight").unwrap()
            ]
        );
    }
}
