use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
use crate::extract::contextualized_dataframe_filters::{ColumnFilter, Filter, SeriesContextFilter};
use crate::transform::error::StrategyError;
use crate::validation::cdf_checks::{check_dangling_sc, check_orphaned_columns};
use crate::validation::contextualised_dataframe_validation::{
    validate_one_context_per_column, validate_single_subject_id_column,
};
use log::{debug, warn};
use ordermap::OrderSet;
use polars::prelude::{Column, DataFrame, NamedFrom, Series};
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

    pub fn context_mut(&mut self) -> &TableContext {
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

    pub fn add_series_context(&mut self, sc: SeriesContext) -> Result<(), StrategyError> {
        check_dangling_sc(&sc, self)?;
        self.context.context_mut().push(sc);
        Ok(())
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

    #[allow(unused)]
    /// The column col_name will be replaced with the data inside the vector transformed_vec
    pub fn replace_column<T, Phantom: ?Sized>(
        &mut self,
        transformed_vec: Vec<T>,
        col_name: &str,
    ) -> Result<&mut ContextualizedDataFrame, StrategyError>
    where
        Series: NamedFrom<Vec<T>, Phantom>,
    {
        let table_name = self.context.name().to_string();
        let transformed_series = Series::new(col_name.into(), transformed_vec);
        let transform_result = self
            .data_mut()
            .replace(col_name, transformed_series)
            .map_err(|_| StrategyError::TransformationError {
                transformation: "replace".to_string(),
                col_name: col_name.to_string(),
                table_name,
            });
        match transform_result {
            Ok(df) => Ok(self),
            Err(e) => Err(e),
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

    pub fn insert_columns_with_series_context(
        &mut self,
        sc: SeriesContext,
        cols: &[Column],
    ) -> Result<&mut Self, StrategyError> {
        let col_names = cols
            .iter()
            .map(|col| col.name().as_str())
            .collect::<Vec<&str>>();
        check_orphaned_columns(col_names.as_slice(), sc.get_identifier())?;

        let table_name = self.context.name().to_string();
        for col in cols {
            let col_name = col.name().to_string();
            self.data
                .with_column(col.clone())
                .map_err(|_| StrategyError::TransformationError {
                    transformation: "add column".to_string(),
                    col_name: col_name.to_string(),
                    table_name: table_name.clone(),
                })?;
        }

        self.add_series_context(sc.clone())?;

        Ok(self)
    }

    pub fn bulk_insert_columns_with_series_context(
        &mut self,
        inserts: &[(SeriesContext, Vec<Column>)],
    ) -> Result<&mut Self, StrategyError> {
        for (sc, cols) in inserts.iter() {
            self.insert_columns_with_series_context(sc.clone(), cols)?;
        }

        Ok(self)
    }

    pub fn drop_scs_and_cols_with_context(
        &mut self,
        header_context: &Context,
        data_context: &Context,
    ) -> Result<&mut Self, StrategyError> {
        let col_names: Vec<String> = self
            .filter_columns()
            .where_header_context(Filter::Is(header_context))
            .where_data_context(Filter::Is(data_context))
            .collect()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        self.remove_many_columns(col_refs.as_slice())?;
        self.remove_scs_with_context(header_context, data_context);

        Ok(self)
    }

    pub fn drop_series_context(&mut self, sc_id: &Identifier) -> Result<&mut Self, StrategyError> {
        let col_names: Vec<String> = self
            .get_columns(sc_id)
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        self.remove_many_columns(&col_refs)?;
        self.remove_series_context(sc_id);

        Ok(self)
    }

    pub fn drop_many_series_context(
        &mut self,
        sc_ids: &[Identifier],
    ) -> Result<&mut Self, StrategyError> {
        for sc_id in sc_ids {
            self.drop_series_context(sc_id)?;
        }
        Ok(self)
    }

    fn remove_scs_with_context(&mut self, header_context: &Context, data_context: &Context) {
        self.context.context_mut().retain(|sc| {
            sc.get_header_context() != header_context || sc.get_data_context() != data_context
        });
    }

    fn remove_series_context(&mut self, to_remove: &Identifier) {
        self.context
            .context_mut()
            .retain(|sc| sc.get_identifier() != to_remove);
    }

    fn remove_column(&mut self, col_name: &str) -> Result<&mut Self, StrategyError> {
        self.data_mut().drop_in_place(col_name).map_err(|_| {
            StrategyError::TransformationError {
                transformation: "drop column".to_string(),
                col_name: col_name.to_string(),
                table_name: self.context.name().to_string(),
            }
        })?;
        Ok(self)
    }

    fn remove_many_columns(&mut self, col_names: &[&str]) -> Result<&mut Self, StrategyError> {
        for col_name in col_names {
            self.remove_column(col_name)?;
        }

        Ok(self)
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
    fn test_get_building_block_ids() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df);
        let mut expected_bb_ids = OrderSet::new();
        expected_bb_ids.insert("block_1");

        assert_eq!(cdf.get_building_block_ids(), expected_bb_ids);
    }

    #[rstest]
    fn test_remove_scs_with_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        cdf.remove_scs_with_context(&Context::HpoLabelOrId, &Context::ObservationStatus);

        assert_eq!(cdf.context.context().len(), 2);
    }

    #[rstest]
    fn test_remove_scs_with_context_no_change() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        cdf.remove_scs_with_context(&Context::VitalStatus, &Context::None);

        assert_eq!(cdf.context.context().len(), 4);
        assert_eq!(cdf.data.width(), 6);
        assert_eq!(cdf.data.height(), 3);
    }

    #[rstest]
    fn test_remove_scs_and_cols_with_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        cdf.drop_scs_and_cols_with_context(&Context::None, &Context::SubjectId)
            .unwrap();

        assert_eq!(cdf.context.context().len(), 3);
        assert_eq!(cdf.data.width(), 4);
    }

    #[rstest]
    fn test_remove_scs_and_cols_with_context_no_change() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        cdf.drop_scs_and_cols_with_context(&Context::VitalStatus, &Context::None)
            .unwrap();

        assert_eq!(cdf.context.context().len(), 4);
        assert_eq!(cdf.data.width(), 6);
        assert_eq!(cdf.data.height(), 3);
    }

    #[rstest]
    fn test_drop_cols_with_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);

        let filter_cols = cdf
            .filter_columns()
            .where_header_context(Filter::Is(&Context::None))
            .where_data_context(Filter::Is(&Context::SubjectId))
            .collect();
        assert!(!filter_cols.is_empty());
        let filter_sc = cdf
            .filter_series_context()
            .where_header_context(Filter::Is(&Context::None))
            .where_data_context(Filter::Is(&Context::SubjectId))
            .collect();
        assert!(!filter_sc.is_empty());

        cdf.drop_scs_and_cols_with_context(&Context::None, &Context::SubjectId)
            .unwrap();

        let filter_cols = cdf
            .filter_columns()
            .where_header_context(Filter::Is(&Context::None))
            .where_data_context(Filter::Is(&Context::SubjectId))
            .collect();
        assert!(filter_cols.is_empty());
        let filter_sc = cdf
            .filter_series_context()
            .where_header_context(Filter::Is(&Context::None))
            .where_data_context(Filter::Is(&Context::SubjectId))
            .collect();
        assert!(filter_sc.is_empty());
    }

    #[rstest]
    fn test_remove_column_nonexistent() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        cdf.drop_scs_and_cols_with_context(&Context::VitalStatus, &Context::None)
            .unwrap();

        let result = cdf.remove_column("nonexistent");
        assert!(result.is_err());
    }

    #[rstest]
    fn test_remove_many_columns() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        let expected_width = cdf.data.width() - 2;

        cdf.remove_many_columns(&["different", "age"]).unwrap();
        assert_eq!(cdf.data().width(), expected_width);
        assert!(cdf.data().column("different").is_err());
        assert!(cdf.data().column("age").is_err());
        assert!(cdf.data().column("bronchitis").is_ok());
    }

    #[rstest]
    fn test_remove_series_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        cdf.drop_scs_and_cols_with_context(&Context::VitalStatus, &Context::None)
            .unwrap();

        let expected_len = cdf.series_contexts().len() - 1;
        cdf.remove_series_context(&Identifier::Regex("bronchitis".to_string()));
        assert_eq!(cdf.series_contexts().len(), expected_len);
    }

    #[rstest]
    fn test_drop_series_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        let expected_len = cdf.context.context().len() - 1;

        cdf.drop_series_context(&Identifier::Regex("bronchitis".to_string()))
            .unwrap();

        assert!(cdf.data().column("bronchitis").is_err());
        assert_eq!(cdf.series_contexts().len(), expected_len);
    }

    #[rstest]
    fn test_drop_many_series_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        let expected_len = cdf.context.context().len() - 1;

        cdf.drop_many_series_context(&[
            Identifier::Regex("different".to_string()),
            Identifier::Regex("age".to_string()),
        ])
        .unwrap();

        assert!(cdf.data().column("different").is_err());
        assert!(cdf.data().column("age").is_err());
        assert_eq!(cdf.series_contexts().len(), expected_len);
    }

    #[rstest]
    fn test_insert_columns_with_series_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        let expected_len = cdf.context.context().len() + 1;
        let expected_width = cdf.data.width() + 1;
        let new_col = Column::new("test_col".into(), &[10, 11, 12]);
        let sc =
            SeriesContext::default().with_identifier(Identifier::Regex("test_col".to_string()));

        cdf.insert_columns_with_series_context(sc, &[new_col])
            .unwrap();

        assert!(cdf.data().column("test_col").is_ok());
        assert_eq!(cdf.series_contexts().len(), expected_len);
        assert_eq!(cdf.data().width(), expected_width);
    }

    #[rstest]
    fn test_bulk_insert_columns_with_series_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        let expected_len = cdf.context.context().len() + 2;
        let expected_width = cdf.data.width() + 2;
        let col_d = Column::new("test_col_1".into(), &[10, 11, 12]);
        let col_e = Column::new("test_col_2".into(), &[13, 14, 15]);
        let sc1 =
            SeriesContext::default().with_identifier(Identifier::Regex("test_col_1".to_string()));
        let sc2 =
            SeriesContext::default().with_identifier(Identifier::Regex("test_col_2".to_string()));

        let inserts = vec![(sc1, vec![col_d]), (sc2, vec![col_e])];

        cdf.bulk_insert_columns_with_series_context(&inserts)
            .unwrap();

        assert!(cdf.data().column("test_col_1").is_ok());
        assert!(cdf.data().column("test_col_2").is_ok());
        assert_eq!(cdf.series_contexts().len(), expected_len);
        assert_eq!(cdf.data().width(), expected_width);
    }
}
