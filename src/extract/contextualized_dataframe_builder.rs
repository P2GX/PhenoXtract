use crate::config::table_context::{Context, Identifier, SeriesContext};
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::error::StrategyError;
use crate::validation::cdf_checks::{check_dangling_sc, check_orphaned_columns};
use crate::validation::error::ValidationError;
use polars::prelude::{Column, Series};
use validator::Validate;

pub struct ContextualizedDataFrameBuilder<'a> {
    cdf: &'a mut ContextualizedDataFrame,
}

impl<'a> ContextualizedDataFrameBuilder<'a> {
    pub fn new(cdf: &'a mut ContextualizedDataFrame) -> Self {
        Self { cdf }
    }

    pub fn add_series_context(self, sc: SeriesContext) -> Result<Self, StrategyError> {
        check_dangling_sc(&sc, self.cdf)?;
        self.cdf.context_mut().context_mut().push(sc);
        Ok(self)
    }

    pub fn replace_column(
        self,
        col_name: &str,
        replacement_data: Series,
    ) -> Result<Self, StrategyError> {
        let table_name = self.cdf.context().name().to_string();
        self.cdf
            .data_mut()
            .replace(col_name, replacement_data)
            .map_err(|_| StrategyError::TransformationError {
                transformation: "replace".to_string(),
                col_name: col_name.to_string(),
                table_name,
            })?;

        Ok(self)
    }
    pub fn drop_scs_and_cols_with_context(
        mut self,
        header_context: &Context,
        data_context: &Context,
    ) -> Result<Self, StrategyError> {
        let col_names: Vec<String> = self
            .cdf
            .filter_columns()
            .where_header_context(Filter::Is(header_context))
            .where_data_context(Filter::Is(data_context))
            .collect()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        self = self.remove_many_columns(col_refs.as_slice())?;
        self = self.remove_scs_with_context(header_context, data_context);
        Ok(self)
    }

    pub fn insert_columns_with_series_context(
        self,
        sc: SeriesContext,
        cols: &[Column],
    ) -> Result<Self, StrategyError> {
        let col_names: Vec<&str> = cols.iter().map(|col| col.name().as_str()).collect();
        check_orphaned_columns(&col_names, sc.get_identifier())?;

        let table_name = self.cdf.context().name().to_string();

        for col in cols {
            self.cdf.data_mut().with_column(col.clone()).map_err(|_| {
                StrategyError::TransformationError {
                    transformation: "add column".to_string(),
                    col_name: col.name().to_string(),
                    table_name: table_name.clone(),
                }
            })?;
        }

        self.cdf.context_mut().context_mut().push(sc);
        Ok(self)
    }

    pub fn bulk_insert_columns_with_series_context(
        mut self,
        inserts: &[(SeriesContext, Vec<Column>)],
    ) -> Result<Self, StrategyError> {
        for (sc, cols) in inserts.iter() {
            self = self.insert_columns_with_series_context(sc.clone(), cols)?;
        }
        Ok(self)
    }

    pub fn drop_series_context(mut self, sc_id: &Identifier) -> Result<Self, StrategyError> {
        let col_names: Vec<String> = self
            .cdf
            .get_columns(sc_id)
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        self = self.remove_many_columns(col_refs.as_slice())?;
        self = self.remove_series_context(sc_id);

        Ok(self)
    }

    pub fn drop_many_series_context(
        mut self,
        sc_ids: &[Identifier],
    ) -> Result<Self, StrategyError> {
        for sc_id in sc_ids {
            self = self.drop_series_context(sc_id)?;
        }

        Ok(self)
    }

    fn remove_series_context(self, to_remove: &Identifier) -> Self {
        self.cdf
            .context_mut()
            .context_mut()
            .retain(|sc| sc.get_identifier() != to_remove);
        self
    }

    fn remove_column(self, col_name: &str) -> Result<Self, StrategyError> {
        self.cdf.data_mut().drop_in_place(col_name).map_err(|_| {
            StrategyError::TransformationError {
                transformation: "drop column".to_string(),
                col_name: col_name.to_string(),
                table_name: self.cdf.context().name().to_string(),
            }
        })?;
        Ok(self)
    }

    fn remove_many_columns(mut self, col_names: &[&str]) -> Result<Self, StrategyError> {
        for col_name in col_names {
            self = self.remove_column(col_name)?;
        }

        Ok(self)
    }

    fn remove_scs_with_context(self, header_context: &Context, data_context: &Context) -> Self {
        self.cdf.context_mut().context_mut().retain(|sc| {
            sc.get_header_context() != header_context || sc.get_data_context() != data_context
        });
        self
    }
    pub fn build(self) -> Result<&'a mut ContextualizedDataFrame, ValidationError> {
        self.cdf.validate().unwrap();
        Ok(self.cdf)
    }
}

impl ContextualizedDataFrame {
    pub fn builder(&'_ mut self) -> ContextualizedDataFrameBuilder<'_> {
        ContextualizedDataFrameBuilder::new(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::ContextualizedDataFrame;
    use crate::extract::contextualized_dataframe_filters::Filter;
    use polars::df;
    use polars::frame::DataFrame;
    use polars::prelude::{Column, NamedFrom, Series};
    use pretty_assertions::assert_eq;
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
    fn test_remove_scs_with_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        cdf.builder()
            .remove_scs_with_context(&Context::HpoLabelOrId, &Context::ObservationStatus);

        assert_eq!(cdf.context().context().len(), 2);
    }

    #[rstest]
    fn test_remove_scs_with_context_no_change() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        cdf.builder()
            .remove_scs_with_context(&Context::VitalStatus, &Context::None);

        assert_eq!(cdf.context().context().len(), 4);
        assert_eq!(cdf.data().width(), 6);
        assert_eq!(cdf.data().height(), 3);
    }

    #[rstest]
    fn test_remove_scs_and_cols_with_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        cdf.builder()
            .drop_scs_and_cols_with_context(&Context::None, &Context::SubjectId)
            .unwrap();

        assert_eq!(cdf.context().context().len(), 3);
        assert_eq!(cdf.data().width(), 4);
    }

    #[rstest]
    fn test_remove_scs_and_cols_with_context_no_change() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        cdf.builder()
            .drop_scs_and_cols_with_context(&Context::VitalStatus, &Context::None)
            .unwrap();

        assert_eq!(cdf.context().context().len(), 4);
        assert_eq!(cdf.data().width(), 6);
        assert_eq!(cdf.data().height(), 3);
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

        cdf.builder()
            .drop_scs_and_cols_with_context(&Context::None, &Context::SubjectId)
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

        let result = cdf.builder().remove_column("nonexistent");
        assert!(result.is_err());
    }

    #[rstest]
    fn test_remove_many_columns() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        let expected_width = cdf.data().width() - 2;

        cdf.builder()
            .remove_many_columns(&["different", "age"])
            .unwrap();
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

        let expected_len = cdf.series_contexts().len() - 1;
        cdf.builder()
            .remove_series_context(&Identifier::Regex("bronchitis".to_string()));
        assert_eq!(cdf.series_contexts().len(), expected_len);
    }

    #[rstest]
    fn test_drop_series_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        let expected_len = cdf.context().context().len() - 1;

        cdf.builder()
            .drop_series_context(&Identifier::Regex("bronchitis".to_string()))
            .unwrap();

        assert!(cdf.data().column("bronchitis").is_err());
        assert_eq!(cdf.series_contexts().len(), expected_len);
    }

    #[rstest]
    fn test_drop_many_series_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        let expected_len = cdf.context().context().len() - 1;

        cdf.builder()
            .drop_many_series_context(&[
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
        let expected_len = cdf.context().context().len() + 1;
        let expected_width = cdf.data().width() + 1;
        let new_col = Column::new("test_col".into(), &[10, 11, 12]);
        let sc =
            SeriesContext::default().with_identifier(Identifier::Regex("test_col".to_string()));

        cdf.builder()
            .insert_columns_with_series_context(sc, &[new_col])
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
        let expected_len = cdf.context().context().len() + 2;
        let expected_width = cdf.data().width() + 2;

        let col_d = Column::new("test_col_1".into(), &[10, 11, 12]);
        let col_e = Column::new("test_col_2".into(), &[13, 14, 15]);
        let sc1 =
            SeriesContext::default().with_identifier(Identifier::Regex("test_col_1".to_string()));
        let sc2 =
            SeriesContext::default().with_identifier(Identifier::Regex("test_col_2".to_string()));

        let inserts = vec![(sc1, vec![col_d]), (sc2, vec![col_e])];

        cdf.builder()
            .bulk_insert_columns_with_series_context(&inserts)
            .unwrap();

        assert!(cdf.data().column("test_col_1").is_ok());
        assert!(cdf.data().column("test_col_2").is_ok());
        assert_eq!(cdf.series_contexts().len(), expected_len);
        assert_eq!(cdf.data().width(), expected_width);
    }

    #[rstest]
    fn test_replace_column() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df);
        let transformed_vec = vec![1001, 1002, 1003];
        cdf.builder()
            .replace_column(
                "user.name",
                Series::new("user.name".to_string().into(), transformed_vec),
            )
            .unwrap();

        let expected_df = df!(
        "user.name" => &[1001,1002,1003],
        "different" => &["Al", "Bobby", "Chaz"],
        "age" => &[25, 30, 40],
        "location (some stuff)" => &["NY", "SF", "LA"],
        "bronchitis" => &["Observed", "Not observed", "Observed"],
        "overweight" => &["Not observed", "Not observed", "Observed"],
        )
        .unwrap();
        assert_eq!(cdf.data(), &expected_df);
    }
}
