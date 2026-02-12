use crate::config::context::Context;
use crate::config::table_context::{Identifier, SeriesContext, TableContext};
use crate::extract::contextualized_dataframe_filters::{ColumnFilter, Filter, SeriesContextFilter};
use crate::transform::error::{CollectorError, DataProcessingError};
use crate::validation::cdf_checks::check_orphaned_columns;
use crate::validation::contextualised_dataframe_validation::validate_dangling_sc;
use crate::validation::contextualised_dataframe_validation::validate_one_context_per_column;
use crate::validation::contextualised_dataframe_validation::validate_subject_id_col_no_nulls;
use crate::validation::error::ValidationError;
use log::{debug, warn};
use ordermap::OrderSet;
use polars::datatypes::StringChunked;
use polars::prelude::{Column, DataFrame, DataType, Float64Chunked, PolarsError, Series};
use regex::Regex;
use std::collections::HashMap;
use std::mem::ManuallyDrop;
use std::ptr;
use thiserror::Error;
use validator::Validate;

/// A structure that combines a `DataFrame` with its corresponding `TableContext`.
///
/// This allows for processing the data within the `DataFrame` according to the
/// rules and semantic information defined in the context.
#[derive(Clone, Validate, Default, Debug, PartialEq)]
#[validate(schema(function = "validate_one_context_per_column",))]
#[validate(schema(function = "validate_dangling_sc",))]
#[validate(schema(function = "validate_subject_id_col_no_nulls",))]
pub struct ContextualizedDataFrame {
    context: TableContext,
    data: DataFrame,
}

impl ContextualizedDataFrame {
    pub fn new(context: TableContext, data: DataFrame) -> Result<Self, ValidationError> {
        let cdf = ContextualizedDataFrame { context, data };
        cdf.validate()?;
        Ok(cdf)
    }

    pub fn context(&self) -> &TableContext {
        &self.context
    }

    pub fn series_contexts(&self) -> &Vec<SeriesContext> {
        self.context.context()
    }

    pub fn series_contexts_mut(&self) -> &Vec<SeriesContext> {
        self.context.context()
    }

    pub fn data(&self) -> &DataFrame {
        &self.data
    }

    pub fn into_data(self) -> DataFrame {
        self.data
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
    /// - `id`: An [`Identifier`] specifying which columns to retrieve. This can be:
    ///   - `Identifier::Regex(pattern)`: Uses a regular expression to match column names.
    ///     - First attempts to find a column whose name exactly matches `pattern`.
    ///     - If none is found, it returns all columns whose names match `pattern` as a regex.
    ///   - `Identifier::Multi(multi)`: A collection of explicit column names to retrieve.
    ///
    /// # Returns
    /// A `Vec<&Column>` containing references to the columns that match the given identifier(s).
    /// If no columns match, an empty vector is returned.
    ///
    /// # Examples
    /// ```ignore
    /// let cols = dataset.get_column(&Identifier::Regex("user.*".into()));
    /// let specific_cols = dataset.get_column(&Identifier::Multi(vec!["id", "name"]));
    /// ```
    pub fn get_columns(&self, id: &Identifier) -> Vec<&Column> {
        match id {
            Identifier::Regex(pattern) => {
                let mut found_columns = self
                    .data
                    .get_columns()
                    .iter()
                    .filter(|col| col.name() == pattern)
                    .collect::<Vec<&Column>>();
                if found_columns.is_empty()
                    && let Ok(regex) = Regex::new(pattern.as_str())
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
    pub fn builder(&'_ mut self) -> ContextualizedDataFrameBuilder<'_> {
        ContextualizedDataFrameBuilder::new(self)
    }

    pub fn get_building_block_ids(&self) -> OrderSet<&str> {
        self.context()
            .context()
            .iter()
            .filter_map(|sc| sc.get_building_block_id())
            .collect()
    }

    pub fn get_subject_id_col(&self) -> &Column {
        self.filter_columns()
            .where_header_context(Filter::Is(&Context::None))
            .where_data_context(Filter::Is(&Context::SubjectId))
            .collect()[0]
    }

    /// Given a column in a ContextualisedDataFrame, this function will create a patient_ID -> string data HashMap,
    /// where the data is whatever is contained in the cells of the string column.
    ///
    /// If the column, does not already have String datatype, and attempt will be made to cast it to String datatype.
    pub fn group_column_by_subject_id(
        &self,
        col_name: &str,
    ) -> Result<HashMap<String, Vec<String>>, PolarsError> {
        let mut hm: HashMap<String, Vec<String>> = HashMap::new();
        let stringified_subject_id_col = self
            .get_subject_id_col()
            .str()
            .expect("SubjectID column should be of String data type.");
        let col = self.data.column(col_name)?;
        let cast_col = if col.dtype() != &DataType::String {
            col.cast(&DataType::String)?
        } else {
            col.clone()
        };
        let string_data = cast_col.str()?;
        for (subject_id_opt, data_val_opt) in
            stringified_subject_id_col.iter().zip(string_data.iter())
        {
            let subject_id =
                subject_id_opt.expect("There should be no gaps in the SubjectID column");
            if let Some(data_val) = data_val_opt {
                hm.entry(subject_id.to_string())
                    .or_default()
                    .push(data_val.to_string())
            }
        }
        Ok(hm)
    }

    /// Given a CDF, building block ID and data contexts
    /// this function will find all columns
    /// - within that building block
    /// - and with data context in data_contexts
    /// * if there are no such columns returns Ok(None)
    /// * if there are several such columns returns CollectorError
    /// * if there is exactly one such column, Ok(Some(&column)) is returned
    pub fn get_single_linked_column<'a>(
        &'a self,
        bb_id: Option<&'a str>,
        data_contexts: &'a [Context],
    ) -> Result<Option<&'a Column>, CollectorError> {
        if let Some(bb_id) = bb_id {
            let filter = self.filter_columns();
            let filter = filter
                .where_header_context(Filter::IsNone)
                .where_building_block(Filter::Is(bb_id));

            let linked_cols = filter.where_data_contexts_are(data_contexts).collect();

            if linked_cols.len() == 1 {
                let single_linked_col = linked_cols
                    .first()
                    .expect("Column empty despite len check.");
                Ok(Some(single_linked_col))
            } else if linked_cols.is_empty() {
                Ok(None)
            } else {
                Err(CollectorError::ExpectedAtMostNLinkedColumnWithContexts {
                    table_name: self.context().name().to_string(),
                    bb_id: bb_id.to_string(),
                    contexts: data_contexts.to_vec(),
                    n_found: linked_cols.len(),
                    n_expected: 1,
                })
            }
        } else {
            Ok(None)
        }
    }

    /// Given a CDF, building block ID and data contexts
    /// this function will find all columns
    /// - within that building block
    /// - and with data context in data_contexts
    /// * if there are no such columns returns Ok(None)
    /// * if there are several such columns returns CollectorError
    /// * if there is exactly one such column,
    ///   this column is converted to StringChunked and Ok(Some(StringChunked)) is returned
    pub fn get_single_linked_column_as_str(
        &self,
        bb_id: Option<&str>,
        data_contexts: &[Context],
    ) -> Result<Option<StringChunked>, CollectorError> {
        let possible_linked_col = self.get_single_linked_column(bb_id, data_contexts)?;
        if let Some(linked_col) = possible_linked_col {
            let cast_linked_col = linked_col.cast(&DataType::String).map_err(|_| {
                DataProcessingError::CastingError {
                    col_name: linked_col.name().to_string(),
                    from: linked_col.dtype().clone(),
                    to: DataType::String,
                }
            })?;
            Ok(Some(cast_linked_col.str()?.clone()))
        } else {
            Ok(None)
        }
    }

    /// Given a CDF, building block ID and data contexts
    /// this function will find all columns
    /// - within that building block
    /// - and with data context in data_contexts
    /// * if there are no such columns returns Ok(None)
    /// * if there are several such columns returns CollectorError
    /// * if there is exactly one such column,
    ///   this column is converted to Float64Chunked and Ok(Some(Float64Chunked)) is returned
    pub fn get_single_linked_column_as_float(
        &self,
        bb_id: Option<&str>,
        data_contexts: &[Context],
    ) -> Result<Option<Float64Chunked>, CollectorError> {
        let possible_linked_col = self.get_single_linked_column(bb_id, data_contexts)?;
        if let Some(linked_col) = possible_linked_col {
            let cast_linked_col = linked_col.cast(&DataType::Float64).map_err(|_| {
                DataProcessingError::CastingError {
                    col_name: linked_col.name().to_string(),
                    from: linked_col.dtype().clone(),
                    to: DataType::Float64,
                }
            })?;
            Ok(Some(cast_linked_col.f64()?.clone()))
        } else {
            Ok(None)
        }
    }

    /// Looks for columns in the CDF which have
    ///
    /// - Building Block ID = bb_id
    /// - data_context = data_context
    /// - header_context = header_context
    ///
    /// and returns their names
    pub(crate) fn get_linked_cols_with_context(
        &self,
        bb_id: Option<&str>,
        data_context: &Context,
        header_context: &Context,
    ) -> Vec<String> {
        bb_id.map_or(vec![], |bb_id| {
            self.filter_columns()
                .where_building_block(Filter::Is(bb_id))
                .where_header_context(Filter::Is(header_context))
                .where_data_context(Filter::Is(data_context))
                .collect_owned_names()
        })
    }

    /// Converts provided columns to StringChunked.
    /// If they don't exist in the CDF, or if they aren't of String datatype, an error will be thrown.
    pub(crate) fn get_stringified_cols(
        &self,
        col_names: Vec<String>,
    ) -> Result<Vec<&StringChunked>, CollectorError> {
        let mut stringified_cols = vec![];

        for col_name in col_names {
            let col = self.data().column(&col_name)?;
            let stringified_col = col.str()?;
            stringified_cols.push(stringified_col);
        }

        Ok(stringified_cols)
    }

    pub(crate) fn get_dangling_scs(&self) -> Vec<Identifier> {
        let mut dangling_scs = vec![];
        for sc in self.series_contexts() {
            let sc_id = sc.get_identifier().clone();
            if self.get_columns(&sc_id).is_empty() {
                dangling_scs.push(sc_id);
            }
        }
        dangling_scs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::context::{Context, TimeElementType};
    use crate::config::traits::SeriesContextBuilding;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use polars::prelude::*;
    use regex::Regex;
    use rstest::rstest;

    fn sample_df() -> DataFrame {
        df!(
        "subject_id" => &["P001", "P002", "P003"],
        "age" => &[25, 30, 40],
        "bronchitis" => &["Observed", "Not observed", "Observed"],
        "overweight" => &["Not observed", "Not observed", "Observed"],
        "sex" => &["MALE", "FEMALE", "MALE"],
        )
        .unwrap()
    }

    fn sample_ctx() -> TableContext {
        TableContext::new(
            "table".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(vec!["subject_id"])
                    .with_data_context(Context::SubjectId)
                    .with_building_block_id("block_1"),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("age".to_string()))
                    .with_data_context(Context::TimeAtLastEncounter(TimeElementType::Age))
                    .with_building_block_id("block_1"),
                SeriesContext::default()
                    .with_identifier("bronchitis")
                    .with_header_context(Context::HpoLabelOrId)
                    .with_data_context(Context::ObservationStatus)
                    .with_building_block_id("block_1"),
                SeriesContext::default()
                    .with_identifier("overweight")
                    .with_header_context(Context::HpoLabelOrId)
                    .with_data_context(Context::ObservationStatus),
                SeriesContext::default()
                    .with_identifier("sex")
                    .with_data_context(Context::SubjectSex)
                    .with_building_block_id("block_1"), // BB is not realistic here, but it tests good with the test_get_single_linked_column
            ],
        )
    }

    #[rstest]
    fn test_regex_match_column_found() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let regex = Regex::new("^a.*").unwrap();
        let cols = cdf.regex_match_column(&regex);

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name(), "age");
    }

    #[rstest]
    fn test_regex_match_column_found_partial_matches() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let regex = Regex::new("a.*").unwrap();
        let cols = cdf.regex_match_column(&regex);

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name(), "age");
    }

    #[rstest]
    fn test_regex_match_column_none() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let regex = Regex::new("does_not_exist").unwrap();
        let cols = cdf.regex_match_column(&regex);

        assert!(cols.is_empty());
    }

    #[rstest]
    fn test_get_column_string_match() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let id = Identifier::Regex("sex".to_string());
        let cols = cdf.get_columns(&id);

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name(), "sex");
    }

    #[rstest]
    fn test_get_column_regex_raw() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let id = Identifier::Regex("^[a,s]{1}[a-z.]*".to_string());
        let cols = cdf.get_columns(&id);

        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name(), "subject_id");
        assert_eq!(cols[1].name(), "age");
        assert_eq!(cols[2].name(), "sex");
    }

    #[rstest]
    fn test_get_column_multi() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let id = Identifier::Multi(vec!["subject_id".to_string(), "age".to_string()]);
        let cols = cdf.get_columns(&id);

        let col_names: Vec<&str> = cols.iter().map(|c| c.name().as_str()).collect();
        assert_eq!(col_names, vec!["subject_id", "age"]);
    }

    #[rstest]
    fn test_get_column_no_partial_matches() {
        let df = df!(
        "blah" => &["Alice", "Bob", "Charlie"],
        "blah_blah" => &["Al", "Bobby", "Chaz"],
        )
        .unwrap();
        let table_context = TableContext::new(
            "test_get_column_no_partial_matches".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("blah".to_string()))
                    .with_data_context(Context::SubjectId),
            ],
        );
        let cdf = ContextualizedDataFrame::new(table_context, df).unwrap();

        let id = Identifier::Regex("blah".to_string());
        let cols = cdf.get_columns(&id);

        let col_names: Vec<&str> = cols.iter().map(|c| c.name().as_str()).collect();
        assert_eq!(col_names, vec!["blah"]);
    }

    #[rstest]
    fn test_get_building_block_ids() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let mut expected_bb_ids = OrderSet::new();
        expected_bb_ids.insert("block_1");

        assert_eq!(cdf.get_building_block_ids(), expected_bb_ids);
    }

    #[rstest]
    fn test_group_column_by_subject_id_no_cast() {
        let df = sample_df();
        let ctx = sample_ctx();

        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let patient_data_hash_map = cdf.group_column_by_subject_id("sex").unwrap();

        assert_eq!(patient_data_hash_map.len(), 3);
        assert_eq!(patient_data_hash_map["P001"], vec!["MALE"]);
        assert_eq!(patient_data_hash_map["P002"], vec!["FEMALE"]);
        assert_eq!(patient_data_hash_map["P003"], vec!["MALE"]);
    }

    #[rstest]
    fn test_group_column_by_subject_id_cast_ages() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let patient_data_hash_map = cdf.group_column_by_subject_id("age").unwrap();
        assert_eq!(patient_data_hash_map.len(), 3);
        assert_eq!(patient_data_hash_map["P001"], vec!["25"]);
        assert_eq!(patient_data_hash_map["P002"], vec!["30"]);
        assert_eq!(patient_data_hash_map["P003"], vec!["40"]);
    }

    #[rstest]
    fn test_get_single_linked_column() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let bb = cdf
            .filter_series_context()
            .where_data_context(Filter::Is(&Context::SubjectSex))
            .collect()
            .first()
            .unwrap()
            .get_building_block_id();

        let extracted_col = cdf
            .get_single_linked_column(bb, &[Context::SubjectSex])
            .unwrap()
            .unwrap();

        assert_eq!(extracted_col.name().to_string(), "sex");
    }

    #[rstest]
    fn test_get_single_linked_column_no_match() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let extracted_col = cdf
            .get_single_linked_column(Some("Absent_BB"), &[Context::DiseaseLabelOrId])
            .unwrap();

        assert!(extracted_col.is_none());
    }

    #[rstest]
    fn test_get_single_linked_column_as_str() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let bb = cdf
            .filter_series_context()
            .where_data_context(Filter::Is(&Context::SubjectSex))
            .collect()
            .first()
            .unwrap()
            .get_building_block_id();

        let extracted_col = cdf
            .get_single_linked_column_as_str(bb, &[Context::SubjectSex])
            .unwrap()
            .unwrap();

        assert_eq!(extracted_col.name().to_string(), "sex");
    }

    #[rstest]
    fn test_get_single_linked_column_as_float() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let bb = cdf
            .filter_series_context()
            .where_data_context(Filter::Is(&Context::TimeAtLastEncounter(
                TimeElementType::Age,
            )))
            .collect()
            .first()
            .unwrap()
            .get_building_block_id();

        let extracted_col = cdf
            .get_single_linked_column_as_float(
                bb,
                &[Context::TimeAtLastEncounter(TimeElementType::Age)],
            )
            .unwrap()
            .unwrap();

        assert_eq!(extracted_col.name().to_string(), "age");
    }

    #[rstest]
    fn test_get_dangling_scs() {
        let mut cdf = generate_minimal_cdf(2, 2);
        let dirty_cdf = cdf
            .builder()
            .insert_scs(&[
                SeriesContext::default().with_identifier(Identifier::from("no_match")),
                SeriesContext::default().with_identifier(Identifier::from("also_no_match")),
            ])
            .unwrap()
            .build_dirty();
        let dangling_scs = dirty_cdf.get_dangling_scs();
        assert_eq!(dangling_scs.len(), 2);
    }
}

#[must_use = "Builder must be finalized with .build()"]
#[derive(Debug)]
pub struct ContextualizedDataFrameBuilder<'a> {
    cdf: &'a mut ContextualizedDataFrame,
    is_dirty: bool,
}

impl<'a> ContextualizedDataFrameBuilder<'a> {
    pub fn new(cdf: &'a mut ContextualizedDataFrame) -> Self {
        Self {
            cdf,
            is_dirty: false,
        }
    }

    fn mark_dirty(mut self) -> Self {
        self.is_dirty = true;
        self
    }
    fn mark_clean(mut self) -> Self {
        self.is_dirty = false;
        self
    }

    pub fn insert_sc(self, sc: SeriesContext) -> Result<Self, CdfBuilderError> {
        self.cdf.context.context_mut().push(sc);
        Ok(self.mark_dirty())
    }

    pub fn insert_scs(mut self, scs: &[SeriesContext]) -> Result<Self, CdfBuilderError> {
        for sc in scs.iter() {
            self = self.insert_sc(sc.clone())?;
        }
        Ok(self.mark_dirty())
    }

    pub fn insert_col(self, col: Column) -> Result<Self, CdfBuilderError> {
        self.cdf.data.with_column(col)?;
        Ok(self.mark_dirty())
    }

    pub fn insert_cols(mut self, cols: &[Column]) -> Result<Self, CdfBuilderError> {
        for col in cols {
            self = self.insert_col(col.clone())?;
        }
        Ok(self.mark_dirty())
    }

    pub fn replace_col(
        self,
        col_name: &str,
        replacement_data: Series,
    ) -> Result<Self, CdfBuilderError> {
        self.cdf.data.replace(col_name, replacement_data)?;

        Ok(self.mark_dirty())
    }
    pub fn drop_scs_alongside_cols_with_context(
        mut self,
        header_context: &Context,
        data_context: &Context,
    ) -> Result<Self, CdfBuilderError> {
        let col_names: Vec<String> = self
            .cdf
            .filter_columns()
            .where_header_context(Filter::Is(header_context))
            .where_data_context(Filter::Is(data_context))
            .collect_owned_names();

        self = self.drop_cols(&col_names)?;
        self = self.drop_scs_with_context(header_context, data_context);

        Ok(self.mark_dirty())
    }

    pub fn insert_sc_alongside_cols(
        mut self,
        sc: SeriesContext,
        cols: &[Column],
    ) -> Result<Self, CdfBuilderError> {
        let col_names: Vec<&str> = cols.iter().map(|col| col.name().as_str()).collect();
        check_orphaned_columns(&col_names, sc.get_identifier())?;

        self = self.insert_cols(cols)?;
        self = self.insert_sc(sc)?;

        Ok(self.mark_dirty())
    }

    pub fn insert_scs_alongside_cols(
        mut self,
        inserts: &[(SeriesContext, Vec<Column>)],
    ) -> Result<Self, CdfBuilderError> {
        for (sc, cols) in inserts.iter() {
            self = self.insert_sc_alongside_cols(sc.clone(), cols)?;
        }

        Ok(self.mark_dirty())
    }

    pub fn drop_sc_alongside_cols(mut self, sc_id: &Identifier) -> Result<Self, CdfBuilderError> {
        let col_names: Vec<String> = self
            .cdf
            .filter_columns()
            .where_identifier(Filter::Is(sc_id))
            .collect_owned_names();
        self = self.drop_cols(&col_names)?;
        self = self.drop_sc(sc_id);

        Ok(self.mark_dirty())
    }

    pub fn drop_scs_alongside_cols(
        mut self,
        sc_ids: &[Identifier],
    ) -> Result<Self, CdfBuilderError> {
        for sc_id in sc_ids {
            self = self.drop_sc_alongside_cols(sc_id)?;
        }

        Ok(self.mark_dirty())
    }

    pub fn drop_scs(mut self, sc_ids: &[Identifier]) -> Self {
        for sc_id in sc_ids {
            self = self.drop_sc(sc_id);
        }
        self.mark_dirty()
    }

    pub fn drop_null_cols_alongside_scs(mut self) -> Result<Self, CdfBuilderError> {
        let null_col_names = self
            .cdf
            .data
            .get_columns()
            .iter()
            .filter_map(|col| {
                if col.null_count() == col.len() {
                    Some(col.name().to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        self = self.drop_cols(&null_col_names)?;
        self = self.drop_dangling_scs();

        Ok(self.mark_dirty())
    }

    fn drop_dangling_scs(mut self) -> Self {
        let dangling_scs = self.cdf.get_dangling_scs();
        self = self.drop_scs(dangling_scs.as_slice());
        self.mark_dirty()
    }

    fn drop_sc(self, to_remove: &Identifier) -> Self {
        self.cdf
            .context
            .context_mut()
            .retain(|sc| sc.get_identifier() != to_remove);
        self.mark_dirty()
    }

    fn drop_col(self, col_name: &str) -> Result<Self, CdfBuilderError> {
        self.cdf.data.drop_in_place(col_name)?;
        Ok(self.mark_dirty())
    }

    fn drop_cols(mut self, col_names: &Vec<String>) -> Result<Self, CdfBuilderError> {
        for col_name in col_names {
            self = self.drop_col(col_name)?;
        }

        Ok(self.mark_dirty())
    }

    fn drop_scs_with_context(self, header_context: &Context, data_context: &Context) -> Self {
        self.cdf.context.context_mut().retain(|sc| {
            sc.get_header_context() != header_context || sc.get_data_context() != data_context
        });
        self.mark_dirty()
    }

    pub fn replace_header_contexts(self, header_context_hm: HashMap<Context, Context>) -> Self {
        let scs = self.cdf.context.context_mut();
        for sc in scs {
            let dc = sc.get_header_context();
            if header_context_hm.contains_key(dc) {
                *sc.header_context_mut() = header_context_hm.get(dc).unwrap().clone();
            }
        }
        self.mark_dirty()
    }

    pub fn replace_data_contexts(self, data_context_hm: HashMap<Context, Context>) -> Self {
        let scs = self.cdf.context.context_mut();
        for sc in scs {
            let dc = sc.get_data_context();
            if data_context_hm.contains_key(dc) {
                *sc.data_context_mut() = data_context_hm.get(dc).unwrap().clone();
            }
        }
        self.mark_dirty()
    }

    pub fn cast(
        self,
        header_context: &Context,
        data_context: &Context,
        output_data_type: DataType,
    ) -> Result<Self, CdfBuilderError> {
        let col_names: Vec<String> = self
            .cdf
            .filter_columns()
            .where_header_context(Filter::Is(header_context))
            .where_data_context(Filter::Is(data_context))
            .collect_owned_names();
        for col_name in col_names.iter() {
            let col = self.cdf.data.column(col_name)?;
            let cast_col = col.cast(&output_data_type)?;
            self.cdf
                .data
                .replace(col_name, cast_col.take_materialized_series())?;
        }
        Ok(self.mark_dirty())
    }

    pub fn build(self) -> Result<&'a mut ContextualizedDataFrame, ValidationError> {
        let builder = ManuallyDrop::new(self.mark_clean());

        let cdf_ref = unsafe { ptr::read(&builder.cdf) };
        cdf_ref.validate()?;
        Ok(cdf_ref)
    }

    /// Only for test use
    #[cfg(test)]
    pub(crate) fn build_dirty(self) -> &'a mut ContextualizedDataFrame {
        let builder = ManuallyDrop::new(self.mark_clean());
        unsafe { ptr::read(&builder.cdf) }
    }
}

impl<'b> Drop for ContextualizedDataFrameBuilder<'b> {
    fn drop(&mut self) {
        if self.is_dirty {
            let struct_name = std::any::type_name::<Self>()
                .split("::")
                .last()
                .unwrap()
                .to_string();
            panic!(".build() was not called on {struct_name} after modifying data.");
        }
    }
}

#[derive(Debug, Error)]
pub enum CdfBuilderError {
    #[error("Polars error: {0}")]
    PolarsError(Box<PolarsError>),
    #[error(transparent)]
    ValidationError(#[from] ValidationError),
}

impl From<PolarsError> for CdfBuilderError {
    fn from(err: PolarsError) -> Self {
        CdfBuilderError::PolarsError(Box::new(err))
    }
}

#[cfg(test)]
mod builder_tests {
    use crate::config::context::{Context, TimeElementType};
    use crate::config::table_context::{Identifier, SeriesContext, TableContext};
    use crate::config::traits::SeriesContextBuilding;
    use crate::extract::ContextualizedDataFrame;
    use crate::extract::contextualized_dataframe_filters::Filter;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use polars::df;
    use polars::frame::DataFrame;
    use polars::prelude::{AnyValue, Column, DataType, NamedFrom, Series};
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};

    #[fixture]
    fn sample_df() -> DataFrame {
        df!(
        "subject_id" => &["Alice", "Bob", "Charlie"],
        "different" => &["Al", "Bobby", "Chaz"],
        "age" => &[25, 30, 40],
        "location (some stuff)" => &["NY", "SF", "LA"],
        "bronchitis" => &["Observed", "Not observed", "Observed"],
        "overweight" => &["Not observed", "Not observed", "Observed"],
                        "null" => &[AnyValue::Null, AnyValue::Null, AnyValue::Null],
        )
        .unwrap()
    }

    #[fixture]
    fn sample_ctx() -> TableContext {
        TableContext::new(
            "table".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(vec!["subject_id"])
                    .with_data_context(Context::SubjectId)
                    .with_building_block_id("block_1"),
                SeriesContext::default()
                    .with_identifier("age")
                    .with_data_context(Context::TimeAtLastEncounter(TimeElementType::Age))
                    .with_building_block_id("block_1"),
                SeriesContext::default()
                    .with_identifier("bronchitis")
                    .with_header_context(Context::HpoLabelOrId)
                    .with_data_context(Context::ObservationStatus)
                    .with_building_block_id("block_1"),
                SeriesContext::default()
                    .with_identifier("overweight")
                    .with_header_context(Context::HpoLabelOrId)
                    .with_data_context(Context::ObservationStatus),
                SeriesContext::default()
                    .with_identifier("null")
                    .with_data_context(Context::TimeAtLastEncounter(TimeElementType::Age))
                    .with_building_block_id("block_1"),
            ],
        )
    }

    #[rstest]
    fn test_drop_scs_with_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let original_context_no = cdf.context().context().len();

        cdf.builder()
            .drop_scs_with_context(&Context::HpoLabelOrId, &Context::ObservationStatus)
            .build_dirty();

        assert_eq!(cdf.context().context().len(), original_context_no - 2);
    }

    #[rstest]
    fn test_drop_scs_with_context_no_change() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let original_context_no = cdf.context().context().len();
        let original_col_no = cdf.data().width();
        let original_col_height = cdf.data().height();

        cdf.builder()
            .drop_scs_with_context(&Context::VitalStatus, &Context::None)
            .build_dirty();

        assert_eq!(cdf.context().context().len(), original_context_no);
        assert_eq!(cdf.data().width(), original_col_no);
        assert_eq!(cdf.data().height(), original_col_height);
    }

    #[rstest]
    fn test_drop_scs_alongside_cols_with_context_no_change() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let original_context_no = cdf.context().context().len();
        let original_col_no = cdf.data().width();
        let original_col_height = cdf.data().height();

        cdf.builder()
            .drop_scs_alongside_cols_with_context(&Context::VitalStatus, &Context::None)
            .unwrap()
            .build_dirty();

        assert_eq!(cdf.context().context().len(), original_context_no);
        assert_eq!(cdf.data().width(), original_col_no);
        assert_eq!(cdf.data().height(), original_col_height);
    }

    #[rstest]
    fn test_drop_scs_alongside_cols_with_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

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
            .drop_scs_alongside_cols_with_context(&Context::None, &Context::SubjectId)
            .unwrap()
            .build_dirty();

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
    fn test_drop_col_nonexistent() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let result = cdf.builder().drop_col("nonexistent");
        assert!(result.is_err());
    }

    #[rstest]
    fn test_drop_cols() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let expected_width = cdf.data().width() - 2;

        cdf.builder()
            .drop_cols(&vec!["different".to_string(), "age".to_string()])
            .unwrap()
            .build_dirty();
        assert_eq!(cdf.data().width(), expected_width);
        assert!(cdf.data().column("different").is_err());
        assert!(cdf.data().column("age").is_err());
        assert!(cdf.data().column("bronchitis").is_ok());
    }

    #[rstest]
    fn test_drop_sc() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let expected_len = cdf.series_contexts().len() - 1;
        cdf.builder()
            .drop_sc(&Identifier::Regex("bronchitis".to_string()))
            .build_dirty();
        assert_eq!(cdf.series_contexts().len(), expected_len);
    }

    #[rstest]
    fn test_drop_sc_alongside_cols() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let expected_len = cdf.context().context().len() - 1;

        cdf.builder()
            .drop_sc_alongside_cols(&Identifier::Regex("bronchitis".to_string()))
            .unwrap()
            .build_dirty();

        assert!(cdf.data().column("bronchitis").is_err());
        assert_eq!(cdf.series_contexts().len(), expected_len);
    }

    #[rstest]
    fn test_drop_scs_alongside_cols() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let expected_len = cdf.context().context().len() - 2;

        cdf.builder()
            .drop_scs_alongside_cols(&[
                Identifier::Regex("age".to_string()),
                Identifier::Regex("overweight".to_string()),
            ])
            .unwrap()
            .build_dirty();

        assert!(cdf.data().column("age").is_err());
        assert!(cdf.data().column("overweight").is_err());
        assert_eq!(cdf.series_contexts().len(), expected_len);
    }

    #[rstest]
    fn test_insert_sc_alongside_cols() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let expected_len = cdf.context().context().len() + 1;
        let expected_width = cdf.data().width() + 1;
        let new_col = Column::new("test_col".into(), &[10, 11, 12]);
        let sc =
            SeriesContext::default().with_identifier(Identifier::Regex("test_col".to_string()));

        cdf.builder()
            .insert_sc_alongside_cols(sc, &[new_col])
            .unwrap()
            .build_dirty();

        assert!(cdf.data().column("test_col").is_ok());
        assert_eq!(cdf.series_contexts().len(), expected_len);
        assert_eq!(cdf.data().width(), expected_width);
    }

    #[rstest]
    fn test_insert_scs_alongside_cols() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
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
            .insert_scs_alongside_cols(&inserts)
            .unwrap()
            .build_dirty();

        assert!(cdf.data().column("test_col_1").is_ok());
        assert!(cdf.data().column("test_col_2").is_ok());
        assert_eq!(cdf.series_contexts().len(), expected_len);
        assert_eq!(cdf.data().width(), expected_width);
    }

    #[rstest]
    fn test_replace_col() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let transformed_vec = vec![1001, 1002, 1003];
        cdf.builder()
            .replace_col(
                "subject_id",
                Series::new("subject_id".to_string().into(), transformed_vec),
            )
            .unwrap()
            .build_dirty();

        let expected_df = df!(
        "subject_id" => &[1001,1002,1003],
        "different" => &["Al", "Bobby", "Chaz"],
        "age" => &[25, 30, 40],
        "location (some stuff)" => &["NY", "SF", "LA"],
        "bronchitis" => &["Observed", "Not observed", "Observed"],
        "overweight" => &["Not observed", "Not observed", "Observed"],
            "null" => [AnyValue::Null, AnyValue::Null, AnyValue::Null],
        )
        .unwrap();
        assert_eq!(cdf.data(), &expected_df);
    }

    #[rstest]
    fn test_replace_header_contexts() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let keys = vec![Context::HpoLabelOrId];
        let values = vec![Context::DiseaseLabelOrId];

        let header_context_hm = keys.into_iter().zip(values.into_iter()).collect();

        cdf.builder()
            .replace_header_contexts(header_context_hm)
            .build_dirty();

        assert_eq!(
            cdf.filter_series_context()
                .where_header_context(Filter::Is(&Context::HpoLabelOrId))
                .collect()
                .len(),
            0
        );

        assert_eq!(
            cdf.filter_series_context()
                .where_header_context(Filter::Is(&Context::DiseaseLabelOrId))
                .collect()
                .len(),
            2
        );
    }

    #[rstest]
    fn test_replace_data_contexts() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let keys = vec![
            Context::ObservationStatus,
            Context::TimeAtLastEncounter(TimeElementType::Age),
        ];
        let values = vec![Context::DateOfBirth, Context::DiseaseLabelOrId];

        let data_context_hm = keys.into_iter().zip(values.into_iter()).collect();

        cdf.builder()
            .replace_data_contexts(data_context_hm)
            .build_dirty();

        assert_eq!(
            cdf.filter_series_context()
                .where_data_context(Filter::Is(&Context::ObservationStatus))
                .where_data_context(Filter::Is(&Context::TimeAtLastEncounter(
                    TimeElementType::Age
                )))
                .collect()
                .len(),
            0
        );

        assert_eq!(
            cdf.filter_series_context()
                .where_data_context(Filter::Is(&Context::DateOfBirth))
                .collect()
                .len(),
            2
        );

        assert_eq!(
            cdf.filter_series_context()
                .where_data_context(Filter::Is(&Context::DiseaseLabelOrId))
                .collect()
                .len(),
            2
        );
    }

    #[rstest]
    fn test_get_subject_id_col() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        assert_eq!(
            cdf.get_subject_id_col(),
            &Column::new("user.name".into(), &["Alice", "Bob", "Charlie"])
        );
    }

    #[rstest]
    fn test_cast() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        cdf.builder()
            .cast(
                &Context::None,
                &Context::TimeAtLastEncounter(TimeElementType::Age),
                DataType::String,
            )
            .unwrap()
            .build_dirty();
        let age_col = cdf.data().column("age").unwrap();
        assert_eq!(age_col.dtype(), &DataType::String);
        assert_eq!(age_col, &Column::new("age".into(), vec!["25", "30", "40"]));
    }

    #[rstest]
    fn test_get_linked_cols_with_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        assert_eq!(
            cdf.get_linked_cols_with_context(
                Some("block_1"),
                &Context::TimeAtLastEncounter(TimeElementType::Age),
                &Context::None
            ),
            vec!["age".to_string(), "null".to_string()]
        )
    }

    #[rstest]
    fn test_get_stringified_cols() {
        let df = sample_df();
        let ctx = sample_ctx();
        let cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        assert_eq!(
            cdf.get_stringified_cols(vec!["bronchitis".to_string(), "overweight".to_string()])
                .unwrap()
                .len(),
            2
        )
    }

    #[rstest]
    fn test_drop_dangling_scs() {
        let mut cdf = generate_minimal_cdf(2, 2);
        let dirty_cdf = cdf
            .builder()
            .insert_scs(&[
                SeriesContext::default().with_identifier(Identifier::from("no_match")),
                SeriesContext::default().with_identifier(Identifier::from("also_no_match")),
            ])
            .unwrap()
            .build_dirty();
        let cdf = dirty_cdf.builder().drop_dangling_scs().build().unwrap();
        assert_eq!(cdf, &mut generate_minimal_cdf(2, 2));
    }

    #[rstest]
    fn test_drop_scs() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let expected_len = cdf.series_contexts().len() - 2;
        cdf.builder()
            .drop_scs(&[
                Identifier::Regex("bronchitis".to_string()),
                Identifier::Regex("overweight".to_string()),
            ])
            .build_dirty();
        assert_eq!(cdf.series_contexts().len(), expected_len);
    }

    #[rstest]
    fn test_drop_null_cols_alongside_scs() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let expected_sc_no = cdf.series_contexts().len() - 1;
        cdf.builder()
            .drop_null_cols_alongside_scs()
            .unwrap()
            .build_dirty();
        assert_eq!(cdf.series_contexts().len(), expected_sc_no);
        assert!(cdf.data().column("null").is_err());
    }
}
