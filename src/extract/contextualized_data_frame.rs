use crate::config::context::Context;
use crate::config::table_context::{Identifier, SeriesContext, TableContext};
use crate::extract::contextualized_dataframe_filters::{ColumnFilter, Filter, SeriesContextFilter};
use crate::transform::error::{CollectorError, DataProcessingError, StrategyError};
use crate::validation::cdf_checks::check_orphaned_columns;
use crate::validation::contextualised_dataframe_validation::validate_dangling_sc;
use crate::validation::contextualised_dataframe_validation::validate_one_context_per_column;
use crate::validation::contextualised_dataframe_validation::validate_subject_id_col_no_nulls;
use crate::validation::error::ValidationError;
use log::{debug, warn};
use ordermap::OrderSet;
use polars::datatypes::StringChunked;
use polars::prelude::{Column, DataFrame, DataType, Series};
use regex::Regex;
use std::mem::ManuallyDrop;
use std::ptr;
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

    /// Extracts a uniquely-defined value from matching contexts in a CDF.
    ///
    /// Hunts through the CDF for all values matching the specified data and header contexts,
    /// then enforces cardinality constraints: zero matches returns `None`, exactly one match
    /// returns that value, but multiple distinct values trigger an error.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Extract a patient's date of birth from their CDF
    /// let dob = patient_cdf.get_single_multiplicity_element(
    ///     Context::DateOfBirth,
    ///     Context::None
    /// )?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `CollectorError::ExpectedSingleValue` when multiple distinct values are found
    /// for the given context pair.
    pub(crate) fn get_single_multiplicity_element<'a>(
        &self,
        data_context: &'a Context,
        header_context: &'a Context,
    ) -> Result<Option<String>, CollectorError> {
        let cols_of_element_type: Vec<&Column> = self
            .filter_columns()
            .where_data_context(Filter::Is(data_context))
            .where_header_context(Filter::Is(header_context))
            .collect();

        if cols_of_element_type.is_empty() {
            return Ok(None);
        }

        let mut combined_col = cols_of_element_type[0].clone();
        for col in cols_of_element_type.iter().skip(1) {
            combined_col.extend(col)?;
        }

        let unique_values = combined_col.drop_nulls().unique_stable()?;

        match unique_values.len() {
            0 => Ok(None),
            1 => {
                let cast_unique = unique_values.cast(&DataType::String)?;
                let val = cast_unique.get(0)?;
                Ok(Some(
                    val.extract_str()
                        .expect("Should have been a string.")
                        .to_string(),
                ))
            }
            _ => Err(CollectorError::ExpectedSingleValue {
                table_name: self.context().name().to_string(),
                patient_id: self.get_subject_id_col().get(0)?.str_value().to_string(),
                data_context: data_context.clone(),
                header_context: header_context.clone(),
            }),
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
    pub fn get_single_linked_column(
        &self,
        bb_id: Option<&str>,
        data_contexts: &[Context],
    ) -> Result<Option<StringChunked>, CollectorError> {
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

                let cast_linked_col = single_linked_col.cast(&DataType::String).map_err(|_| {
                    DataProcessingError::CastingError {
                        col_name: single_linked_col.name().to_string(),
                        from: single_linked_col.dtype().clone(),
                        to: DataType::String,
                    }
                })?;
                Ok(Some(cast_linked_col.str()?.clone()))
            } else if linked_cols.is_empty() {
                Ok(None)
            } else {
                Err(CollectorError::ExpectedAtMostOneLinkedColumnWithContexts {
                    table_name: self.context().name().to_string(),
                    bb_id: bb_id.to_string(),
                    contexts: data_contexts.to_vec(),
                    amount_found: linked_cols.len(),
                })
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::context::Context;
    use crate::test_utils::generate_minimal_cdf_components;
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
                    .with_identifier(Identifier::Multi(vec!["subject_id".to_string()]))
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
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("sex".to_string()))
                    .with_data_context(Context::SubjectSex)
                    .with_building_block_id(Some("block_1".to_string())), // BB is not realistic here, but it tests good with the test_get_single_linked_column
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
            .get_single_linked_column(Some("Absent_BB"), &[Context::OrphanetLabelOrId])
            .unwrap();

        assert!(extracted_col.is_none());
    }

    #[rstest]
    fn test_collect_single_multiplicity_element_multiple() {
        let (subject_col, subject_tc) = generate_minimal_cdf_components(1, 2);

        let df = DataFrame::new(vec![
            subject_col.clone(),
            Column::new(
                "sex".into(),
                &[AnyValue::String("MALE"), AnyValue::String("MALE")],
            ),
        ])
        .unwrap();

        let context = TableContext::new(
            "test_collect_single_multiplicity_element_err".to_string(),
            vec![
                subject_tc,
                SeriesContext::default()
                    .with_identifier(Identifier::from("sex"))
                    .with_data_context(Context::SubjectSex),
            ],
        );
        let cdf = ContextualizedDataFrame::new(context, df).unwrap();

        let sme = cdf
            .get_single_multiplicity_element(&Context::SubjectSex, &Context::None)
            .unwrap()
            .unwrap();
        assert_eq!(sme, "MALE");
    }

    #[rstest]
    fn test_collect_single_multiplicity_element_err() {
        let (subject_col, subject_sc) = generate_minimal_cdf_components(1, 2);
        let context = Context::SubjectAge;

        let df = DataFrame::new(vec![
            subject_col.clone(),
            Column::new("age".into(), &[46, 22]),
        ])
        .unwrap();
        let tc = TableContext::new(
            "test_collect_single_multiplicity_element_err".to_string(),
            vec![
                subject_sc,
                SeriesContext::default()
                    .with_identifier(Identifier::from("age"))
                    .with_data_context(context.clone()),
            ],
        );
        let cdf = ContextualizedDataFrame::new(tc, df).unwrap();

        let sme = cdf.get_single_multiplicity_element(&context, &Context::None);
        assert!(sme.is_err());
    }
}

#[must_use = "Builder must be finalized with .build()"]
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

    pub fn add_series_context(self, sc: SeriesContext) -> Result<Self, StrategyError> {
        self.cdf.context.context_mut().push(sc);

        Ok(self.mark_dirty())
    }

    pub fn replace_column(
        self,
        col_name: &str,
        replacement_data: Series,
    ) -> Result<Self, StrategyError> {
        self.cdf.data.replace(col_name, replacement_data)?;

        Ok(self.mark_dirty())
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
            .collect_owned_names();

        let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        self = self.remove_many_columns(col_refs.as_slice())?;
        self = self.remove_scs_with_context(header_context, data_context);

        Ok(self.mark_dirty())
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
            self.cdf
                .data
                .with_column(col.clone())
                .map_err(|_| StrategyError::BuilderError {
                    transformation: "add column".to_string(),
                    col_name: col.name().to_string(),
                    table_name: table_name.clone(),
                })?;
        }

        self.cdf.context.context_mut().push(sc);

        Ok(self.mark_dirty())
    }

    pub fn bulk_insert_columns_with_series_context(
        mut self,
        inserts: &[(SeriesContext, Vec<Column>)],
    ) -> Result<Self, StrategyError> {
        for (sc, cols) in inserts.iter() {
            self = self.insert_columns_with_series_context(sc.clone(), cols)?;
        }

        Ok(self.mark_dirty())
    }

    pub fn drop_series_context(mut self, sc_id: &Identifier) -> Result<Self, StrategyError> {
        let col_names: Vec<String> = self
            .cdf
            .filter_columns()
            .where_identifier(Filter::Is(sc_id))
            .collect_owned_names();

        let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        self = self.remove_many_columns(col_refs.as_slice())?;
        self = self.remove_series_context(sc_id);

        Ok(self.mark_dirty())
    }

    pub fn drop_many_series_context(
        mut self,
        sc_ids: &[Identifier],
    ) -> Result<Self, StrategyError> {
        for sc_id in sc_ids {
            self = self.drop_series_context(sc_id)?;
        }

        Ok(self.mark_dirty())
    }

    fn remove_series_context(self, to_remove: &Identifier) -> Self {
        self.cdf
            .context
            .context_mut()
            .retain(|sc| sc.get_identifier() != to_remove);
        self
    }

    fn remove_column(self, col_name: &str) -> Result<Self, StrategyError> {
        self.cdf
            .data
            .drop_in_place(col_name)
            .map_err(|_| StrategyError::BuilderError {
                transformation: "drop column".to_string(),
                col_name: col_name.to_string(),
                table_name: self.cdf.context().name().to_string(),
            })?;

        Ok(self.mark_dirty())
    }

    fn remove_many_columns(mut self, col_names: &[&str]) -> Result<Self, StrategyError> {
        for col_name in col_names {
            self = self.remove_column(col_name)?;
        }

        Ok(self.mark_dirty())
    }

    fn remove_scs_with_context(self, header_context: &Context, data_context: &Context) -> Self {
        self.cdf.context.context_mut().retain(|sc| {
            sc.get_header_context() != header_context || sc.get_data_context() != data_context
        });
        self.mark_dirty()
    }

    pub fn cast(
        self,
        header_context: &Context,
        data_context: &Context,
        output_data_type: DataType,
    ) -> Result<Self, StrategyError> {
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

#[cfg(test)]
mod builder_tests {
    use crate::config::context::Context;
    use crate::config::table_context::{Identifier, SeriesContext, TableContext};
    use crate::extract::ContextualizedDataFrame;
    use crate::extract::contextualized_dataframe_filters::Filter;
    use polars::df;
    use polars::frame::DataFrame;
    use polars::prelude::{Column, DataType, NamedFrom, Series};
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
        )
        .unwrap()
    }

    #[fixture]
    fn sample_ctx() -> TableContext {
        TableContext::new(
            "table".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Multi(vec!["subject_id".to_string()]))
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
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        cdf.builder()
            .remove_scs_with_context(&Context::HpoLabelOrId, &Context::ObservationStatus)
            .build_dirty();

        assert_eq!(cdf.context().context().len(), 2);
    }

    #[rstest]
    fn test_remove_scs_with_context_no_change() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        cdf.builder()
            .remove_scs_with_context(&Context::VitalStatus, &Context::None)
            .build_dirty();

        assert_eq!(cdf.context().context().len(), 4);
        assert_eq!(cdf.data().width(), 6);
        assert_eq!(cdf.data().height(), 3);
    }

    #[rstest]
    fn test_remove_scs_and_cols_with_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        cdf.builder()
            .drop_scs_and_cols_with_context(&Context::None, &Context::SubjectId)
            .unwrap()
            .build_dirty();

        assert_eq!(cdf.context().context().len(), 3);
        assert_eq!(cdf.data().width(), 5);
    }

    #[rstest]
    fn test_remove_scs_and_cols_with_context_no_change() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        cdf.builder()
            .drop_scs_and_cols_with_context(&Context::VitalStatus, &Context::None)
            .unwrap()
            .build_dirty();

        assert_eq!(cdf.context().context().len(), 4);
        assert_eq!(cdf.data().width(), 6);
        assert_eq!(cdf.data().height(), 3);
    }

    #[rstest]
    fn test_drop_cols_with_context() {
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
            .drop_scs_and_cols_with_context(&Context::None, &Context::SubjectId)
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
    fn test_remove_column_nonexistent() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let result = cdf.builder().remove_column("nonexistent");
        assert!(result.is_err());
    }

    #[rstest]
    fn test_remove_many_columns() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let expected_width = cdf.data().width() - 2;

        cdf.builder()
            .remove_many_columns(&["different", "age"])
            .unwrap()
            .build_dirty();
        assert_eq!(cdf.data().width(), expected_width);
        assert!(cdf.data().column("different").is_err());
        assert!(cdf.data().column("age").is_err());
        assert!(cdf.data().column("bronchitis").is_ok());
    }

    #[rstest]
    fn test_remove_series_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();

        let expected_len = cdf.series_contexts().len() - 1;
        cdf.builder()
            .remove_series_context(&Identifier::Regex("bronchitis".to_string()))
            .build_dirty();
        assert_eq!(cdf.series_contexts().len(), expected_len);
    }

    #[rstest]
    fn test_drop_series_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let expected_len = cdf.context().context().len() - 1;

        cdf.builder()
            .drop_series_context(&Identifier::Regex("bronchitis".to_string()))
            .unwrap()
            .build_dirty();

        assert!(cdf.data().column("bronchitis").is_err());
        assert_eq!(cdf.series_contexts().len(), expected_len);
    }

    #[rstest]
    fn test_drop_many_series_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let expected_len = cdf.context().context().len() - 2;

        cdf.builder()
            .drop_many_series_context(&[
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
    fn test_insert_columns_with_series_context() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let expected_len = cdf.context().context().len() + 1;
        let expected_width = cdf.data().width() + 1;
        let new_col = Column::new("test_col".into(), &[10, 11, 12]);
        let sc =
            SeriesContext::default().with_identifier(Identifier::Regex("test_col".to_string()));

        cdf.builder()
            .insert_columns_with_series_context(sc, &[new_col])
            .unwrap()
            .build_dirty();

        assert!(cdf.data().column("test_col").is_ok());
        assert_eq!(cdf.series_contexts().len(), expected_len);
        assert_eq!(cdf.data().width(), expected_width);
    }

    #[rstest]
    fn test_bulk_insert_columns_with_series_context() {
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
            .bulk_insert_columns_with_series_context(&inserts)
            .unwrap()
            .build_dirty();

        assert!(cdf.data().column("test_col_1").is_ok());
        assert!(cdf.data().column("test_col_2").is_ok());
        assert_eq!(cdf.series_contexts().len(), expected_len);
        assert_eq!(cdf.data().width(), expected_width);
    }

    #[rstest]
    fn test_replace_column() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        let transformed_vec = vec![1001, 1002, 1003];
        cdf.builder()
            .replace_column(
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
        )
        .unwrap();
        assert_eq!(cdf.data(), &expected_df);
    }

    #[rstest]
    fn test_cast() {
        let df = sample_df();
        let ctx = sample_ctx();
        let mut cdf = ContextualizedDataFrame::new(ctx, df).unwrap();
        cdf.builder()
            .cast(&Context::None, &Context::SubjectAge, DataType::String)
            .unwrap()
            .build_dirty();
        let age_col = cdf.data().column("age").unwrap();
        assert_eq!(age_col.dtype(), &DataType::String);
        assert_eq!(age_col, &Column::new("age".into(), vec!["25", "30", "40"]));
    }
}
