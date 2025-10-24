use crate::config::table_context::{AliasMap, OutputDataType};
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::{DataProcessingError, StrategyError};
use crate::transform::traits::Strategy;
use crate::transform::utils::polars_column_cast_specific;
use log::info;
use polars::datatypes::{DataType, PlSmallStr};
use polars::prelude::Column;
use std::borrow::Cow;

/// Given a collection of contextualised dataframes, this strategy will apply all the aliases
/// found in the SeriesContexts.
/// For example if a Contextualised Dataframe has a SeriesContext consisting of a SubjectSex column
/// and a ToString AliasMap which converts "M" to "Male" and "F" to "Female"
/// then the strategy will apply those aliases to each cell.
/// # NOTE
/// This does not transform the headers of the Dataframe.
#[allow(dead_code)]
#[derive(Debug)]
pub struct AliasMapStrategy;

impl AliasMapStrategy {
    fn get_col_name_alias_map_pairs(cdf: &ContextualizedDataFrame) -> Vec<(PlSmallStr, AliasMap)> {
        let mut col_name_alias_map_pairs = vec![];
        for series_context in cdf.series_contexts() {
            if let Some(am) = series_context.get_alias_map() {
                let cols = cdf.get_columns(series_context.get_identifier());
                for col in cols {
                    col_name_alias_map_pairs.push((col.name().clone(), am.clone()))
                }
            }
        }
        col_name_alias_map_pairs
    }
}

impl Strategy for AliasMapStrategy {
    fn is_valid(&self, _tables: &[&mut ContextualizedDataFrame]) -> bool {
        true
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!("Applying AliasMap strategy to data.");

        for table in tables.iter_mut() {
            let table_name = table.context().name().to_string();
            info!("Applying AliasMap strategy to table: {table_name}");

            let col_name_alias_pairs = AliasMapStrategy::get_col_name_alias_map_pairs(table);

            for (col_name, alias_map) in col_name_alias_pairs {
                info!("Applying AliasMap strategy to column: {col_name}.");

                let original_column = table.data().column(&col_name).unwrap();

                let stringified_col: Cow<Column> = if original_column.dtype() != &DataType::String {
                    let casted_col = original_column.cast(&DataType::String).map_err(|_| {
                        DataProcessingError::CastingError {
                            col_name: col_name.to_string(),
                            from: original_column.dtype().clone(),
                            to: DataType::String,
                        }
                    })?;
                    Cow::Owned(casted_col)
                } else {
                    Cow::Borrowed(original_column)
                };

                let hash_map = alias_map.get_hash_map();

                let aliased_string_chunked = stringified_col.str()?.apply_mut(|s| {
                    if s.is_empty() {
                        return s;
                    }
                    match hash_map.get(s) {
                        Some(alias) => alias,
                        None => s,
                    }
                });

                let aliased_col = Column::new(col_name.clone(), aliased_string_chunked);

                let desired_output_dtype = alias_map.get_output_dtype();
                let recast_series = if desired_output_dtype == &OutputDataType::String {
                    aliased_col.take_materialized_series()
                } else {
                    polars_column_cast_specific(&aliased_col, desired_output_dtype)?
                        .take_materialized_series()
                };

                table
                    .data_mut()
                    .replace(&col_name, recast_series)
                    .map_err(|_| StrategyError::TransformationError {
                        transformation: "replace".to_string(),
                        col_name: col_name.to_string(),
                        table_name: table_name.to_string(),
                    })?;
            }

            info!("AliasMap strategy successfully applied to table: {table_name}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::{
        AliasMap, Context, Identifier, OutputDataType, SeriesContext, TableContext,
    };
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::transform::strategies::alias_map::AliasMapStrategy;
    use crate::transform::traits::Strategy;
    use polars::datatypes::DataType;
    use polars::frame::DataFrame;
    use polars::prelude::{AnyValue, Column};
    use rstest::{fixture, rstest};
    use std::collections::HashMap;

    #[fixture]
    fn sc_string_aliases() -> SeriesContext {
        SeriesContext::default()
            .with_identifier(Identifier::Regex("patient_id".to_string()))
            .with_data_context(Context::SubjectId)
            .with_alias_map(Some(AliasMap::new(
                HashMap::from([
                    ("P001".to_string(), "patient_1".to_string()),
                    ("P002".to_string(), "patient_2".to_string()),
                    ("P003".to_string(), "patient_3".to_string()),
                    ("P004".to_string(), "patient_4".to_string()),
                ]),
                OutputDataType::String,
            )))
    }

    #[fixture]
    fn sc_int_alias() -> SeriesContext {
        SeriesContext::default()
            .with_identifier(Identifier::Regex("age".to_string()))
            .with_data_context(Context::SubjectAge)
            .with_alias_map(Some(AliasMap::new(
                HashMap::from([("11".to_string(), "22".to_string())]),
                OutputDataType::Int32,
            )))
    }

    #[fixture]
    fn sc_float_aliases() -> SeriesContext {
        SeriesContext::default()
            .with_identifier(Identifier::Regex("weight".to_string()))
            .with_data_context(Context::WeightInKg)
            .with_alias_map(Some(AliasMap::new(
                HashMap::from([
                    ("10.1".to_string(), "20.2".to_string()),
                    ("20.2".to_string(), "40.4".to_string()),
                    ("30.3".to_string(), "60.6".to_string()),
                    ("40.4".to_string(), "80.8".to_string()),
                ]),
                OutputDataType::Float64,
            )))
    }

    #[fixture]
    fn sc_bool_alias() -> SeriesContext {
        SeriesContext::default()
            .with_identifier(Identifier::Regex("smokes.".to_string()))
            .with_data_context(Context::SmokerBool)
            .with_alias_map(Some(AliasMap::new(
                HashMap::from([("false".to_string(), "true".to_string())]),
                OutputDataType::Boolean,
            )))
    }

    #[fixture]
    fn sc_convert_to_int_fail() -> SeriesContext {
        SeriesContext::default()
            .with_identifier(Identifier::Regex("patient_id".to_string()))
            .with_data_context(Context::SubjectId)
            .with_alias_map(Some(AliasMap::new(
                HashMap::from([("P001".to_string(), "1001".to_string())]),
                OutputDataType::Int32,
            )))
    }

    #[fixture]
    fn sc_convert_to_int_success() -> SeriesContext {
        SeriesContext::default()
            .with_identifier(Identifier::Regex("patient_id".to_string()))
            .with_data_context(Context::SubjectId)
            .with_alias_map(Some(AliasMap::new(
                HashMap::from([
                    ("P001".to_string(), "1001".to_string()),
                    ("P002".to_string(), "1002".to_string()),
                    ("P003".to_string(), "1003".to_string()),
                    ("P004".to_string(), "1004".to_string()),
                ]),
                OutputDataType::Int32,
            )))
    }

    #[fixture]
    fn tc(
        sc_string_aliases: SeriesContext,
        sc_int_alias: SeriesContext,
        sc_float_aliases: SeriesContext,
        sc_bool_alias: SeriesContext,
    ) -> TableContext {
        TableContext::new(
            "patient_data".to_string(),
            vec![
                sc_string_aliases,
                sc_int_alias,
                sc_float_aliases,
                sc_bool_alias,
            ],
        )
    }

    #[fixture]
    fn df_aliasing() -> DataFrame {
        let col1 = Column::new("patient_id".into(), ["P001", "P002", "P003", "P004"]);
        let col2 = Column::new("age".into(), [11, 22, 33, 44]);
        let col3 = Column::new("weight".into(), [10.1, 20.2, 30.3, 40.4]);
        let col4 = Column::new("smokes1".into(), [true, true, false, false]);
        let col5 = Column::new("smokes2".into(), [true, true, false, false]);
        DataFrame::new(vec![col1, col2, col3, col4, col5]).unwrap()
    }

    #[fixture]
    fn cdf_aliasing(tc: TableContext, df_aliasing: DataFrame) -> ContextualizedDataFrame {
        ContextualizedDataFrame::new(tc, df_aliasing)
    }

    #[fixture]
    fn df_no_aliasing() -> DataFrame {
        let col1 = Column::new("patient_id".into(), ["P1", "P2", "P3", "P4"]);
        let col2 = Column::new("age".into(), [10, 20, 30, 40]);
        let col3 = Column::new("weight".into(), [10.2, 20.3, 30.4, 40.5]);
        let col4 = Column::new("smokes".into(), [true, true, true, true]);
        DataFrame::new(vec![col1, col2, col3, col4]).unwrap()
    }

    #[fixture]
    fn cdf_no_aliasing(tc: TableContext, df_no_aliasing: DataFrame) -> ContextualizedDataFrame {
        ContextualizedDataFrame::new(tc, df_no_aliasing)
    }

    #[fixture]
    fn cdf_convert_to_int_fail(
        sc_convert_to_int_fail: SeriesContext,
        df_aliasing: DataFrame,
    ) -> ContextualizedDataFrame {
        let tc = TableContext::new("patient_data".to_string(), vec![sc_convert_to_int_fail]);
        ContextualizedDataFrame::new(tc, df_aliasing)
    }

    #[fixture]
    fn cdf_convert_to_int_success(
        sc_convert_to_int_success: SeriesContext,
        df_aliasing: DataFrame,
    ) -> ContextualizedDataFrame {
        let tc = TableContext::new("patient_data".to_string(), vec![sc_convert_to_int_success]);
        ContextualizedDataFrame::new(tc, df_aliasing)
    }

    #[fixture]
    fn df_nulls() -> DataFrame {
        let col1 = Column::new(
            "patient_id".into(),
            [
                AnyValue::String("P001"),
                AnyValue::String("P002"),
                AnyValue::Null,
                AnyValue::String("P004"),
            ],
        );
        let col2 = Column::new(
            "age".into(),
            [
                AnyValue::Null,
                AnyValue::Int32(22),
                AnyValue::Null,
                AnyValue::Int32(44),
            ],
        );
        let col3 = Column::new(
            "weight".into(),
            [
                AnyValue::Float64(10.1),
                AnyValue::Float64(20.2),
                AnyValue::Float64(30.3),
                AnyValue::Null,
            ],
        );
        let col4 = Column::new(
            "smokes1".into(),
            [
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,
                AnyValue::Null,
            ],
        );
        let col5 = Column::new(
            "smokes2".into(),
            [
                AnyValue::Boolean(true),
                AnyValue::Null,
                AnyValue::Boolean(false),
                AnyValue::Boolean(false),
            ],
        );
        DataFrame::new(vec![col1, col2, col3, col4, col5]).unwrap()
    }

    #[fixture]
    fn cdf_nulls(tc: TableContext, df_nulls: DataFrame) -> ContextualizedDataFrame {
        ContextualizedDataFrame::new(tc, df_nulls)
    }

    //tests that the alias map makes the desired changes
    #[rstest]
    fn test_aliasing(mut cdf_aliasing: ContextualizedDataFrame) {
        let alias_map_transform = AliasMapStrategy {};

        alias_map_transform
            .transform(&mut [&mut cdf_aliasing])
            .unwrap();

        assert_eq!(
            cdf_aliasing.clone().data().column("patient_id").unwrap(),
            &Column::new(
                "patient_id".into(),
                ["patient_1", "patient_2", "patient_3", "patient_4"]
            )
        );
        assert_eq!(
            cdf_aliasing.clone().data().column("age").unwrap(),
            &Column::new("age".into(), [22, 22, 33, 44])
        );
        assert_eq!(
            cdf_aliasing.clone().data().column("weight").unwrap(),
            &Column::new("weight".into(), [20.2, 40.4, 60.6, 80.8])
        );
        assert_eq!(
            cdf_aliasing.clone().data().column("smokes1").unwrap(),
            &Column::new("smokes1".into(), [true, true, true, true])
        );
        assert_eq!(
            cdf_aliasing.clone().data().column("smokes2").unwrap(),
            &Column::new("smokes2".into(), [true, true, true, true])
        );
    }

    #[rstest]
    fn test_aliasing_with_nulls(mut cdf_nulls: ContextualizedDataFrame) {
        let alias_map_transform = AliasMapStrategy {};
        alias_map_transform
            .transform(&mut [&mut cdf_nulls])
            .unwrap();
        assert_eq!(
            cdf_nulls.data().column("patient_id").unwrap(),
            &Column::new(
                "patient_id".into(),
                [
                    AnyValue::String("patient_1"),
                    AnyValue::String("patient_2"),
                    AnyValue::Null,
                    AnyValue::String("patient_4")
                ]
            )
        );
        assert_eq!(
            cdf_nulls.data().column("age").unwrap(),
            &Column::new(
                "age".into(),
                [
                    AnyValue::Null,
                    AnyValue::Int32(22),
                    AnyValue::Null,
                    AnyValue::Int32(44)
                ]
            )
        );
        assert_eq!(
            cdf_nulls.data().column("weight").unwrap(),
            &Column::new(
                "weight".into(),
                [
                    AnyValue::Float64(20.2),
                    AnyValue::Float64(40.4),
                    AnyValue::Float64(60.6),
                    AnyValue::Null
                ]
            )
        );
        assert_eq!(
            cdf_nulls.data().column("smokes1").unwrap(),
            &Column::new(
                "smokes1".into(),
                [
                    AnyValue::Null,
                    AnyValue::Null,
                    AnyValue::Null,
                    AnyValue::Null
                ]
            )
        );
        assert_eq!(
            cdf_nulls.data().column("smokes2").unwrap(),
            &Column::new(
                "smokes2".into(),
                [
                    AnyValue::Boolean(true),
                    AnyValue::Null,
                    AnyValue::Boolean(true),
                    AnyValue::Boolean(true)
                ]
            )
        );
    }

    //tests that the alias map makes no change when none of the dataframe elements are keys
    #[rstest]
    fn test_no_aliasing(mut cdf_no_aliasing: ContextualizedDataFrame, df_no_aliasing: DataFrame) {
        let alias_map_transform = AliasMapStrategy {};
        assert!(
            alias_map_transform
                .transform(&mut [&mut cdf_no_aliasing])
                .is_ok()
        );
        assert_eq!(cdf_no_aliasing.into_data(), df_no_aliasing)
    }

    //tests that we can change column types if we have sufficient aliases
    #[rstest]
    fn test_type_change(mut cdf_convert_to_int_success: ContextualizedDataFrame) {
        let alias_map_transform = AliasMapStrategy {};
        assert!(
            alias_map_transform
                .transform(&mut [&mut cdf_convert_to_int_success])
                .is_ok()
        );
        assert_eq!(
            cdf_convert_to_int_success
                .data()
                .column("patient_id")
                .unwrap(),
            &Column::new("patient_id".into(), [1001, 1002, 1003, 1004])
        );
        assert_eq!(
            cdf_convert_to_int_success
                .data()
                .column("patient_id")
                .unwrap()
                .dtype(),
            &DataType::Int32
        )
    }

    #[rstest]
    fn test_get_column_alias_map_pairs(
        cdf_aliasing: ContextualizedDataFrame,
        sc_string_aliases: SeriesContext,
        sc_int_alias: SeriesContext,
        sc_float_aliases: SeriesContext,
        sc_bool_alias: SeriesContext,
    ) {
        let df = cdf_aliasing.data().clone();
        let col_string = df.column("patient_id").unwrap().name().clone();
        let col_int = df.column("age").unwrap().name().clone();
        let col_float = df.column("weight").unwrap().name().clone();
        let col_bool1 = df.column("smokes1").unwrap().name().clone();
        let col_bool2 = df.column("smokes2").unwrap().name().clone();

        let expected_col_name_alias_map_pairs = vec![
            (
                col_string,
                sc_string_aliases.get_alias_map().unwrap().clone(),
            ),
            (col_int, sc_int_alias.get_alias_map().unwrap().clone()),
            (col_float, sc_float_aliases.get_alias_map().unwrap().clone()),
            (col_bool1, sc_bool_alias.get_alias_map().unwrap().clone()),
            (col_bool2, sc_bool_alias.get_alias_map().unwrap().clone()),
        ];

        let extracted_col_name_alias_map_pairs =
            AliasMapStrategy::get_col_name_alias_map_pairs(&cdf_aliasing);
        assert_eq!(
            expected_col_name_alias_map_pairs,
            extracted_col_name_alias_map_pairs
        );
    }
}
