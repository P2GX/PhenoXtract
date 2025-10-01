use crate::config::table_context::AliasMap;
use crate::config::table_context::AliasMap::{ToBool, ToFloat, ToInt, ToString};
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::strategies::utils::convert_col_to_string_vec;
use crate::transform::traits::Strategy;
use log::info;
use polars::prelude::{AnyValue, Column};
use std::any::type_name;
use std::collections::HashMap;
use std::str::FromStr;

/// Given a contextualised dataframe, this strategy will apply all the aliases
/// found in the SeriesContexts.
/// For example if the Contextualised Dataframe has a SeriesContext consisting of a SubjectSex column
/// and a ToString AliasMap which converts "M" to "Male" and "F" to "Female"
/// then the strategy will apply those aliases to each cell.
/// # NOTE
/// This does not transform the headers of the Dataframe.
#[allow(dead_code)]
#[derive(Debug)]
pub struct AliasMapStrategy;

impl AliasMapStrategy {
    #[allow(unused)]
    ///Applies aliases from a hash map to a vector of strings
    fn map_values<'a, T: FromStr + Copy + Into<AnyValue<'a>>>(
        stringified_col: Vec<String>,
        hm: HashMap<String, T>,
        col_name: &str,
        table_name: &str,
    ) -> Result<Vec<AnyValue<'a>>, TransformError> {
        stringified_col
            .iter()
            .map(|s| match hm.get(s) {
                Some(&alias) => Ok(alias.into()),
                None => {
                    if s == "null" {
                        Ok(AnyValue::Null)
                    } else {
                        let attempted_parsed_val = s.parse::<T>();
                        if let Ok(parsed_val) = attempted_parsed_val {
                            Ok(parsed_val.into())
                        } else {
                            Err(StrategyError(
                            format!(
                                "Could not convert column {} in table {} to a vector of type {}.",
                                col_name,
                                table_name,
                                type_name::<T>()
                            )
                                .to_string(),
                        ))
                        }
                    }
                }
            })
            .collect()
    }

    fn get_col_alias_map_pairs(cdf: &ContextualizedDataFrame) -> Vec<(Column, AliasMap)> {
        let mut col_alias_map_pairs = vec![];
        for series_context in cdf.get_series_contexts() {
            if let Some(am) = series_context.get_alias_map() {
                let cols = cdf.get_columns(series_context.get_identifier());
                for col_ref in cols {
                    col_alias_map_pairs.push((col_ref.clone(), am.clone()))
                }
            }
        }
        col_alias_map_pairs
    }
}

impl Strategy for AliasMapStrategy {
    fn is_valid(&self, _table: &ContextualizedDataFrame) -> bool {
        true
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let table_name = &table.context().name.clone();
        info!("Applying AliasMap strategy to table: {table_name}");

        for (col, alias_map) in AliasMapStrategy::get_col_alias_map_pairs(table) {
            let col_name = col.name();
            info!("Applying AliasMap strategy to column: {col_name}");
            let stringified_col = convert_col_to_string_vec(&col)?;

            match alias_map {
                ToString(hm) => {
                    let transformed_vec = stringified_col
                        .iter()
                        .map(|s| match hm.get(s) {
                            Some(alias) => AnyValue::String(alias),
                            None => {
                                if s == "null" {
                                    AnyValue::Null
                                } else {
                                    AnyValue::String(s)
                                }
                            }
                        })
                        .collect();
                    table.replace_column(transformed_vec, col_name)?;
                    Ok(())
                }
                ToInt(hm) => {
                    let transformed_vec =
                        Self::map_values(stringified_col, hm, col_name, table_name)?;
                    table.replace_column(transformed_vec, col_name)?;
                    Ok(())
                }
                ToFloat(hm) => {
                    let transformed_vec =
                        Self::map_values(stringified_col, hm, col_name, table_name)?;
                    table.replace_column(transformed_vec, col_name)?;
                    Ok(())
                }
                ToBool(hm) => {
                    let transformed_vec =
                        Self::map_values(stringified_col, hm, col_name, table_name)?;
                    table.replace_column(transformed_vec, col_name)?;
                    Ok(())
                }
            }?;
        }

        info!("AliasMap strategy successfully applied to table: {table_name}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::AliasMap::{ToBool, ToFloat, ToInt, ToString};
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::transform::strategies::alias_map::AliasMapStrategy;
    use crate::transform::traits::Strategy;
    use polars::frame::DataFrame;
    use polars::prelude::{AnyValue, Column, DataType};
    use rstest::{fixture, rstest};
    use std::collections::HashMap;

    #[fixture]
    fn sc_string_aliases() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("patient_id".to_string()),
            Context::None,
            Context::SubjectId,
            None,
            Some(ToString(HashMap::from([
                ("P001".to_string(), "patient_1".to_string()),
                ("P002".to_string(), "patient_2".to_string()),
                ("P003".to_string(), "patient_3".to_string()),
                ("P004".to_string(), "patient_4".to_string()),
            ]))),
            None,
        )
    }

    #[fixture]
    fn sc_int_alias() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("age".to_string()),
            Context::None,
            Context::SubjectAge,
            None,
            Some(ToInt(HashMap::from([("11".to_string(), 22)]))),
            None,
        )
    }

    #[fixture]
    fn sc_float_aliases() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("weight".to_string()),
            Context::None,
            Context::WeightInKg,
            None,
            Some(ToFloat(HashMap::from([
                ("10.1".to_string(), 20.2),
                ("20.2".to_string(), 40.4),
                ("30.3".to_string(), 60.6),
                ("40.4".to_string(), 80.8),
            ]))),
            None,
        )
    }

    #[fixture]
    fn sc_bool_alias() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("smokes.".to_string()),
            Context::None,
            Context::SmokerBool,
            None,
            Some(ToBool(HashMap::from([("false".to_string(), true)]))),
            None,
        )
    }

    #[fixture]
    fn sc_convert_to_int_fail() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("patient_id".to_string()),
            Context::None,
            Context::SubjectId,
            None,
            Some(ToInt(HashMap::from([("P001".to_string(), 1001)]))),
            None,
        )
    }

    #[fixture]
    fn sc_convert_to_int_success() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("patient_id".to_string()),
            Context::None,
            Context::SubjectId,
            None,
            Some(ToInt(HashMap::from([
                ("P001".to_string(), 1001),
                ("P002".to_string(), 1002),
                ("P003".to_string(), 1003),
                ("P004".to_string(), 1004),
            ]))),
            None,
        )
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

    #[rstest]
    fn test_map_values() {
        let vec_of_strings = vec![
            "P001".to_string(),
            "P002".to_string(),
            "P003".to_string(),
            "P004".to_string(),
            "null".to_string(),
        ];
        let hm = HashMap::from([
            ("P001".to_string(), 1001),
            ("P002".to_string(), 1002),
            ("P003".to_string(), 1003),
            ("P004".to_string(), 1004),
        ]);
        let mapped_vec =
            AliasMapStrategy::map_values(vec_of_strings, hm, "col_name", "table_name").unwrap();
        assert_eq!(
            mapped_vec,
            vec![
                AnyValue::Int32(1001),
                AnyValue::Int32(1002),
                AnyValue::Int32(1003),
                AnyValue::Int32(1004),
                AnyValue::Null
            ]
        );
    }

    //tests that the alias map makes the desired changes
    #[rstest]
    fn test_aliasing(mut cdf_aliasing: ContextualizedDataFrame) {
        let alias_map_transform = AliasMapStrategy {};
        assert!(alias_map_transform.transform(&mut cdf_aliasing).is_ok());
        assert_eq!(
            cdf_aliasing.data.column("patient_id").unwrap(),
            &Column::new(
                "patient_id".into(),
                ["patient_1", "patient_2", "patient_3", "patient_4"]
            )
        );
        assert_eq!(
            cdf_aliasing.data.column("age").unwrap(),
            &Column::new("age".into(), [22, 22, 33, 44])
        );
        assert_eq!(
            cdf_aliasing.data.column("weight").unwrap(),
            &Column::new("weight".into(), [20.2, 40.4, 60.6, 80.8])
        );
        assert_eq!(
            cdf_aliasing.data.column("smokes1").unwrap(),
            &Column::new("smokes1".into(), [true, true, true, true])
        );
        assert_eq!(
            cdf_aliasing.data.column("smokes2").unwrap(),
            &Column::new("smokes2".into(), [true, true, true, true])
        );
    }

    #[rstest]
    fn test_aliasing_with_nulls(mut cdf_nulls: ContextualizedDataFrame) {
        let alias_map_transform = AliasMapStrategy {};
        assert!(alias_map_transform.transform(&mut cdf_nulls).is_ok());
        assert_eq!(
            cdf_nulls.data.column("patient_id").unwrap(),
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
            cdf_nulls.data.column("age").unwrap(),
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
            cdf_nulls.data.column("weight").unwrap(),
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
            cdf_nulls.data.column("smokes1").unwrap(),
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
            cdf_nulls.data.column("smokes2").unwrap(),
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
        assert!(alias_map_transform.transform(&mut cdf_no_aliasing).is_ok());
        assert_eq!(cdf_no_aliasing.data, df_no_aliasing)
    }

    //tests that we get an error when we unsuccessfully change a column into a different type
    #[rstest]
    fn test_to_int_conversion_error(
        mut cdf_convert_to_int_fail: ContextualizedDataFrame,
        df_aliasing: DataFrame,
    ) {
        let alias_map_transform = AliasMapStrategy {};
        assert!(
            alias_map_transform
                .transform(&mut cdf_convert_to_int_fail)
                .is_err()
        );
        //make sure that nothing has changed despite the error
        assert_eq!(cdf_convert_to_int_fail.data, df_aliasing);
    }

    //tests that we can change column types if we have sufficient aliases
    #[rstest]
    fn test_type_change(mut cdf_convert_to_int_success: ContextualizedDataFrame) {
        let alias_map_transform = AliasMapStrategy {};
        assert!(
            alias_map_transform
                .transform(&mut cdf_convert_to_int_success)
                .is_ok()
        );
        assert_eq!(
            cdf_convert_to_int_success
                .data
                .column("patient_id")
                .unwrap(),
            &Column::new("patient_id".into(), [1001, 1002, 1003, 1004])
        );
        assert_eq!(
            cdf_convert_to_int_success
                .data
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
        let am_string = sc_string_aliases.get_alias_map().clone().unwrap();
        let am_int = sc_int_alias.get_alias_map().clone().unwrap();
        let am_float = sc_float_aliases.get_alias_map().clone().unwrap();
        let am_bool1 = sc_bool_alias.get_alias_map().clone().unwrap();
        let am_bool2 = sc_bool_alias.get_alias_map().clone().unwrap();

        let df = cdf_aliasing.data.clone();
        let col_string = df.column("patient_id").unwrap().clone();
        let col_int = df.column("age").unwrap().clone();
        let col_float = df.column("weight").unwrap().clone();
        let col_bool1 = df.column("smokes1").unwrap().clone();
        let col_bool2 = df.column("smokes2").unwrap().clone();

        let expected_col_alias_map_pairs = vec![
            (col_string, am_string),
            (col_int, am_int),
            (col_float, am_float),
            (col_bool1, am_bool1),
            (col_bool2, am_bool2),
        ];

        let extracted_col_alias_map_pairs =
            AliasMapStrategy::get_col_alias_map_pairs(&cdf_aliasing);
        assert_eq!(extracted_col_alias_map_pairs, expected_col_alias_map_pairs);
    }
}
