use crate::config::table_context::AliasMap::{ToBool, ToFloat, ToInt, ToString};
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::traits::Strategy;
use polars::frame::DataFrame;
use polars::prelude::{AnyValue, NamedFrom, PlSmallStr, Series};
use std::any::type_name;
use std::collections::HashMap;
use std::str::FromStr;

pub struct AliasMapTransform {}

impl Strategy for AliasMapTransform {
    fn is_valid(&self, _table: &ContextualizedDataFrame) -> bool {
        true
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        fn transform_vec_with_hash_map<T: FromStr + Copy>(
            vec_of_strings: Vec<String>,
            hm: HashMap<String, T>,
            col_name: &PlSmallStr,
            table_name: &String,
        ) -> Result<Vec<T>, TransformError> {
            vec_of_strings
                .iter()
                .map(|s| match hm.get(s) {
                    Some(&alias) => Ok(alias),
                    None => s.parse::<T>().map_err(|_e| {
                        StrategyError(
                            format!(
                                "Could not convert column {} in table {} to a vector of type {}.",
                                col_name,
                                table_name,
                                type_name::<T>()
                            )
                            .to_string(),
                        )
                    }),
                })
                .collect()
        }

        fn insert_vec_into_table<'a, T, Phantom: ?Sized>(
            transformed_vec: Vec<T>,
            col_name: &PlSmallStr,
            table: &'a mut ContextualizedDataFrame,
            table_name: &String,
        ) -> Result<&'a mut DataFrame, TransformError>
        where
            Series: NamedFrom<Vec<T>, Phantom>,
        {
            let transformed_series = Series::new(col_name.clone(), transformed_vec);
            table
                .data_mut()
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

        let table_name = &table.context().name.clone();

        let mut col_alias_map_pairs = vec![];
        for series_context in &table.context().context {
            if let Some(am) = series_context.get_alias_map_opt() {
                let cols = table.get_columns(&series_context.identifier);
                for col_ref in cols {
                    col_alias_map_pairs.push((col_ref.clone(), am.clone()))
                }
            }
        }

        for (col, alias_map) in col_alias_map_pairs {
            let col_name = col.name();
            let vec_of_strings = col
                .as_series()
                .ok_or(StrategyError(format!(
                    "Could not convert column {col_name} to a series."
                )))?
                .iter()
                .map(|val| match val {
                    AnyValue::String(s) => s.to_string(),
                    _ => val.to_string(),
                })
                .collect::<Vec<String>>();

            match alias_map {
                ToString(hm) => {
                    let transformed_vec = vec_of_strings
                        .iter()
                        .map(|s| match hm.get(s) {
                            Some(alias) => alias.clone(),
                            None => s.clone(),
                        })
                        .collect();
                    insert_vec_into_table(transformed_vec, col_name, table, table_name)?;
                    Ok(())
                }
                ToInt(hm) => {
                    let transformed_vec =
                        transform_vec_with_hash_map(vec_of_strings, hm, col_name, table_name)?;
                    insert_vec_into_table(transformed_vec, col_name, table, table_name)?;
                    Ok(())
                }
                ToFloat(hm) => {
                    let transformed_vec =
                        transform_vec_with_hash_map(vec_of_strings, hm, col_name, table_name)?;
                    insert_vec_into_table(transformed_vec, col_name, table, table_name)?;
                    Ok(())
                }
                ToBool(hm) => {
                    let transformed_vec =
                        transform_vec_with_hash_map(vec_of_strings, hm, col_name, table_name)?;
                    insert_vec_into_table(transformed_vec, col_name, table, table_name)?;
                    Ok(())
                }
            }?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::AliasMap::{ToBool, ToFloat, ToInt, ToString};
    use crate::config::table_context::Context::{SmokerBool, SubjectAge, SubjectId, WeightInKg};
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::transform::alias_mapping::AliasMapTransform;
    use crate::transform::traits::Strategy;
    use polars::frame::DataFrame;
    use polars::prelude::{Column, DataType};
    use rstest::{fixture, rstest};
    use std::collections::HashMap;

    #[fixture]
    fn sc_P00X_to_patient_X() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("patient_id".to_string()),
            Context::None,
            SubjectId,
            None,
            Some(ToString(HashMap::from([
                ("P001".to_string(), "patient_1".to_string()),
                ("P002".to_string(), "patient_2".to_string()),
                ("P003".to_string(), "patient_3".to_string()),
                ("P004".to_string(), "patient_4".to_string()),
            ]))),
            vec![],
        )
    }

    #[fixture]
    fn sc_11_to_22() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("age".to_string()),
            Context::None,
            SubjectAge,
            None,
            Some(ToInt(HashMap::from([("11".to_string(), 22)]))),
            vec![],
        )
    }

    #[fixture]
    fn sc_doubling() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("weight".to_string()),
            Context::None,
            WeightInKg,
            None,
            Some(ToFloat(HashMap::from([
                ("10.1".to_string(), 20.2),
                ("20.2".to_string(), 40.4),
                ("30.3".to_string(), 60.6),
                ("40.4".to_string(), 80.8),
            ]))),
            vec![],
        )
    }

    #[fixture]
    fn sc_no_false() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("smokes".to_string()),
            Context::None,
            SmokerBool,
            None,
            Some(ToBool(HashMap::from([("false".to_string(), true)]))),
            vec![],
        )
    }

    #[fixture]
    fn sc_convert_to_int_fail() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("patient_id".to_string()),
            Context::None,
            SubjectId,
            None,
            Some(ToInt(HashMap::from([("P001".to_string(), 1001)]))),
            vec![],
        )
    }

    #[fixture]
    fn sc_convert_to_int_success() -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("patient_id".to_string()),
            Context::None,
            SubjectId,
            None,
            Some(ToInt(HashMap::from([
                ("P001".to_string(), 1001),
                ("P002".to_string(), 1002),
                ("P003".to_string(), 1003),
                ("P004".to_string(), 1004),
            ]))),
            vec![],
        )
    }

    #[fixture]
    fn tc(
        sc_P00X_to_patient_X: SeriesContext,
        sc_11_to_22: SeriesContext,
        sc_doubling: SeriesContext,
        sc_no_false: SeriesContext,
    ) -> TableContext {
        TableContext::new(
            "patient_data".to_string(),
            vec![sc_P00X_to_patient_X, sc_11_to_22, sc_doubling, sc_no_false],
        )
    }

    #[fixture]
    fn df1() -> DataFrame {
        let col1 = Column::new("patient_id".into(), ["P001", "P002", "P003", "P004"]);
        let col2 = Column::new("age".into(), [11, 22, 33, 44]);
        let col3 = Column::new("weight".into(), [10.1, 20.2, 30.3, 40.4]);
        let col4 = Column::new("smokes".into(), [true, true, false, false]);
        DataFrame::new(vec![col1, col2, col3, col4]).unwrap()
    }

    #[fixture]
    fn cdf_aliasing(tc: TableContext, df1: DataFrame) -> ContextualizedDataFrame {
        ContextualizedDataFrame::new(tc, df1)
    }

    #[fixture]
    fn df2() -> DataFrame {
        let col1 = Column::new("patient_id".into(), ["P1", "P2", "P3", "P4"]);
        let col2 = Column::new("age".into(), [10, 20, 30, 40]);
        let col3 = Column::new("weight".into(), [10.2, 20.3, 30.4, 40.5]);
        let col4 = Column::new("smokes".into(), [true, true, true, true]);
        DataFrame::new(vec![col1, col2, col3, col4]).unwrap()
    }

    #[fixture]
    fn cdf_no_aliasing(tc: TableContext, df2: DataFrame) -> ContextualizedDataFrame {
        ContextualizedDataFrame::new(tc, df2)
    }

    #[fixture]
    fn cdf_convert_to_int_fail(
        sc_convert_to_int_fail: SeriesContext,
        df1: DataFrame,
    ) -> ContextualizedDataFrame {
        let tc = TableContext::new("patient_data".to_string(), vec![sc_convert_to_int_fail]);
        ContextualizedDataFrame::new(tc, df1)
    }

    #[fixture]
    fn cdf_convert_to_int_success(
        sc_convert_to_int_success: SeriesContext,
        df1: DataFrame,
    ) -> ContextualizedDataFrame {
        let tc = TableContext::new("patient_data".to_string(), vec![sc_convert_to_int_success]);
        ContextualizedDataFrame::new(tc, df1)
    }

    //tests that the alias map makes the desired changes
    #[rstest]
    fn test_aliasing(mut cdf_aliasing: ContextualizedDataFrame) {
        let alias_map_transform = AliasMapTransform {};
        assert_eq!(
            alias_map_transform.transform(&mut cdf_aliasing).is_err(),
            false
        );
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
            cdf_aliasing.data.column("smokes").unwrap(),
            &Column::new("smokes".into(), [true, true, true, true])
        );
    }

    //tests that the alias map makes no change when none of the dataframe elements are keys
    #[rstest]
    fn test_no_aliasing(mut cdf_no_aliasing: ContextualizedDataFrame, df2: DataFrame) {
        let alias_map_transform = AliasMapTransform {};
        assert_eq!(
            alias_map_transform.transform(&mut cdf_no_aliasing).is_err(),
            false
        );
        assert_eq!(cdf_no_aliasing.data, df2)
    }

    //tests that we get an error when we unsuccessfully change a column into a different type
    #[rstest]
    fn test_error(mut cdf_convert_to_int_fail: ContextualizedDataFrame, df1: DataFrame) {
        let alias_map_transform = AliasMapTransform {};
        assert_eq!(
            alias_map_transform
                .transform(&mut cdf_convert_to_int_fail)
                .is_err(),
            true
        );
        //make sure that nothing has changed despite the error
        assert_eq!(cdf_convert_to_int_fail.data, df1);
    }

    //tests that we can change column types if we have sufficient aliases
    #[rstest]
    fn test_type_change(mut cdf_convert_to_int_success: ContextualizedDataFrame) {
        let alias_map_transform = AliasMapTransform {};
        assert_eq!(
            alias_map_transform
                .transform(&mut cdf_convert_to_int_success)
                .is_err(),
            false
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
}
