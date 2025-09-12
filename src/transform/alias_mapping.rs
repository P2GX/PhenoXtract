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
                            "Could not insert transformed column {} into table {}.",
                            col_name, table_name,
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
    use crate::config::table_context::AliasMap::{ToInt, ToString};
    use crate::config::table_context::Context::{SubjectAge, SubjectId};
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::transform::alias_mapping::AliasMapTransform;
    use crate::transform::traits::Strategy;
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};
    use std::collections::HashMap;

    #[fixture]
    fn hm1() -> HashMap<String, String> {
        let mut hm1 = HashMap::new();
        hm1.insert(String::from("P002"), String::from("patient 2"));
        hm1.insert(String::from("P004"), String::from("P4"));
        hm1
    }

    #[fixture]
    fn sc1(hm1: HashMap<String, String>) -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("patient_id".to_string()),
            Context::None,
            SubjectId,
            None,
            Some(ToString(hm1)),
            vec![],
        )
    }

    #[fixture]
    fn hm2() -> HashMap<String, i32> {
        let mut hm2 = HashMap::new();
        hm2.insert(String::from("35"), 40);
        hm2
    }

    #[fixture]
    fn sc2(hm2: HashMap<String, i32>) -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex("age".to_string()),
            Context::None,
            SubjectAge,
            None,
            Some(ToInt(hm2)),
            vec![],
        )
    }


    #[fixture]
    fn tc(sc1: SeriesContext, sc2: SeriesContext) -> TableContext {
        TableContext::new("patient_data".to_string(), vec![sc1, sc2])
    }

    #[fixture]
    fn data() -> DataFrame {
        let col1 = Column::new("patient_id".into(), ["P001", "P002", "P003", "P004"]);
        let col2 = Column::new("age".into(), [35, 16, 35, 25]);
        DataFrame::new(vec![col1, col2]).unwrap()
    }

    #[fixture]
    fn cdf(tc: TableContext, data: DataFrame) -> ContextualizedDataFrame {
        ContextualizedDataFrame::new(tc, data)
    }

    #[rstest]
    fn test_transformation(mut cdf: ContextualizedDataFrame) {
        let alias_map_transform = AliasMapTransform {};
        println!("{:?}", cdf);
        alias_map_transform.transform(&mut cdf).unwrap();
        println!("{:?}", cdf);
    }
}
