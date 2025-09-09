use crate::config::table_context::AliasMap::{
    StringToBool, StringToFloat, StringToInt, StringToString,
};
use crate::config::table_context::SeriesContext;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::traits::Strategy;
use polars::prelude::{NamedFrom, Series};
use std::num::{ParseFloatError, ParseIntError};
use std::str::ParseBoolError;

pub struct AliasMapTransform {}

impl Strategy for AliasMapTransform {
    fn is_valid(&self, _table: &ContextualizedDataFrame) -> bool {
        true
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let mut col_name_am_pairs = vec![];
        for series_context in &table.context().context {
            if let Some(cc) = series_context.get_cell_context_option() {
                if let Some(am) = cc.get_alias_map() {
                    //todo currently just implementing this for single_sc because I don't yet know how to work with regex
                    if let SeriesContext::Single(single_sc) = series_context {
                        col_name_am_pairs.push((single_sc.identifier.clone(), am.clone()));
                    }
                }
            }
        }

        for (col_name, alias_map) in col_name_am_pairs {
            let table_name = &table.context().name;
            let col_search_result = table.data().column(&col_name);
            match col_search_result {
                Ok(col) => {
                    let vec_of_strings = col
                        .as_series()
                        .ok_or(StrategyError(
                            "Could not convert a column to a series.".to_string(),
                        ))?
                        .iter()
                        .map(|val| val.to_string())
                        .collect::<Vec<String>>();

                    match alias_map {
                        StringToString(hm) => {
                            let transformed_vec: Vec<String> = vec_of_strings
                                .iter()
                                .map(|str| match hm.get(str) {
                                    Some(alias) => alias.clone(),
                                    None => str.clone(),
                                })
                                .collect();
                            let transformed_s =
                                Series::new(col_name.clone().into(), transformed_vec);
                            table.data_mut().replace(&col_name, transformed_s).unwrap();
                            Ok(())
                        }
                        StringToInt(hm) => {
                            let transformed_vec_result: Result<Vec<i64>, ParseIntError> =
                                vec_of_strings
                                    .iter()
                                    .map(|str| match hm.get(str) {
                                        Some(alias) => Ok(*alias),
                                        None => str.parse::<i64>(),
                                    })
                                    .collect();
                            match transformed_vec_result {
                                Ok(transformed_vec) => {
                                    let transformed_s =
                                        Series::new(col_name.clone().into(), transformed_vec);
                                    table.data_mut().replace(&col_name, transformed_s).unwrap();
                                    Ok(())
                                }
                                Err(_e) => Err(StrategyError(
                                    format!(
                                        "Could not convert {col_name} in table {table_name} to i64."
                                    )
                                    .to_string(),
                                )),
                            }
                        }
                        StringToFloat(hm) => {
                            let transformed_vec_result: Result<Vec<f64>, ParseFloatError> =
                                vec_of_strings
                                    .iter()
                                    .map(|str| match hm.get(str) {
                                        Some(alias) => Ok(*alias),
                                        None => str.parse::<f64>(),
                                    })
                                    .collect();
                            match transformed_vec_result {
                                Ok(transformed_vec) => {
                                    let transformed_s =
                                        Series::new(col_name.clone().into(), transformed_vec);
                                    table.data_mut().replace(&col_name, transformed_s).unwrap();
                                    Ok(())
                                }
                                Err(_e) => Err(StrategyError(
                                    format!(
                                        "Could not convert {col_name} in table {table_name} to f64."
                                    )
                                    .to_string(),
                                )),
                            }
                        }
                        StringToBool(hm) => {
                            let transformed_vec_result: Result<Vec<bool>, ParseBoolError> =
                                vec_of_strings
                                    .iter()
                                    .map(|str| match hm.get(str) {
                                        Some(alias) => Ok(*alias),
                                        None => str.parse::<bool>(),
                                    })
                                    .collect();
                            match transformed_vec_result {
                                Ok(transformed_vec) => {
                                    let transformed_s =
                                        Series::new(col_name.clone().into(), transformed_vec);
                                    table.data_mut().replace(&col_name, transformed_s).unwrap();
                                    Ok(())
                                }
                                Err(_e) => Err(StrategyError(
                                    format!(
                                        "Could not convert {col_name} in table {table_name} to boolean."
                                    )
                                        .to_string(),
                                )),
                            }
                        }
                    }?
                }
                Err(_e) => {
                    return Err(StrategyError(
                        format!("Could not find column {col_name} in table {table_name}.")
                            .to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}
