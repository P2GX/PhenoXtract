use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::StrategyError::MappingError;
use crate::transform::error::{MappingErrorInfo, StrategyError};
use crate::transform::traits::Strategy;
use log::info;

use crate::extract::contextualized_dataframe_filters::Filter;

use polars::prelude::{DataType, IntoSeries, PlSmallStr};
use std::any::type_name;
use std::collections::{HashMap, HashSet};
use crate::transform::utils::is_iso8601_duration;

#[allow(dead_code)]
#[derive(Debug)]
/// todo
pub struct AgeToIso8601Strategy {
    min_age: i32,
    max_age: i32,
}

impl AgeToIso8601Strategy {
    pub fn new() -> Self {
        AgeToIso8601Strategy {min_age: 0, max_age: 150}
    }

    fn is_valid_age(&self, age: i32) -> bool{
        age >= self.min_age && age <= self.max_age
    }
}

impl Strategy for AgeToIso8601Strategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context_is_age()
                .collect()
                .is_empty()
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!("Applying AgeToISO8601 strategy to data.");

        let min_age = 0;
        let max_age = 150;

        let i32_to_iso8601: HashMap<i32, String> =
            (min_age..=max_age).map(|n| (n, format!("P{n}Y"))).collect();

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        for table in tables.iter_mut() {
            let column_names: Vec<PlSmallStr> = table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context_is_age()
                .collect()
                .iter()
                .map(|col| col.name())
                .cloned()
                .collect();

            for col_name in column_names {
                let col = table.data().column(&col_name)?;
                let cast_col = col.cast(&DataType::String)?;
                let mapped_column = cast_col.str()?.apply_mut(|cell_value| {
                    if is_iso8601_duration(cell_value) {
                        cell_value
                    } else if let Ok(years) = cell_value.parse::<i32>() && self.is_valid_age(years)  {
                        i32_to_iso8601.get(&years).expect("Age was too high or too low")
                    } else {
                        if !cell_value.is_empty() {
                            let mapping_error_info = MappingErrorInfo {
                                column: col.name().to_string(),
                                table: table.context().name().to_string(),
                                old_value: cell_value.to_string(),
                                possible_mappings: vec![],
                            };
                            if !error_info.contains(&mapping_error_info) {
                                error_info.insert(mapping_error_info);
                            }
                        }
                        cell_value
                    }
                });
                table
                    .builder()
                    .replace_column(&col_name, mapped_column.into_series())?
                    .build()?;
            }
        }

        // return an error if not every cell term could be parsed
        if !error_info.is_empty() {
            Err(MappingError {
                strategy_name: type_name::<Self>().split("::").last().unwrap().to_string(),
                info: error_info.into_iter().collect(),
            })
        } else {
            Ok(())
        }
    }
}