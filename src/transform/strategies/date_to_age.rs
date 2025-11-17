use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::transform::error::StrategyError::MappingError;
use crate::transform::error::{MappingErrorInfo, StrategyError};
use crate::transform::traits::Strategy;
use log::{info, warn};

use crate::extract::contextualized_dataframe_filters::Filter;

use polars::prelude::{DataType, IntoSeries, PlSmallStr};
use std::any::type_name;
use std::collections::HashSet;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug)]
/// todo!
pub struct DateToAgeStrategy;

impl DateToAgeStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl Strategy for DateToAgeStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        let exists_dob_column = tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&Context::DateOfBirth))
                .collect()
                .is_empty()
        });
        if exists_dob_column {
            true
        } else {
            warn!("No DateOfBirth column found in data. DateToAge Strategy cannot be applied.");
            false
        }
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!("Applying DateToAge strategy to data.");

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        let mut dob_columns = vec![];

        for table in tables.iter() {
            let table_dob_columns = table.filter_columns().where_data_context(Filter::Is(&Context::DateOfBirth)).collect();
            dob_columns.extend(table_dob_columns);
        }

        // there should be validation so I can do this
        let dob_column = dob_columns.first().unwrap();

        let patient_dob_hash_map;
        // need to zip together the subject IDs and Dobs
        // there should be an optional column name filter in the strategy if you want to apply it more specifically
        // (possibly also for other strategies?)

        for table in tables.iter_mut() {
            let column_names: Vec<PlSmallStr> = table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&self.data_context))
                .collect()
                .iter()
                .map(|col| col.name())
                .cloned()
                .collect();

            for col_name in column_names {
                let col = table.data().column(&col_name)?;
                let mapped_column = col.str()?.apply_mut(|cell_value| {
                    if self.ontology_dict.is_id(cell_value) {
                        cell_value
                    } else if let Some(curie_id) = self.ontology_dict.get(cell_value) {
                        curie_id
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
