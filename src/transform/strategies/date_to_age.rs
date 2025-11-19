use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::StrategyError::MappingError;
use crate::transform::error::{MappingErrorInfo, StrategyError};
use crate::transform::traits::Strategy;
use log::{info, warn};

use crate::extract::contextualized_dataframe_filters::Filter;

use std::any::type_name;
use std::collections::HashSet;
use polars::prelude::{AnyValue, Column};
use crate::config::context::{Context, DATE_CONTEXTS};

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

        let dob_table = tables.iter().find(|table|                !table
            .filter_columns()
            .where_data_context(Filter::Is(&Context::DateOfBirth))
            .collect()
            .is_empty()).expect("Unexpectedly could not find table with DateOfBirth data when applying DateToAge strategy.");

        let dob_columns = dob_table
            .filter_columns()
            .where_data_context(Filter::Is(&Context::DateOfBirth))
            .collect();

        let dob_column = dob_columns
            .first()
            .expect("Unexpectedly could not find DateOfBirth column in table.");

        let patient_dob_hash_map = dob_table.create_subject_id_string_data_hash_map(dob_column.str()?);

        for table in tables.iter_mut() {

            let table_name = table.context().name().to_string();

            let stringified_subject_id_col = table.get_subject_id_col().str()?;

            let date_column_names = table
                .filter_columns()
                .where_data_contexts(&DATE_CONTEXTS)
                .collect_owned_names();

            for date_col_name in date_column_names {
                let stringified_date_col = table.data().column(&date_col_name)?.str()?;
                let mut ages = vec![];
                for row_idx in 0..stringified_date_col.len() {

                    let subject_id = stringified_subject_id_col.get(row_idx).expect("Missing SubjectID");
                    let subject_dob = patient_dob_hash_map.get(subject_id).cloned().flatten();

                    let date = stringified_date_col.get(row_idx);

                    if let Some(date) = date {
                        if let Some(subject_dob) = subject_dob {
                            //magic! todo!
                        } else {
                                let mapping_error_info = MappingErrorInfo {
                                    column: date_col_name.to_string(),
                                    table: table_name.clone(),
                                    old_value: date.to_string(),
                                    possible_mappings: vec![],
                                };
                                if !error_info.contains(&mapping_error_info) {
                                    error_info.insert(mapping_error_info);
                                }
                            ages.push(AnyValue::String(date));
                        }
                    } else {
                        ages.push(AnyValue::Null);
                    }

                }
                let ages_column = Column::new(date_col_name.clone().into(), ages);

                table
                    .builder()
                    .replace_column(&date_col_name, ages_column.take_materialized_series())?
                    .build()?;
            }
        }

        // return an error if not every cell term could be parsed
        if !error_info.is_empty() {
            Err(MappingError {
                strategy_name: type_name::<Self>().split("::").last().unwrap().to_string(),
                message: "Cannot convert dates for subjects without date of birth data.".to_string(),
                info: error_info.into_iter().collect(),
            })
        } else {
            Ok(())
        }
    }
}
