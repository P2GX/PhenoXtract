use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::StrategyError::MappingError;
use crate::transform::error::{MappingErrorInfo, StrategyError};
use crate::transform::traits::Strategy;
use log::{info, warn};

use crate::extract::contextualized_dataframe_filters::Filter;

use crate::config::context::{Context, DATE_CONTEXTS};
use chrono::NaiveDate;
use date_differencer::date_diff;
use iso8601_duration::Duration;
use polars::prelude::{AnyValue, Column};
use std::any::type_name;
use std::collections::{HashMap, HashSet};

#[allow(dead_code)]
#[derive(Debug)]
/// This strategy finds columns whose cells contain dates, and converts these dates
/// to a certain age of the patient, by leveraging data on the patient's date of birth.
/// 
/// If there is no data on a certain patient's date of birth, 
/// yet there is a date corresponding to this patient,
/// then an error will be thrown. 
pub struct DateToAgeStrategy;

impl DateToAgeStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DateToAgeStrategy {
    fn default() -> Self {
        DateToAgeStrategy::new()
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
        let exists_date_column = tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_contexts(&DATE_CONTEXTS)
                .collect()
                .is_empty()
        });
        if exists_dob_column && exists_date_column {
            true
        } else if exists_date_column && !exists_dob_column {
            warn!("Date columns were found in the data, yet there was no DateOfBirth column. DateToAge Strategy was not applied.");
            false
        } else {
            false
        }
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!("Applying DateToAge strategy to data.");

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        let patient_dob_hash_map = Self::create_patient_dob_hash_map(tables)?;

        for table in tables.iter_mut() {
            let stringified_subject_id_col = table.get_subject_id_col().str()?.clone();

            let date_column_names = table
                .filter_columns()
                .where_data_contexts(&DATE_CONTEXTS)
                .collect_owned_names();

            for date_col_name in date_column_names.iter() {
                let stringified_date_col = table.data().column(date_col_name)?.str()?;
                let mut ages = vec![];

                for (subject_id_opt, date_opt) in stringified_subject_id_col
                    .iter()
                    .zip(stringified_date_col.iter())
                {
                    let subject_id = subject_id_opt.expect("Missing SubjectID");
                    let subject_dob_opt = patient_dob_hash_map.get(subject_id).cloned().flatten();

                    if date_opt.is_none() {
                        ages.push(AnyValue::Null);
                    } else if let Some(date) = date_opt {
                        if let Some(subject_dob) = subject_dob_opt {
                            let age = Self::date_and_dob_to_age(date, subject_dob)?;
                            ages.push(AnyValue::StringOwned(age.into()));
                        } else {
                            Self::upsert_mapping_error(&mut error_info, date_col_name, table, date);
                            ages.push(AnyValue::String(date));
                        }
                    }
                }
                let ages_column = Column::new(date_col_name.clone().into(), ages);

                table
                    .builder()
                    .replace_column(date_col_name, ages_column.take_materialized_series())?
                    .build()?;
            }
        }

        // return an error if not every cell term could be parsed
        if !error_info.is_empty() {
            Err(MappingError {
                strategy_name: type_name::<Self>().split("::").last().unwrap().to_string(),
                message: "Cannot convert dates for subjects without date of birth data."
                    .to_string(),
                info: error_info.into_iter().collect(),
            })
        } else {
            Ok(())
        }
    }
}

impl DateToAgeStrategy {
    fn create_patient_dob_hash_map(
        tables: &[&mut ContextualizedDataFrame],
    ) -> Result<HashMap<String, Option<String>>, StrategyError> {
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

        Ok(dob_table.create_subject_id_string_data_hash_map(dob_column.str()?))
    }

    fn date_and_dob_to_age(date: &str, dob: String) -> Result<String, StrategyError> {
        let date_object = date
            .parse::<NaiveDate>()
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let dob_object = dob
            .parse::<NaiveDate>()
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let diff = date_diff(date_object, dob_object);
        let dur = Duration::new(
            diff.years as f32,
            diff.months as f32,
            diff.days as f32,
            0f32,
            0f32,
            0f32,
        );
        Ok(dur.to_string())
    }

    fn upsert_mapping_error(
        error_info: &mut HashSet<MappingErrorInfo>,
        date_col_name: &str,
        table: &ContextualizedDataFrame,
        date: &str,
    ) {
        let mapping_error_info = MappingErrorInfo {
            column: date_col_name.to_string(),
            table: table.context().name().to_string(),
            old_value: date.to_string(),
            possible_mappings: vec![],
        };
        if !error_info.contains(&mapping_error_info) {
            error_info.insert(mapping_error_info);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_date_and_dob_age() {
        let iso8601dur =
            DateToAgeStrategy::date_and_dob_to_age("2000-01-01", "2025-11-21".to_string()).unwrap();
        assert_eq!(iso8601dur, "P25Y10M20D");
    }
}
