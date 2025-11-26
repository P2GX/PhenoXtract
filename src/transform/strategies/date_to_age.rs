use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::StrategyError::MappingError;
use crate::transform::error::{MappingErrorInfo, StrategyError};
use crate::transform::traits::Strategy;
use log::{info, warn};

use crate::extract::contextualized_dataframe_filters::Filter;

use crate::config::context::{Context, DATE_CONTEXTS, date_to_age_contexts_hash_map};
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
                .where_data_contexts_are(&DATE_CONTEXTS)
                .collect()
                .is_empty()
        });
        if exists_dob_column && exists_date_column {
            true
        } else if exists_date_column && !exists_dob_column {
            warn!(
                "Date columns were found in the data, yet there was no DateOfBirth column. DateToAge Strategy was not applied."
            );
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
                .where_data_contexts_are(&DATE_CONTEXTS)
                .collect_owned_names();

            for date_col_name in date_column_names.iter() {
                let stringified_date_col = table.data().column(date_col_name)?.str()?;

                let subject_id_date_zip = stringified_subject_id_col
                    .iter()
                    .zip(stringified_date_col.iter());

                let ages: Vec<AnyValue> = subject_id_date_zip
                    .map(|(subject_id_opt, date_opt)| {
                        let subject_id =
                            subject_id_opt.expect("SubjectID column should have no gaps.");
                        let subject_dob_opt = patient_dob_hash_map.get(subject_id).cloned();

                        if let Some(date) = date_opt {
                            if let Some(subject_dob) = subject_dob_opt
                                && let Ok(age) = Self::date_and_dob_to_age(subject_dob, date)
                            {
                                AnyValue::StringOwned(age.into())
                            } else {
                                Self::upsert_mapping_error(
                                    &mut error_info,
                                    date_col_name,
                                    table,
                                    date,
                                );
                                AnyValue::String(date)
                            }
                        } else {
                            AnyValue::Null
                        }
                    })
                    .collect();

                let ages_column = Column::new(date_col_name.clone().into(), ages);

                table
                    .builder()
                    .replace_column(date_col_name, ages_column.take_materialized_series())?
                    .build()?;
            }

            let cdf_builder = table.builder();
            cdf_builder
                .change_data_contexts_via_hm(date_to_age_contexts_hash_map())
                .build()?;
        }

        // return an error if not every cell term could be parsed
        if !error_info.is_empty() {
            Err(MappingError {
                strategy_name: type_name::<Self>().split("::").last().unwrap().to_string(),
                message: "DOB data is missing, or DOB/date could not be parsed as NaiveDate."
                    .to_string(),
                info: error_info.into_iter().collect(),
            })
        } else {
            Ok(())
        }
    }
}

impl DateToAgeStrategy {
    /// The date of birth column in the data will be found
    /// and a patient-DOB HashMap is constructed
    fn create_patient_dob_hash_map(
        tables: &[&mut ContextualizedDataFrame],
    ) -> Result<HashMap<String, String>, StrategyError> {
        let dob_table = tables.iter().find(|table|                !table
            .filter_columns()
            .where_data_context(Filter::Is(&Context::DateOfBirth))
            .collect()
            .is_empty()).expect("Unexpectedly could not find table with DateOfBirth data when applying DateToAge strategy.");

        let dob_columns = dob_table
            .filter_columns()
            .where_data_context(Filter::Is(&Context::DateOfBirth))
            .collect();

        let dob_col_name = dob_columns
            .first()
            .expect("Unexpectedly could not find DateOfBirth column in table.")
            .name()
            .as_str();

        Ok(dob_table.create_subject_id_string_data_hash_map(dob_col_name)?)
    }

    /// Given the date of birth of a patient, and a date in their life
    /// this will calculate the age of a patient at that date.
    ///
    /// An error will be thrown if the date of birth, or the date, cannot be interpreted as
    /// chrono::NaiveDate.
    fn date_and_dob_to_age(dob: String, date: &str) -> Result<String, StrategyError> {
        let dob_object = dob.parse::<NaiveDate>()?.and_hms_opt(0, 0, 0).unwrap();
        let date_object = date.parse::<NaiveDate>()?.and_hms_opt(0, 0, 0).unwrap();
        let diff = date_diff(dob_object, date_object);
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
    use crate::config::TableContext;
    use crate::config::context::AGE_CONTEXTS;
    use crate::config::table_context::Identifier::Regex;
    use crate::config::table_context::SeriesContext;
    use polars::df;
    use polars::frame::DataFrame;
    use rstest::{fixture, rstest};

    #[fixture]
    fn dob_alice() -> String {
        "1995-06-01".to_string()
    }

    #[fixture]
    fn dob_bob() -> String {
        "1990-12-01".to_string()
    }

    #[fixture]
    fn dob_charlie() -> String {
        "1980-01-08".to_string()
    }

    #[fixture]
    fn onset_bob() -> String {
        "1991-12-01".to_string()
    }

    #[fixture]
    fn onset_charlie() -> String {
        "2025-11-25".to_string()
    }

    #[fixture]
    fn df1() -> DataFrame {
        df!(
        "subject_id" => &["Alice", "Bob", "Charlie"],
        "DOB" => &[AnyValue::String(dob_alice().as_str()), AnyValue::String(dob_bob().as_str()), AnyValue::String(dob_charlie().as_str())],
        "bronchitis" => &["Observed", "Not observed", "Observed"],
        )
            .unwrap()
    }

    #[fixture]
    fn tc1() -> TableContext {
        TableContext::new(
            "table1".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Regex("subject_id".to_string()))
                    .with_data_context(Context::SubjectId),
                SeriesContext::default()
                    .with_identifier(Regex("DOB".to_string()))
                    .with_data_context(Context::DateOfBirth),
                SeriesContext::default()
                    .with_identifier(Regex("bronchitis".to_string()))
                    .with_header_context(Context::HpoLabelOrId)
                    .with_data_context(Context::ObservationStatus),
            ],
        )
    }

    #[fixture]
    fn df2() -> DataFrame {
        df!(
        "subject_id" => &["Alice", "Bob", "Charlie"],
        "pneumonia" => &["Not observed", "Not observed", "Observed"],
        "onset" => &[AnyValue::Null, AnyValue::String(onset_bob().as_str()), AnyValue::String(onset_charlie().as_str())],
        )
            .unwrap()
    }

    #[fixture]
    fn tc2() -> TableContext {
        TableContext::new(
            "table2".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Regex("subject_id".to_string()))
                    .with_data_context(Context::SubjectId),
                SeriesContext::default()
                    .with_identifier(Regex("pneumonia".to_string()))
                    .with_header_context(Context::HpoLabelOrId)
                    .with_data_context(Context::ObservationStatus),
                SeriesContext::default()
                    .with_identifier(Regex("onset".to_string()))
                    .with_data_context(Context::OnsetDateTime),
            ],
        )
    }

    #[rstest]
    fn test_date_to_age_strategy() {
        let mut cdf1 = ContextualizedDataFrame::new(tc1(), df1()).unwrap();
        let mut cdf2 = ContextualizedDataFrame::new(tc2(), df2()).unwrap();
        let tables = &mut [&mut cdf1, &mut cdf2];
        let date_to_age_strat = DateToAgeStrategy;
        date_to_age_strat.transform(tables).unwrap();

        //check the transformation is as expected
        let onset_col = cdf2.data().column("onset").unwrap();
        assert_eq!(
            onset_col,
            &Column::new(
                "onset".into(),
                vec![
                    AnyValue::Null,
                    AnyValue::String("P1Y"),
                    AnyValue::String("P45Y10M17D")
                ]
            )
        );

        //check the change of contexts has succeeded
        assert_eq!(
            cdf2.filter_series_context()
                .where_data_contexts_are(&DATE_CONTEXTS)
                .collect()
                .len(),
            0
        );
        assert_eq!(
            cdf2.filter_series_context()
                .where_data_contexts_are(&AGE_CONTEXTS)
                .collect()
                .len(),
            1
        );
    }

    #[rstest]
    fn test_date_and_dob_age() {
        let onset_age_bob =
            DateToAgeStrategy::date_and_dob_to_age(dob_bob(), onset_bob().as_str()).unwrap();
        assert_eq!(onset_age_bob, "P1Y");

        let onset_age_charlie =
            DateToAgeStrategy::date_and_dob_to_age(dob_charlie(), onset_charlie().as_str())
                .unwrap();
        assert_eq!(onset_age_charlie, "P45Y10M17D");
    }

    #[rstest]
    fn test_date_and_dob_age_err() {
        let result = DateToAgeStrategy::date_and_dob_to_age("2000-13-50".to_string(), "2025-11-21");
        assert!(result.is_err());
    }

    #[rstest]
    fn test_create_patient_dob_hash_map() {
        let mut cdf1 = ContextualizedDataFrame::new(tc1(), df1()).unwrap();
        let mut cdf2 = ContextualizedDataFrame::new(tc2(), df2()).unwrap();
        let tables = [&mut cdf1, &mut cdf2];
        let patient_dob_hm = DateToAgeStrategy::create_patient_dob_hash_map(&tables).unwrap();
        assert_eq!(patient_dob_hm.len(), 3);
        assert_eq!(patient_dob_hm["Alice"], dob_alice());
        assert_eq!(patient_dob_hm["Bob"], dob_bob());
        assert_eq!(patient_dob_hm["Charlie"], dob_charlie());
    }
}
