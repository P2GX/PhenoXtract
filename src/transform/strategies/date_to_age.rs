use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::StrategyError::MappingError;
use crate::transform::error::{MappingErrorInfo, PushMappingError, StrategyError};
use log::{info, warn};

use crate::extract::contextualized_dataframe_filters::Filter;

use crate::config::context::{AGE_CONTEXTS, Context};

use crate::transform::data_processing::parsing::{
    try_parse_string_date, try_parse_string_datetime,
};
use crate::transform::strategies::traits::Strategy;
use chrono::NaiveDateTime;
use date_differencer::date_diff;
use iso8601_duration::Duration;
use polars::prelude::{AnyValue, Column, DataType};
use std::any::type_name;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

const DATE_CONTEXTS_WITHOUT_DOB: [Context; 3] = [
    Context::DateAtLastEncounter,
    Context::OnsetDate,
    Context::DateOfDeath,
];

#[allow(dead_code)]
#[derive(Debug, Default)]
/// This strategy finds columns whose cells contain dates, and converts these dates
/// to a certain age of the patient, by leveraging the patient's date of birth.
///
/// If there is no data on a certain patient's date of birth,
/// yet there is a date corresponding to this patient,
/// then an error will be thrown.
pub struct DateToAgeStrategy;

impl Strategy for DateToAgeStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        let has_dob_column = tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&Context::DateOfBirth))
                .collect()
                .is_empty()
        });
        let has_date_column = tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_contexts_are(&DATE_CONTEXTS_WITHOUT_DOB)
                .collect()
                .is_empty()
        });
        match (has_dob_column, has_date_column) {
            (true, true) => true,
            (false, true) => {
                warn!(
                    "Date columns were found in the data, yet there was no DateOfBirth column. \
                 DateToAge Strategy was not applied."
                );
                false
            }
            _ => false,
        }
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!("Applying DateToAge strategy to data.");

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        let patient_dob_hash_map = Self::map_patient_to_dob(tables)?;

        for table in tables.iter_mut() {
            let stringified_subject_id_col = table.get_subject_id_col().str()?.clone();

            let date_column_names = table
                .filter_columns()
                .where_data_contexts_are(&DATE_CONTEXTS_WITHOUT_DOB)
                .collect_owned_names();

            for date_col_name in date_column_names.iter() {
                let date_col = table.data().column(date_col_name)?;

                let casted_date_col = match date_col.dtype() {
                    DataType::String => Cow::Borrowed(date_col),
                    DataType::Int32 => Cow::Owned(date_col.cast(&DataType::String)?),
                    DataType::Int64 => Cow::Owned(date_col.cast(&DataType::String)?),
                    DataType::Date => Cow::Owned(date_col.cast(&DataType::String)?),
                    DataType::Datetime(..) => Cow::Owned(date_col.cast(&DataType::String)?),
                    DataType::Null => Cow::Owned(date_col.cast(&DataType::String)?),
                    other_datatype => {
                        return Err(StrategyError::DataTypeError {
                            column_name: date_col_name.clone(),
                            strategy: "DateToAge".to_string(),
                            allowed_datatypes: vec![
                                DataType::String.to_string(),
                                DataType::Int32.to_string(),
                                DataType::Int64.to_string(),
                                DataType::Date.to_string(),
                                "Datetime".to_string(),
                                DataType::Null.to_string(),
                            ],
                            found_datatype: other_datatype.to_string(),
                        });
                    }
                };

                let stringified_date_col = casted_date_col.str()?;

                let subject_id_date_zip = stringified_subject_id_col
                    .iter()
                    .zip(stringified_date_col.iter());

                let ages: Vec<AnyValue> = subject_id_date_zip
                    .map(|(subject_id_opt, date_opt)| {
                        let subject_id =
                            subject_id_opt.expect("SubjectID column should have no gaps.");
                        let subject_dob_opt = patient_dob_hash_map.get(subject_id);

                        if let Some(date) = date_opt {
                            if let Some(subject_dob) = subject_dob_opt
                                && let Ok(age) =
                                    Self::date_and_dob_to_age(subject_id, subject_dob.clone(), date)
                            {
                                AnyValue::StringOwned(age.into())
                            } else {
                                error_info.insert_error(
                                    date_col_name.clone(),
                                    table.context().name().to_string(),
                                    date.to_string(),
                                    vec![],
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
                .replace_data_contexts(Self::date_to_age_contexts_hash_map())
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
    fn map_patient_to_dob(
        tables: &[&mut ContextualizedDataFrame],
    ) -> Result<HashMap<String, String>, StrategyError> {
        let dob_table = tables.iter().find(|table|
            !table.filter_columns()
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

        let patient_dob_hash_map = Self::flatten_patient_dob_hash_map(
            &dob_table.group_column_by_subject_id(dob_col_name)?,
        )?;

        Ok(patient_dob_hash_map)
    }

    fn flatten_patient_dob_hash_map(
        patient_dob_hash_map: &HashMap<String, Vec<String>>,
    ) -> Result<HashMap<String, String>, StrategyError> {
        let mut valid_dob = HashMap::with_capacity(patient_dob_hash_map.len());
        let mut invalid_p_ids: Vec<String> = Vec::new();

        for (p_id, dobs) in patient_dob_hash_map {
            if dobs.len() != 1 {
                invalid_p_ids.push(p_id.clone());
            } else {
                valid_dob.insert(p_id.to_string(), dobs.first().unwrap().to_string());
            }
        }

        if invalid_p_ids.is_empty() {
            Ok(valid_dob)
        } else {
            Err(StrategyError::MultiplicityError {
                context: Context::DateOfBirth,
                message: "These patients did not have exactly one date of birth.".to_string(),
                patients: invalid_p_ids,
            })
        }
    }

    /// Given the date of birth of a patient, and a date in their life
    /// this will calculate the age of a patient at that date.
    ///
    /// An error will be thrown if the date of birth, or the date, cannot be interpreted as
    /// chrono::NaiveDate.
    fn date_and_dob_to_age(
        subject_id: &str,
        dob: String,
        date: &str,
    ) -> Result<String, StrategyError> {
        let dob_object = if let Some(dob) = try_parse_string_date(dob.as_str()) {
            dob.and_hms_opt(0, 0, 0).unwrap()
        } else {
            try_parse_string_datetime(dob.as_str()).ok_or_else(|| {
                StrategyError::DateParsingError {
                    subject_id: subject_id.to_string(),
                    unparseable_date: dob,
                }
            })?
        };

        let date_object = if let Some(date) = try_parse_string_date(date) {
            date.and_hms_opt(0, 0, 0).unwrap()
        } else {
            try_parse_string_datetime(date).ok_or_else(|| StrategyError::DateParsingError {
                subject_id: subject_id.to_string(),
                unparseable_date: date.to_string(),
            })?
        };

        Self::date_differencer(subject_id, dob_object, date_object)
    }

    fn date_to_age_contexts_hash_map() -> HashMap<Context, Context> {
        DATE_CONTEXTS_WITHOUT_DOB
            .into_iter()
            .zip(AGE_CONTEXTS)
            .collect()
    }

    fn date_differencer(
        subject_id: &str,
        dob: NaiveDateTime,
        date: NaiveDateTime,
    ) -> Result<String, StrategyError> {
        if dob == date {
            Ok("P0Y".to_string())
        } else {
            let diff = date_diff(dob, date);
            if diff.years < 0 || diff.months < 0 || diff.days < 0 {
                Err(StrategyError::NegativeAge {
                    subject_id: subject_id.to_string(),
                    date_of_birth: dob.to_string(),
                    date: date.to_string(),
                })
            } else {
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
    use crate::test_suite::cdf_generation::default_patient_id;
    use crate::test_suite::phenopacket_component_generation::default_datetime;
    use chrono::NaiveDateTime;
    use chrono::{Datelike, NaiveDate};
    use polars::datatypes::TimeUnit;
    use polars::df;
    use polars::frame::DataFrame;
    use rstest::{fixture, rstest};
    use std::str::FromStr;

    #[fixture]
    fn epoch_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
    }

    #[fixture]
    fn epoch_datetime() -> NaiveDateTime {
        epoch_date().and_hms_opt(0, 0, 0).unwrap()
    }

    #[fixture]
    fn dob_alice_string() -> String {
        "1995-06-01".to_string()
    }

    #[fixture]
    fn dob_alice_date() -> i32 {
        let date = NaiveDate::from_str(dob_alice_string().as_str()).unwrap();
        (date - epoch_date()).num_days() as i32
    }

    #[fixture]
    fn dob_alice_datetime() -> i64 {
        let datetime = NaiveDate::from_str(dob_alice_string().as_str())
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        (datetime - epoch_datetime()).num_milliseconds()
    }

    #[fixture]
    fn dob_alice_datetime_str() -> String {
        let mut datetime = dob_alice_string();
        datetime.push_str(" 00:00:00.000");
        datetime.to_string()
    }

    #[fixture]
    fn dob_bob_string() -> String {
        "1990-12-01".to_string()
    }

    #[fixture]
    fn dob_bob_date() -> i32 {
        let date = NaiveDate::from_str(dob_bob_string().as_str()).unwrap();
        (date - epoch_date()).num_days() as i32
    }

    #[fixture]
    fn dob_bob_datetime() -> i64 {
        let datetime = NaiveDate::from_str(dob_bob_string().as_str())
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        (datetime - epoch_datetime()).num_milliseconds()
    }

    #[fixture]
    fn dob_bob_datetime_str() -> String {
        let mut datetime = dob_bob_string();
        datetime.push_str(" 00:00:00.000");
        datetime.to_string()
    }

    #[fixture]
    fn dob_charlie_eu_string() -> String {
        "08-01-1980".to_string()
    }

    #[fixture]
    fn dob_charlie_us_string() -> String {
        "1980-01-08".to_string()
    }

    #[fixture]
    fn dob_charlie_date() -> i32 {
        let date = NaiveDate::parse_from_str(dob_charlie_eu_string().as_str(), "%d-%m-%Y").unwrap();
        (date - epoch_date()).num_days() as i32
    }

    #[fixture]
    fn dob_charlie_datetime() -> i64 {
        let datetime = NaiveDate::parse_from_str(dob_charlie_eu_string().as_str(), "%d-%m-%Y")
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        (datetime - epoch_datetime()).num_milliseconds()
    }

    #[fixture]
    fn dob_charlie_eu_datetime_str() -> String {
        let mut datetime = dob_charlie_eu_string();
        datetime.push_str(" 00:00:00.000");
        datetime.to_string()
    }

    #[fixture]
    fn dob_charlie_us_datetime_str() -> String {
        let mut datetime = dob_charlie_us_string();
        datetime.push_str(" 00:00:00.000");
        datetime.to_string()
    }

    #[fixture]
    fn onset_bob() -> String {
        "1991-01-01".to_string()
    }

    #[fixture]
    fn onset_bob_date() -> i32 {
        let date = NaiveDate::from_str(onset_bob().as_str()).unwrap();
        (date - epoch_date()).num_days() as i32
    }

    #[fixture]
    fn onset_bob_datetime() -> i64 {
        let datetime = NaiveDate::from_str(onset_bob().as_str())
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        (datetime - epoch_datetime()).num_milliseconds()
    }

    #[fixture]
    fn onset_bob_int() -> i32 {
        let date = NaiveDate::from_str(onset_bob().as_str()).unwrap();
        date.year()
    }

    #[fixture]
    fn onset_charlie() -> String {
        "2025-01-01".to_string()
    }

    #[fixture]
    fn onset_charlie_date() -> i32 {
        let date = NaiveDate::from_str(onset_charlie().as_str()).unwrap();
        (date - epoch_date()).num_days() as i32
    }

    #[fixture]
    fn onset_charlie_datetime() -> i64 {
        let datetime = NaiveDate::from_str(onset_charlie().as_str())
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        (datetime - epoch_datetime()).num_milliseconds()
    }

    #[fixture]
    fn onset_charlie_int() -> i32 {
        let date = NaiveDate::from_str(onset_charlie().as_str()).unwrap();
        date.year()
    }

    #[fixture]
    fn df_with_string_dob() -> DataFrame {
        df!(
        "subject_id" => &["Alice", "Bob", "Charlie"],
        "DOB" => &[AnyValue::String(dob_alice_string().as_str()), AnyValue::String(dob_bob_string().as_str()), AnyValue::String(dob_charlie_eu_string().as_str())],
        "bronchitis" => &["Observed", "Not observed", "Observed"],
        )
            .unwrap()
    }

    #[fixture]
    fn df_with_date_dob() -> DataFrame {
        df!(
        "subject_id" => &["Alice", "Bob", "Charlie"],
        "DOB" => &[AnyValue::Date(dob_alice_date()), AnyValue::Date(dob_bob_date()), AnyValue::Date(dob_charlie_date())],
        "bronchitis" => &["Observed", "Not observed", "Observed"],
        )
            .unwrap()
    }

    #[fixture]
    fn df_with_datetime_dob() -> DataFrame {
        df!(
        "subject_id" => &["Alice", "Bob", "Charlie"],
        "DOB" => &[AnyValue::Datetime(dob_alice_datetime(), TimeUnit::Milliseconds, None), AnyValue::Datetime(dob_bob_datetime(), TimeUnit::Milliseconds, None), AnyValue::Datetime(dob_charlie_datetime(), TimeUnit::Milliseconds, None)],
        "bronchitis" => &["Observed", "Not observed", "Observed"],
        )
            .unwrap()
    }

    #[fixture]
    fn dob_tc() -> TableContext {
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
    fn df_with_str_onset() -> DataFrame {
        df!(
        "subject_id" => &["Alice", "Bob", "Charlie"],
        "pneumonia" => &["Not observed", "Not observed", "Observed"],
        "onset" => &[AnyValue::Null, AnyValue::String(onset_bob().as_str()), AnyValue::String(onset_charlie().as_str())],
        )
            .unwrap()
    }

    #[fixture]
    fn df_with_date_onset() -> DataFrame {
        df!(
        "subject_id" => &["Alice", "Bob", "Charlie"],
        "pneumonia" => &["Not observed", "Not observed", "Observed"],
        "onset" => &[AnyValue::Null, AnyValue::Date(onset_bob_date()), AnyValue::Date(onset_charlie_date())],
        )
            .unwrap()
    }

    #[fixture]
    fn df_with_datetime_onset() -> DataFrame {
        df!(
        "subject_id" => &["Alice", "Bob", "Charlie"],
        "pneumonia" => &["Not observed", "Not observed", "Observed"],
        "onset" => &[AnyValue::Null, AnyValue::Datetime(onset_bob_datetime(), TimeUnit::Milliseconds, None), AnyValue::Datetime(onset_charlie_datetime(), TimeUnit::Milliseconds, None)],
        )
            .unwrap()
    }

    #[fixture]
    fn df_with_int_onset() -> DataFrame {
        df!(
        "subject_id" => &["Alice", "Bob", "Charlie"],
        "pneumonia" => &["Not observed", "Not observed", "Observed"],
        "onset" => &[AnyValue::Null, AnyValue::Int32(onset_bob_int()), AnyValue::Int32(onset_charlie_int())],
        )
            .unwrap()
    }

    #[fixture]
    fn onset_tc() -> TableContext {
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
                    .with_data_context(Context::OnsetDate),
            ],
        )
    }

    #[rstest]
    fn test_date_to_age_strategy(
        #[values(df_with_string_dob(), df_with_date_dob(), df_with_datetime_dob())]
        dob_df: DataFrame,
        #[values(
            df_with_str_onset(),
            df_with_date_onset(),
            df_with_datetime_onset(),
            df_with_int_onset()
        )]
        onset_df: DataFrame,
    ) {
        let mut cdf1 = ContextualizedDataFrame::new(dob_tc(), dob_df).unwrap();
        let mut cdf2 = ContextualizedDataFrame::new(onset_tc(), onset_df).unwrap();
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
                    AnyValue::String("P1M"),
                    AnyValue::String("P44Y11M24D")
                ]
            )
        );

        //check the change of contexts has succeeded
        assert_eq!(
            cdf2.filter_series_context()
                .where_data_contexts_are(&DATE_CONTEXTS_WITHOUT_DOB)
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
    fn test_date_and_dob_to_age() {
        let onset_age_bob =
            DateToAgeStrategy::date_and_dob_to_age("P001", dob_bob_string(), onset_bob().as_str())
                .unwrap();
        assert_eq!(onset_age_bob, "P1M");

        let onset_age_charlie = DateToAgeStrategy::date_and_dob_to_age(
            "P001",
            dob_charlie_eu_string(),
            onset_charlie().as_str(),
        )
        .unwrap();
        assert_eq!(onset_age_charlie, "P44Y11M24D");
    }

    #[rstest]
    fn test_date_and_dob_to_age_with_datetimes() {
        let mut onset_bob_datetime = onset_bob();
        onset_bob_datetime.push_str(" 00:00:00.000");
        let onset_age_bob = DateToAgeStrategy::date_and_dob_to_age(
            "P001",
            dob_bob_datetime_str(),
            onset_bob().as_str(),
        )
        .unwrap();
        assert_eq!(onset_age_bob, "P1M");
    }

    #[rstest]
    fn test_date_and_dob_to_age_err() {
        let result =
            DateToAgeStrategy::date_and_dob_to_age("P001", "2000-13-50".to_string(), "2025-11-21");
        assert!(result.is_err());
    }

    #[rstest]
    fn test_map_patient_to_dob_string() {
        let mut cdf1 = ContextualizedDataFrame::new(dob_tc(), df_with_string_dob()).unwrap();
        let mut cdf2 = ContextualizedDataFrame::new(onset_tc(), df_with_str_onset()).unwrap();
        let tables = [&mut cdf1, &mut cdf2];
        let patient_dob_hm = DateToAgeStrategy::map_patient_to_dob(&tables).unwrap();
        assert_eq!(patient_dob_hm.len(), 3);
        assert_eq!(patient_dob_hm["Alice"], dob_alice_string());
        assert_eq!(patient_dob_hm["Bob"], dob_bob_string());
        assert_eq!(patient_dob_hm["Charlie"], dob_charlie_eu_string());
    }

    #[rstest]
    fn test_map_patient_to_dob_date() {
        let mut cdf1 = ContextualizedDataFrame::new(dob_tc(), df_with_date_dob()).unwrap();
        let mut cdf2 = ContextualizedDataFrame::new(onset_tc(), df_with_str_onset()).unwrap();
        let tables = [&mut cdf1, &mut cdf2];
        let patient_dob_hm = DateToAgeStrategy::map_patient_to_dob(&tables).unwrap();
        assert_eq!(patient_dob_hm.len(), 3);
        assert_eq!(patient_dob_hm["Alice"], dob_alice_string());
        assert_eq!(patient_dob_hm["Bob"], dob_bob_string());
        assert_eq!(patient_dob_hm["Charlie"], dob_charlie_us_string());
    }

    #[rstest]
    fn test_map_patient_to_dob_datetimes() {
        let mut cdf1 = ContextualizedDataFrame::new(dob_tc(), df_with_datetime_dob()).unwrap();
        let mut cdf2 = ContextualizedDataFrame::new(onset_tc(), df_with_str_onset()).unwrap();
        let tables = [&mut cdf1, &mut cdf2];
        let patient_dob_hm = DateToAgeStrategy::map_patient_to_dob(&tables).unwrap();
        assert_eq!(patient_dob_hm.len(), 3);
        assert_eq!(patient_dob_hm["Alice"], dob_alice_datetime_str());
        assert_eq!(patient_dob_hm["Bob"], dob_bob_datetime_str());
        assert_eq!(patient_dob_hm["Charlie"], dob_charlie_us_datetime_str());
    }

    #[rstest]
    fn test_flatten_patient_dob_hash_map() {
        let p_ids = [
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
        ];
        let dob_vecs = [
            vec![dob_alice_string()],
            vec![dob_bob_string()],
            vec![dob_charlie_us_string()],
        ];
        let dobs = [
            dob_alice_string(),
            dob_bob_string(),
            dob_charlie_us_string(),
        ];

        let hm: HashMap<String, Vec<String>> = p_ids.clone().into_iter().zip(dob_vecs).collect();
        let flattened_hm = DateToAgeStrategy::flatten_patient_dob_hash_map(&hm).unwrap();

        let expected_flattened_hm: HashMap<String, String> = p_ids.into_iter().zip(dobs).collect();
        assert_eq!(flattened_hm, expected_flattened_hm);
    }

    #[rstest]
    fn test_flatten_patient_dob_hash_map_err_on_multiple() {
        let p_ids = [
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
        ];
        let dob_vecs = [
            vec![dob_alice_string()],
            vec![dob_bob_string(), onset_bob()],
            vec![dob_charlie_us_string()],
        ];

        let hm: HashMap<String, Vec<String>> = p_ids.clone().into_iter().zip(dob_vecs).collect();
        assert!(DateToAgeStrategy::flatten_patient_dob_hash_map(&hm).is_err());
    }

    #[rstest]
    fn test_flatten_patient_dob_hash_map_err_on_none() {
        let p_ids = [
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
        ];
        let dob_vecs = [
            vec![],
            vec![dob_bob_string()],
            vec![dob_charlie_us_string()],
        ];

        let hm: HashMap<String, Vec<String>> = p_ids.clone().into_iter().zip(dob_vecs).collect();
        assert!(DateToAgeStrategy::flatten_patient_dob_hash_map(&hm).is_err());
    }

    #[rstest]
    fn test_date_to_age_contexts_hash_map() {
        let hm = DateToAgeStrategy::date_to_age_contexts_hash_map();
        assert_eq!(hm.len(), 3);
        assert_eq!(
            hm[&Context::DateAtLastEncounter],
            Context::AgeAtLastEncounter
        );
        assert_eq!(hm[&Context::OnsetDate], Context::OnsetAge);
        assert_eq!(hm[&Context::DateOfDeath], Context::AgeOfDeath);
    }

    #[rstest]
    fn test_date_differencer_positive_dur() {
        let dob = default_datetime();
        let date = dob
            .date()
            .with_year(dob.year() + 1)
            .unwrap()
            .and_time(dob.time());
        assert_eq!(
            DateToAgeStrategy::date_differencer(default_patient_id().as_str(), dob, date).unwrap(),
            "P1Y".to_string()
        );
    }

    #[rstest]
    fn test_date_differencer_zero_dur() {
        let dob = default_datetime();
        let date = dob;
        assert_eq!(
            DateToAgeStrategy::date_differencer(default_patient_id().as_str(), dob, date).unwrap(),
            "P0Y".to_string()
        );
    }

    #[rstest]
    fn test_date_differencer_negative_dur() {
        let dob = default_datetime();
        let date = dob
            .date()
            .with_year(dob.year() - 1)
            .unwrap()
            .and_time(dob.time());
        assert!(
            DateToAgeStrategy::date_differencer(default_patient_id().as_str(), dob, date).is_err()
        );
    }
}
