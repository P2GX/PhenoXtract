use crate::config::context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::data_processing::casting::{is_ints, polars_column_cast_ambivalent};
use crate::transform::error::DataProcessingError;
use polars::datatypes::DataType;
use polars::prelude::ChunkApply;
use polars::series::IntoSeries;
use std::borrow::Cow;

pub(crate) struct CdfPreprocessor;

impl CdfPreprocessor {
    pub(crate) fn process(cdf: &mut ContextualizedDataFrame) -> Result<(), DataProcessingError> {
        Self::trim_strings(cdf)?;
        Self::ensure_ints(cdf)?;
        Self::cast_cdf(cdf)?;
        Ok(())
    }

    /// Trims whitespace from all string columns and converts empty strings to null.
    ///
    /// This method:
    /// - Identifies all columns with `DataType::String`
    /// - Applies `.trim()` to each string value
    /// - Converts empty strings (after trimming) to `None`
    /// - Leaves existing null values unchanged
    fn trim_strings(cdf: &mut ContextualizedDataFrame) -> Result<(), DataProcessingError> {
        let string_col_names: Vec<String> = cdf
            .filter_columns()
            .where_data_type(Filter::Is(&DataType::String))
            .collect_owned_names();

        for col_name in string_col_names {
            let column = cdf.data().column(&col_name)?;
            let trimmed_col = column.str()?.apply(|s| match s {
                None => None,
                Some(s) => {
                    let trimmed = s.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(Cow::Borrowed(trimmed))
                    }
                }
            });
            cdf.builder()
                .replace_col(&col_name, trimmed_col.into_series())?
                .build()?;
        }
        Ok(())
    }

    /// Converts float columns to Int64 if all values are whole numbers within i64 range.
    ///
    /// Scans all Float32 and Float64 columns in the dataframe. If a column contains only
    /// integer values (or nulls), it is cast to Int64 type in-place.
    fn ensure_ints(cdf: &mut ContextualizedDataFrame) -> Result<(), DataProcessingError> {
        let float_col_names: Vec<String> = cdf
            .filter_columns()
            .where_data_type(Filter::Is(&DataType::Float64))
            .where_data_type(Filter::Is(&DataType::Float32))
            .where_data_type(Filter::Is(&DataType::Int32))
            .collect_owned_names();

        for col_name in float_col_names {
            let column = cdf.data().column(&col_name)?;

            let is_all_integers = match column.dtype() {
                DataType::Float64 => is_ints(column.f64()?),
                DataType::Float32 => is_ints(column.f32()?),
                DataType::Int32 => true,
                _ => false,
            };

            if is_all_integers {
                let casted = column.cast(&DataType::Int64)?;
                cdf.builder()
                    .replace_col(
                        casted.name().to_string().as_str(),
                        casted.take_materialized_series(),
                    )?
                    .build()?;
            }
        }
        Ok(())
    }

    /// Applies context-aware type casting to all columns in the dataframe.
    ///
    /// This method:
    /// 1. Identifies the SubjectId column (expects exactly one)
    /// 2. Applies ambivalent casting to all non-SubjectId columns via
    ///    `polars_column_cast_ambivalent`
    /// 3. Explicitly casts all SubjectId columns to String type
    fn cast_cdf(cdf: &mut ContextualizedDataFrame) -> Result<(), DataProcessingError> {
        let possible_subject_id_col_names = cdf
            .filter_columns()
            .where_data_context(Filter::Is(&Context::SubjectId))
            .collect_owned_names();
        let subject_id_col_name = possible_subject_id_col_names
            .first()
            .expect("Should be exactly one SubjectID column in data.");

        let non_subject_id_col_names = cdf
            .data()
            .get_column_names()
            .into_iter()
            .filter(|&name| name != subject_id_col_name)
            .map(|name| name.to_string())
            .collect::<Vec<String>>();

        for col_name in non_subject_id_col_names {
            let column = cdf.data().column(col_name.as_str())?;

            let casted_series = polars_column_cast_ambivalent(column).take_materialized_series();
            cdf.builder()
                .replace_col(col_name.as_str(), casted_series)?
                .build()?;
        }

        cdf.builder()
            .cast(&Context::None, &Context::SubjectId, DataType::String)?
            .build()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::context::Context;
    use crate::config::table_context::{Identifier, SeriesContext, TableContext};
    use crate::config::traits::SeriesContextBuilding;
    use polars::df;
    use polars::prelude::{AnyValue, DataType, TimeUnit};
    use rstest::rstest;

    #[rstest]
    fn test_cast_cdf() {
        let df = df![
            "int_col" => &["1", "2", "3"],
            "float_col" => &["1.5", "2.5", "3.5"],
            "bool_col" => &["True", "False", "True"],
            "date_col" => &["2023-01-01", "2023-01-02", "2023-01-03"],
            "datetime_col" => &["2023-01-01T12:00:00", "2023-01-02T13:30:00", "2023-01-03T15:45:00"],
            "string_col" => &["hello", "world", "test"]
        ].unwrap();
        let mut cdf = ContextualizedDataFrame::new(
            TableContext::new(
                "".to_string(),
                vec![
                    SeriesContext::from_identifier("string_col")
                        .with_data_context(Context::SubjectId),
                ],
            ),
            df.clone(),
        )
        .unwrap();

        let result = CdfPreprocessor::cast_cdf(&mut cdf);
        assert!(result.is_ok());
        assert_eq!(
            cdf.data().column("int_col").unwrap().dtype(),
            &DataType::Int64
        );
        assert_eq!(
            cdf.data().column("float_col").unwrap().dtype(),
            &DataType::Float64
        );
        assert_eq!(
            cdf.data().column("bool_col").unwrap().dtype(),
            &DataType::Boolean
        );
        assert_eq!(
            cdf.data().column("date_col").unwrap().dtype(),
            &DataType::Date
        );
        assert_eq!(
            cdf.data().column("datetime_col").unwrap().dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
        assert_eq!(
            cdf.data().column("string_col").unwrap().dtype(),
            &DataType::String
        );
    }

    #[rstest]
    fn test_ensure_ints_with_float32() {
        let col_name = "values";
        let float32_df = df!(
            col_name => &[1.0f32, 2.0f32, 3.0f32, 4.0f32],
            "subject_id" => &["a", "b", "c", "d"])
        .unwrap();

        let mut cdf = ContextualizedDataFrame::new(
            TableContext::new(
                "".to_string(),
                vec![
                    SeriesContext::default()
                        .with_data_context(Context::SubjectId)
                        .with_identifier(Identifier::from("subject")),
                    SeriesContext::default().with_identifier(Identifier::from(col_name)),
                ],
            ),
            float32_df,
        )
        .unwrap();
        CdfPreprocessor::ensure_ints(&mut cdf).unwrap();

        // Verify the column was cast to Int64
        let result_col = cdf.data().column("values").unwrap();
        assert_eq!(result_col.dtype(), &DataType::Int64);

        // Verify values are preserved
        let int_values = result_col.i64().unwrap();
        assert_eq!(int_values.get(0), Some(1));
        assert_eq!(int_values.get(1), Some(2));
        assert_eq!(int_values.get(2), Some(3));
        assert_eq!(int_values.get(3), Some(4));
    }

    #[rstest]
    fn test_ensure_ints_with_float64() {
        let col_name = "values";
        let float64_df = df!(
            col_name => &[10.0f64, 20.0f64, 30.0f64, 40.0f64],
            "subject_id" => &["a", "b", "c", "d"])
        .unwrap();

        let mut cdf = ContextualizedDataFrame::new(
            TableContext::new(
                "".to_string(),
                vec![
                    SeriesContext::default()
                        .with_data_context(Context::SubjectId)
                        .with_identifier(Identifier::from("subject")),
                    SeriesContext::default().with_identifier(Identifier::from(col_name)),
                ],
            ),
            float64_df,
        )
        .unwrap();
        CdfPreprocessor::ensure_ints(&mut cdf).unwrap();

        let result_col = cdf.data().column("values").unwrap();
        assert_eq!(result_col.dtype(), &DataType::Int64);

        let int_values = result_col.i64().unwrap();
        assert_eq!(int_values.get(0), Some(10));
        assert_eq!(int_values.get(1), Some(20));
        assert_eq!(int_values.get(2), Some(30));
        assert_eq!(int_values.get(3), Some(40));
    }

    #[rstest]
    fn test_ensure_ints_with_actual_floats() {
        let col_name_f64 = "f64";
        let col_name_f32 = "f32";
        let col_name_i32 = "i32";
        let float64_df = df!(
            col_name_f64 => &[1.5f64, 2.7f64, 3.2f64],
            col_name_f32 => &[1.5f32, 2.7f32, 3.2f32],
            col_name_i32 => &[1i32, 2i32, 3i32],
            "subject_id" => &["a", "b", "c"]
        )
        .unwrap();

        let mut cdf = ContextualizedDataFrame::new(
            TableContext::new(
                "".to_string(),
                vec![
                    SeriesContext::default()
                        .with_data_context(Context::SubjectId)
                        .with_identifier(Identifier::from("subject")),
                    SeriesContext::default().with_identifier(Identifier::from(col_name_f64)),
                ],
            ),
            float64_df,
        )
        .unwrap();
        CdfPreprocessor::ensure_ints(&mut cdf).unwrap();

        for (expected_data_type, col_name) in [
            (DataType::Float32, col_name_f32),
            (DataType::Float64, col_name_f64),
            (DataType::Int32, col_name_i32),
        ] {
            let result_col = cdf.data().column(col_name).unwrap();

            assert_eq!(
                result_col.dtype(),
                &expected_data_type,
                "Expected {:?}, got {:?}",
                expected_data_type,
                result_col.dtype()
            );
        }
    }

    #[rstest]
    fn test_trim_strings() {
        let df = df![
            "subject_id" => ["P001", "P002", "P003", "P004", "P005"],
            "string_col" => &["   hello", "world  ", "  test  ", "blah", "  "],
            "int_col" => &[1, 2, 3, 4, 5],
        ]
        .unwrap();
        let mut cdf = ContextualizedDataFrame::new(
            TableContext::new(
                "table".to_string(),
                vec![
                    SeriesContext::from_identifier("subject_id".to_string())
                        .with_data_context(Context::SubjectId),
                    SeriesContext::from_identifier("string_col".to_string()),
                    SeriesContext::from_identifier("int_col".to_string()),
                ],
            ),
            df,
        )
        .unwrap();

        CdfPreprocessor::trim_strings(&mut cdf).unwrap();

        assert_eq!(
            cdf.data(),
            &df!["subject_id" => ["P001", "P002", "P003", "P004", "P005"],
                "string_col" => &[AnyValue::String("hello"), AnyValue::String("world"), AnyValue::String("test"), AnyValue::String("blah"), AnyValue::Null],
                "int col" => &[1, 2, 3, 4, 5],
            ]
                .unwrap()
        );
    }
}
