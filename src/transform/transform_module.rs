use crate::config::context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::collecting::broker::CDFBroker;
use crate::transform::error::{DataProcessingError, TransformError};
use crate::transform::traits::Strategy;
use crate::transform::utils::polars_column_cast_ambivalent;
use phenopackets::schema::v2::Phenopacket;
use polars::prelude::{ChunkApply, DataType, IntoSeries};
use std::borrow::Cow;

#[allow(dead_code)]
#[derive(Debug)]
pub struct TransformerModule {
    strategies: Vec<Box<dyn Strategy>>,
    collector: CDFBroker,
}

impl TransformerModule {
    pub fn new(strategies: Vec<Box<dyn Strategy>>, collector: CDFBroker) -> Self {
        TransformerModule {
            strategies,
            collector,
        }
    }

    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy>) {
        self.strategies.push(strategy);
    }
    pub fn insert_strategy(&mut self, idx: usize, strategy: Box<dyn Strategy>) {
        self.strategies.insert(idx, strategy);
    }

    pub fn run(
        &mut self,
        mut data: Vec<ContextualizedDataFrame>,
    ) -> Result<Vec<Phenopacket>, TransformError> {
        let mut tables_refs = data
            .iter_mut()
            .collect::<Vec<&mut ContextualizedDataFrame>>();

        for table in &mut tables_refs {
            Self::trim_strings(table)?;
            Self::ensure_ints(table)?;
            Self::ambivalent_cast_non_id_columns(table)?;
            table
                .builder()
                .cast(&Context::None, &Context::SubjectId, DataType::String)?
                .build()?;
        }

        for strategy in &self.strategies {
            strategy.transform(tables_refs.as_mut_slice())?;
        }

        Ok(self.collector.broker(data)?)
    }

    fn trim_strings(cdf: &mut ContextualizedDataFrame) -> Result<(), DataProcessingError> {
        let string_col_names: Vec<String> = cdf
            .filter_columns()
            .where_dtype(Filter::Is(&DataType::String))
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
                .replace_column(&col_name, trimmed_col.into_series())?
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
            .where_dtype(Filter::Is(&DataType::Float64))
            .where_dtype(Filter::Is(&DataType::Float32))
            .where_dtype(Filter::Is(&DataType::Int32))
            .collect_owned_names();

        for col_name in float_col_names {
            let column = cdf.data().column(&col_name)?;

            let is_all_integers = match column.dtype() {
                DataType::Float64 => column.f64()?.into_iter().all(|val_opt: Option<f64>| {
                    val_opt.is_none_or(|val| {
                        val.fract() == 0.0
                            && val.is_finite()
                            && val >= i64::MIN as f64
                            && val <= i64::MAX as f64
                    })
                }),
                DataType::Float32 => column.f32()?.into_iter().all(|val_opt: Option<f32>| {
                    val_opt.is_none_or(|val| {
                        val.fract() == 0.0
                            && val.is_finite()
                            && val >= i64::MIN as f32
                            && val <= i64::MAX as f32
                    })
                }),
                DataType::Int32 => true,
                _ => false,
            };

            if is_all_integers {
                let casted = column.cast(&DataType::Int64)?;
                cdf.builder()
                    .replace_column(
                        casted.name().to_string().as_str(),
                        casted.take_materialized_series(),
                    )?
                    .build()?;
            }
        }
        Ok(())
    }

    fn ambivalent_cast_non_id_columns(
        cdf: &mut ContextualizedDataFrame,
    ) -> Result<(), DataProcessingError> {
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
                .replace_column(col_name.as_str(), casted_series)?
                .build()?;
        }
        Ok(())
    }
}

impl PartialEq for TransformerModule {
    fn eq(&self, other: &Self) -> bool {
        self.collector == other.collector
            && self.strategies.len() == other.strategies.len()
            && self
                .strategies
                .iter()
                .zip(other.strategies.iter())
                .all(|(a, b)| format!("{:?}", a) == format!("{:?}", b))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::context::Context;
    use crate::config::table_context::{Identifier, SeriesContext, TableContext};
    use polars::df;
    use polars::prelude::{AnyValue, DataType, TimeUnit};
    use rstest::rstest;

    #[rstest]
    fn test_ambivalent_cast_non_id_columns() {
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
                    SeriesContext::default()
                        .with_data_context(Context::SubjectId)
                        .with_identifier(Identifier::Regex("string_col".to_string())),
                ],
            ),
            df.clone(),
        )
        .unwrap();

        let result = TransformerModule::ambivalent_cast_non_id_columns(&mut cdf);
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

    #[test]
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
        TransformerModule::ensure_ints(&mut cdf).unwrap();

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

    #[test]
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
        TransformerModule::ensure_ints(&mut cdf).unwrap();

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
        TransformerModule::ensure_ints(&mut cdf).unwrap();

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
                    SeriesContext::default()
                        .with_identifier(Identifier::Regex("subject_id".to_string()))
                        .with_data_context(Context::SubjectId),
                    SeriesContext::default()
                        .with_identifier(Identifier::Regex("string_col".to_string())),
                    SeriesContext::default()
                        .with_identifier(Identifier::Regex("int_col".to_string())),
                ],
            ),
            df,
        )
        .unwrap();

        TransformerModule::trim_strings(&mut cdf).unwrap();

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
