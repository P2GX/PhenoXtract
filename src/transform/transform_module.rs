use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::collector::Collector;
use crate::transform::error::TransformError;
use crate::transform::traits::Strategy;
use crate::utils::{try_parse_string_date, try_parse_string_datetime};
use chrono::{NaiveDate, NaiveDateTime};
use log::debug;
use phenopackets::schema::v2::Phenopacket;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::prelude::{NamedFrom, Series};

#[allow(dead_code)]
#[derive(Debug)]
pub struct TransformerModule {
    strategies: Vec<Box<dyn Strategy>>,
    collector: Collector,
}

impl TransformerModule {
    #[allow(dead_code)]
    pub fn run(
        &mut self,
        mut data: Vec<ContextualizedDataFrame>,
    ) -> Result<Vec<Phenopacket>, TransformError> {
        let mut tables_refs = data
            .iter_mut()
            .collect::<Vec<&mut ContextualizedDataFrame>>();

        for table in &mut tables_refs {
            Self::polars_column_string_cast(&mut table.data)?;
        }

        for strategy in &self.strategies {
            strategy.transform(tables_refs.as_mut_slice())?;
        }

        self.collector.collect(data)
    }

    pub fn new(strategies: Vec<Box<dyn Strategy>>, collector: Collector) -> Self {
        TransformerModule {
            strategies,
            collector,
        }
    }

    fn polars_column_string_cast(data: &mut DataFrame) -> Result<(), TransformError> {
        let col_names: Vec<String> = data
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        for col_name in col_names {
            let column = data
                .column(col_name.as_str())
                .map_err(|err| TransformError::CastingError(err.to_string()))?;

            debug!("Try casting Column: {col_name}");
            if column.dtype() != &DataType::String {
                debug!("Skipped column {col_name}. Not of string type.");
                continue;
            }

            if let Some(bools) = column
                .str()
                .map_err(|err| TransformError::CastingError(err.to_string()))?
                .iter()
                .map(|opt| {
                    opt.as_ref().and_then(|s| match s.to_lowercase().as_str() {
                        "true" => Some(true),
                        "false" => Some(false),
                        _ => None,
                    })
                })
                .collect::<Option<Vec<bool>>>()
            {
                debug!("Casted column: {col_name} to bool.");
                let s: Series = Series::new(col_name.clone().into(), bools);
                data.replace(col_name.as_str(), s.clone())
                    .map_err(|err| TransformError::CastingError(err.to_string()))?;
                continue;
            }

            if let Ok(mut casted) = column.strict_cast(&DataType::Int64) {
                debug!("Casted column: {col_name} to i64.");
                data.replace(
                    col_name.as_str(),
                    casted.into_materialized_series().to_owned(),
                )
                .map_err(|err| TransformError::CastingError(err.to_string()))?;
                continue;
            }

            if let Ok(mut casted) = column.strict_cast(&DataType::Float64) {
                debug!("Casted column: {col_name} to f64.");
                data.replace(
                    col_name.as_str(),
                    casted.into_materialized_series().to_owned(),
                )
                .map_err(|err| TransformError::CastingError(err.to_string()))?;
                continue;
            }

            if let Some(dates) = column
                .str()
                .map_err(|err| TransformError::CastingError(err.to_string()))?
                .iter()
                .map(|s| s.and_then(try_parse_string_date))
                .collect::<Option<Vec<NaiveDate>>>()
            {
                debug!("Casted column: {col_name} to date.");
                let s: Series = Series::new(col_name.clone().into(), dates);
                data.replace(col_name.as_str(), s.clone())
                    .map_err(|err| TransformError::CastingError(err.to_string()))?;
                continue;
            }

            if let Some(dates) = column
                .str()
                .map_err(|err| TransformError::CastingError(err.to_string()))?
                .iter()
                .map(|s| s.and_then(try_parse_string_datetime))
                .collect::<Option<Vec<NaiveDateTime>>>()
            {
                debug!("Casted column: {col_name} to datetime.");
                let s: Series = Series::new(col_name.clone().into(), dates);
                data.replace(col_name.as_str(), s.clone())
                    .map_err(|err| TransformError::CastingError(err.to_string()))?;
                continue;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;
    use polars::prelude::TimeUnit;
    use rstest::rstest;

    #[rstest]
    fn test_polars_column_string_cast() {
        let mut df = df![
            "int_col" => &["1", "2", "3"],
            "float_col" => &["1.5", "2.5", "3.5"],
            "bool_col" => &["True", "False", "True"],
            "date_col" => &["2023-01-01", "2023-01-02", "2023-01-03"],
            "datetime_col" => &["2023-01-01T12:00:00", "2023-01-02T13:30:00", "2023-01-03T15:45:00"],
            "string_col" => &["hello", "world", "test"]
        ].unwrap();

        let result = TransformerModule::polars_column_string_cast(&mut df);
        assert!(result.is_ok());
        assert_eq!(df.column("int_col").unwrap().dtype(), &DataType::Int64);
        assert_eq!(df.column("float_col").unwrap().dtype(), &DataType::Float64);
        assert_eq!(df.column("bool_col").unwrap().dtype(), &DataType::Boolean);
        assert_eq!(df.column("date_col").unwrap().dtype(), &DataType::Date);
        assert_eq!(
            df.column("datetime_col").unwrap().dtype(),
            &DataType::Datetime(TimeUnit::Milliseconds, None)
        );
        assert_eq!(df.column("string_col").unwrap().dtype(), &DataType::String);
    }
}
