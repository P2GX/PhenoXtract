use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::collector::Collector;
use crate::transform::error::{StrategyError, TransformError};
use crate::transform::traits::Strategy;
use crate::transform::utils::polars_column_cast_ambivalent;
use phenopackets::schema::v2::Phenopacket;
use polars::frame::DataFrame;

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
            Self::polars_dataframe_cast_ambivalent(table.data_mut())?;
        }

        for strategy in &self.strategies {
            strategy.transform(tables_refs.as_mut_slice())?;
        }

        Ok(self.collector.collect(data)?)
    }

    pub fn new(strategies: Vec<Box<dyn Strategy>>, collector: Collector) -> Self {
        TransformerModule {
            strategies,
            collector,
        }
    }

    fn polars_dataframe_cast_ambivalent(data: &mut DataFrame) -> Result<(), StrategyError> {
        let col_names: Vec<String> = data
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        for col_name in col_names {
            let column = data.column(col_name.as_str())?;

            let casted_series = polars_column_cast_ambivalent(column).take_materialized_series();

            data.replace(col_name.as_str(), casted_series.clone())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;
    use polars::prelude::{DataType, TimeUnit};
    use rstest::rstest;

    #[rstest]
    fn test_polars_dataframe_cast_ambivalent() {
        let mut df = df![
            "int_col" => &["1", "2", "3"],
            "float_col" => &["1.5", "2.5", "3.5"],
            "bool_col" => &["True", "False", "True"],
            "date_col" => &["2023-01-01", "2023-01-02", "2023-01-03"],
            "datetime_col" => &["2023-01-01T12:00:00", "2023-01-02T13:30:00", "2023-01-03T15:45:00"],
            "string_col" => &["hello", "world", "test"]
        ].unwrap();

        let result = TransformerModule::polars_dataframe_cast_ambivalent(&mut df);
        assert!(result.is_ok());
        assert_eq!(df.column("int_col").unwrap().dtype(), &DataType::Int32);
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
