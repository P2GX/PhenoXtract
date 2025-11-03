use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::collector::Collector;
use crate::transform::error::{StrategyError, TransformError};
use crate::transform::traits::Strategy;
use crate::transform::utils::polars_column_cast_ambivalent;
use phenopackets::schema::v2::Phenopacket;

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
            Self::polars_dataframe_cast_ambivalent(table)?;
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

    fn polars_dataframe_cast_ambivalent(
        cdf: &mut ContextualizedDataFrame,
    ) -> Result<(), StrategyError> {
        let col_names: Vec<String> = cdf
            .data()
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        for col_name in col_names {
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
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use polars::df;
    use polars::prelude::{DataType, TimeUnit};
    use rstest::rstest;

    #[rstest]
    fn test_polars_dataframe_cast_ambivalent() {
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
                        .with_identifier(Identifier::Regex("int_col".to_string())),
                ],
            ),
            df.clone(),
        );

        let result = TransformerModule::polars_dataframe_cast_ambivalent(&mut cdf);
        assert!(result.is_ok());
        assert_eq!(
            cdf.data().column("int_col").unwrap().dtype(),
            &DataType::Int32
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
}
