use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use polars::datatypes::DataType;
use polars::prelude::{AnyValue, NamedFrom, Series};

#[allow(dead_code)]
/// Represents a strategy for transforming a `ContextualizedDataFrame`.
///
/// This trait defines a standard interface for applying a conditional transformation
/// to a data structure in place. It decouples the decision of *whether* a transformation
/// should be applied from the transformation logic itself.
///
/// The main entry point is the `transform` method, which first checks for validity
/// using `is_valid`. If the check passes, it proceeds to execute the core logic
/// defined in `internal_transform`. This pattern ensures that transformations are
/// only attempted when the context is appropriate, preventing unnecessary work or
/// potential errors.
pub trait Strategy {
    fn transform(&self, table: &mut ContextualizedDataFrame) -> Result<(), TransformError> {
        match self.is_valid(table) {
            true => self.internal_transform(table),
            false => Ok(()),
        }
    }

    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool;

    fn internal_transform(&self, table: &mut ContextualizedDataFrame)
    -> Result<(), TransformError>;
}

pub struct StringSwap {
    pub input_string: String,
    pub output_string: String,
    pub table_column_pairs_to_transform: Vec<[String; 2]>,
}

impl StringSwap {
    pub fn get_col_names(&self) -> Vec<String> {
        self.table_column_pairs_to_transform
            .iter()
            .map(|pair| pair[1].clone())
            .collect::<Vec<String>>()
    }

    pub fn get_table_names(&self) -> Vec<String> {
        self.table_column_pairs_to_transform
            .iter()
            .map(|pair| pair[1].clone())
            .collect::<Vec<String>>()
    }
}

impl Strategy for StringSwap {
    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool {
        //checks that all relevant columns have the string data type
        self.get_col_names().iter().all(|col_name| {
            let col_search_result = table.data().column(col_name);
            match col_search_result {
                Ok(col) => col.dtype() == &DataType::String,
                Err(_) => false,
            }
        })
    }
    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let output_string = self.output_string.clone();
        let table_column_pairs_to_transform = self.table_column_pairs_to_transform.clone();

        for table_col_pair in table_column_pairs_to_transform {
            let table_name = &table_col_pair[0];
            let col_name = &table_col_pair[1];
            if table_name == &table.context().name {
                let col_search_result = table.data().column(col_name);
                match col_search_result {
                    Ok(col) => {
                        let vec_of_strings = col
                            .as_series()
                            .unwrap()
                            .iter()
                            .map(|val| match val {
                                AnyValue::String(s) => {
                                    if s == self.input_string {
                                        AnyValue::String(&output_string)
                                    } else {
                                        AnyValue::String(s)
                                    }
                                }
                                _ => AnyValue::Null,
                            })
                            .collect::<Vec<AnyValue>>();
                        let transformed_s = Series::new(col_name.into(), vec_of_strings);
                        table.data_mut().replace(col_name, transformed_s).unwrap();
                    }
                    Err(_) => {
                        return Err(StrategyError(
                            format!("Could not find column {col_name} in table {table_name}.")
                                .to_string(),
                        ));
                    }
                }
            } else {
                continue;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::TableContext;
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::transform::traits::{Strategy, StringSwap};
    use polars::frame::DataFrame;
    use polars::prelude::Column;
    use rstest::{fixture, rstest};

    #[fixture]
    fn tc() -> TableContext {
        TableContext::new("patient_data".to_string(), vec![])
    }

    #[fixture]
    fn data() -> DataFrame {
        let col1 = Column::new("patient_id".into(), ["P001", "P002", "P003", "P004"]);
        let col2 = Column::new("sex".into(), ["Male", "Female", "Female", "Male"]);
        DataFrame::new(vec![col1, col2]).unwrap()
    }

    #[fixture]
    fn cdf(tc: TableContext, data: DataFrame) -> ContextualizedDataFrame {
        ContextualizedDataFrame::new(tc, data)
    }

    #[rstest]
    fn test_transformation(mut cdf: ContextualizedDataFrame) {
        let male_to_m = StringSwap {
            input_string: String::from("Male"),
            output_string: String::from("M"),
            table_column_pairs_to_transform: vec![["patient_data".to_string(), "sex".to_string()]],
        };

        println!("{:?}", cdf);
        male_to_m.transform(&mut cdf).unwrap();
        println!("{:?}", cdf);
    }
}
