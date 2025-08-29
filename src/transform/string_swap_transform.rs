use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::traits::Strategy;
use polars::datatypes::{AnyValue, DataType};
use polars::prelude::{NamedFrom, Series};

pub struct StringSwap {
    pub input_string: String,
    pub output_string: String,
    pub table_col_pair_to_transform: [String; 2],
}
impl Strategy for StringSwap {
    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool {
        //checks that the relevant column exists and has the string data type
        if table.context().name == self.table_col_pair_to_transform[0] {
            let col_search_result = table.data().column(&self.table_col_pair_to_transform[1]);
            match col_search_result {
                Ok(col) => col.dtype() == &DataType::String,
                Err(_) => false,
            }
        } else {
            true
        }
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let output_string = &self.output_string;
        let table_name = &self.table_col_pair_to_transform[0];
        let col_name = &self.table_col_pair_to_transform[1];

        if table_name == &table.context().name {
            let col_search_result = table.data().column(col_name);
            match col_search_result {
                Ok(col) => {
                    let vec_of_any_values = col
                        .as_series()
                        .unwrap()
                        .iter()
                        .map(|val| match val {
                            AnyValue::String(s) => {
                                if s == self.input_string {
                                    AnyValue::String(output_string)
                                } else {
                                    AnyValue::String(s)
                                }
                            }
                            _ => AnyValue::Null,
                        })
                        .collect::<Vec<AnyValue>>();
                    let transformed_s = Series::new(col_name.into(), vec_of_any_values);
                    table.data_mut().replace(col_name, transformed_s).unwrap();
                }
                Err(_) => {
                    return Err(StrategyError(
                        format!("Could not find column {col_name} in table {table_name}.")
                            .to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::TableContext;
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::transform::string_swap_transform::StringSwap;
    use crate::transform::traits::Strategy;
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
            table_col_pair_to_transform: ["patient_data".to_string(), "sex".to_string()],
        };

        println!("{:?}", cdf);
        male_to_m.transform(&mut cdf).unwrap();
        println!("{:?}", cdf);
    }
}
