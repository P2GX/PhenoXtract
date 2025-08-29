use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::traits::Strategy;
use polars::datatypes::{AnyValue, DataType};
use polars::prelude::{NamedFrom, Series};

pub struct GenericStringTransformation {
    pub table_col_pair_to_transform: [String; 2],
    pub transformation: fn(&str) -> String,
}

impl Strategy for GenericStringTransformation {
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
        let table_name = &self.table_col_pair_to_transform[0];
        let col_name = &self.table_col_pair_to_transform[1];
        let transformation = self.transformation;

        if table_name == &table.context().name {
            let col_search_result = table.data().column(col_name);
            match col_search_result {
                Ok(col) => {
                    let vec_of_transformed_strings = col
                        .as_series()
                        .unwrap()
                        .iter()
                        .map(|val| match val {
                            AnyValue::String(s) => transformation(s),
                            _ => "".to_string(),
                        })
                        .collect::<Vec<String>>();
                    let transformed_s = Series::new(col_name.into(), vec_of_transformed_strings);
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
    use crate::transform::generic_string_transformation::GenericStringTransformation;
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
        fn string_swap(input_string: &str, output_string: &str, s: &str) -> String {
            if s == input_string {
                output_string.to_string()
            } else {
                s.to_string()
            }
        }

        //todo but how would this deserialise?
        //ideally we could create a StringSwap struct which is some sort of child of GenericStringTransformation...
        let male_to_m = GenericStringTransformation {
            table_col_pair_to_transform: ["patient_data".to_string(), "sex".to_string()],
            transformation: |s| string_swap("Male", "M", s),
        };

        println!("{:?}", cdf);
        male_to_m.transform(&mut cdf).unwrap();
        println!("{:?}", cdf);
    }
}
