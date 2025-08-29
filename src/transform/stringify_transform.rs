use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::traits::Strategy;
use polars::prelude::{NamedFrom, Series};

pub struct Stringify {
    pub table_col_pair_to_stringify: [String; 2],
}

impl Strategy for Stringify {
    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool {
        //checks that the relevant column exists in the relevant table
        if table.context().name == self.table_col_pair_to_stringify[0] {
            let col_search_result = table.data().column(&self.table_col_pair_to_stringify[1]);
            col_search_result.is_ok()
        } else {
            true
        }
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let table_name = &self.table_col_pair_to_stringify[0];
        let col_name = &self.table_col_pair_to_stringify[1];

        if table_name == &table.context().name {
            let col_search_result = table.data().column(col_name);
            match col_search_result {
                Ok(col) => {
                    let vec_of_strings = col
                        .as_series()
                        .unwrap()
                        .iter()
                        .map(|val| val.to_string())
                        .collect::<Vec<String>>();
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
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::TableContext;
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::transform::stringify_transform::Stringify;
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
        let col2 = Column::new("age".into(), [35, 100, 10, 25]);
        DataFrame::new(vec![col1, col2]).unwrap()
    }

    #[fixture]
    fn cdf(tc: TableContext, data: DataFrame) -> ContextualizedDataFrame {
        ContextualizedDataFrame::new(tc, data)
    }

    #[rstest]
    fn test_transformation(mut cdf: ContextualizedDataFrame) {
        let age_to_string = Stringify {
            table_col_pair_to_stringify: ["patient_data".to_string(), "age".to_string()],
        };

        println!("{:?}", cdf);
        age_to_string.transform(&mut cdf).unwrap();
        println!("{:?}", cdf);
    }
}
