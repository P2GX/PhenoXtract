use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use polars::datatypes::DataType;
use polars::prelude::{Column, NamedFrom, Series};

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

struct WordSwap {
    input_string: String,
    output_string: String,
    columns_to_transform: Vec<String>,
    expected_context: Option<Context>,
}

impl Strategy for WordSwap {
    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool {
        let col_names = self.columns_to_transform.clone();
        //checks that all columns have the string data type
        //and that they all have the appropriate context
        col_names.iter().all(|col_name| {
            table.data().column(col_name).unwrap().dtype() == &DataType::String
                && match &self.expected_context {
                    Some(context) => table
                        .context()
                        .context
                        .iter()
                        .all(|s_context| *context == s_context.get_context()),
                    None => true,
                }
        })
    }
    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let col_names = self.columns_to_transform.clone();
        let mut vec_of_transformed_series: Vec<Series> = vec![];

        for col_name in col_names {
            let col = table.data().column(&*col_name).unwrap();
            let vec_of_strings = col
                .as_series()
                .unwrap()
                .iter()
                .map(|val| {
                    if val.get_str().unwrap() == "Male" {
                        "M".to_string()
                    } else {
                        val.get_str().unwrap().to_string()
                    }
                })
                .collect::<Vec<String>>();
            let transformed_s = Series::new(col_name.into(), vec_of_strings);
            vec_of_transformed_series.push(transformed_s);
        }

        for transformed_series in vec_of_transformed_series {
            let col_name = transformed_series.name().clone();
            table
                .data_mut()
                .replace(&*col_name, transformed_series)
                .unwrap();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::table_context::{
        CellContext, Context, SeriesContext, SingleSeriesContext, TableContext,
    };
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use crate::transform::traits::{Strategy, WordSwap};
    use polars::frame::DataFrame;
    use polars::prelude::NamedFrom;
    use polars::prelude::{AnyValue, Column};
    use polars::series::Series;
    use rstest::{fixture, rstest};

    #[fixture]
    fn tc() -> TableContext {
        TableContext::new(
            "patient_sex_column".to_string(),
            vec![SeriesContext::Single(SingleSeriesContext::new(
                "sex".to_string(),
                Context::SubjectSex,
                Some(CellContext::new(
                    Context::SubjectId,
                    None,
                    Default::default(),
                )),
                vec![],
            ))],
        )
    }

    #[fixture]
    fn data() -> DataFrame {
        let col = Column::new("sex".into(), ["Male", "Female", "Female", "Male"]);
        DataFrame::new(vec![col]).unwrap()
    }

    #[fixture]
    fn cdf(tc: TableContext, data: DataFrame) -> ContextualizedDataFrame {
        ContextualizedDataFrame::new(tc, data)
    }

    #[rstest]
    fn test_transformation(mut cdf: ContextualizedDataFrame) {
        let a = WordSwap {
            input_string: String::from("Male"),
            output_string: String::from("M"),
            columns_to_transform: vec!["sex".to_string()],
            expected_context: Some(Context::SubjectSex),
        };

        println!("{:?}", cdf);
        a.transform(&mut cdf).unwrap();
        println!("{:?}", cdf);
    }
}
