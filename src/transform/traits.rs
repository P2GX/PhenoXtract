use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use polars::datatypes::DataType;

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
    expected_context: Option<Context>,
}

impl Strategy for WordSwap {
    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool {
        let only_string_cols_bool = table
            .data()
            .dtypes()
            .iter()
            .all(|dtype| *dtype == DataType::String);
        let expected_context_bool = match &self.expected_context {
            Some(context) => table
                .context()
                .context
                .iter()
                .all(|s_context| *context == s_context.get_context()),
            None => true,
        };
        only_string_cols_bool && expected_context_bool
    }
    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        //how to do this...

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
    use polars::prelude::Column;
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
            expected_context: Some(Context::SubjectSex),
        };
        a.transform(&mut cdf).expect("transform failed");
    }
}
