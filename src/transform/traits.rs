use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;

#[allow(dead_code)]
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
