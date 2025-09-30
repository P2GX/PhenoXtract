use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use std::fmt::Debug;

#[allow(dead_code)]
/// Represents a strategy for transforming a collection of references to `ContextualizedDataFrame` structs.
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
pub trait Strategy: Debug {
    fn transform(&self, tables: &mut [&mut ContextualizedDataFrame]) -> Result<(), TransformError> {
        match self.is_valid(tables) {
            true => self.internal_transform(tables),
            false => Ok(()),
        }
    }

    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool;

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), TransformError>;
}
