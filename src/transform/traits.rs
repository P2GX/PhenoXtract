use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;

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
///
/// # Examples
///
/// A simple implementation that only runs if a specific column exists.
///
/// ```
/// #
/// #
/// /// A strategy that requires column "A" to be present.
/// struct RenameColumnAStrategy;
///
/// impl Strategy for RenameColumnAStrategy {
///     fn is_valid(&self, table: &ContextualizedDataFrame) -> bool {
///         // The transformation is only valid if the table has a column named "A".
///         table.has_column("A")
///     }
///
///     fn internal_transform(&self, table: &mut ContextualizedDataFrame) -> Result<(), TransformError> {
///         // The actual transformation logic goes here.
///         println!("Column 'A' found, applying transformation...");
///         table.rename("A", "B");
///         Ok(())
///     }
/// }
///
/// # fn main() -> Result<(), TransformError> {
/// let strategy = ColumnAStrategy;
/// let mut data_frame = ContextualizedDataFrame;
///
/// // The transform will run because `is_valid` returns true.
/// strategy.transform(&mut data_frame)?;
/// # Ok(())
/// # }
/// ```
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
