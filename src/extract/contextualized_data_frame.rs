use crate::config::table_context::{MultiIdentifier, SeriesContext, SetId, TableContext};
use crate::extract::error::ContextError;
use polars::prelude::DataFrame;

/// A structure that combines a `DataFrame` with its corresponding `TableContext`.
///
/// This allows for processing the data within the `DataFrame` according to the
/// rules and semantic information defined in the context.
#[derive(Debug, PartialEq, Clone)]
pub struct ContextualizedDataFrame {
    #[allow(unused)]
    context: TableContext,
    #[allow(unused)]
    pub data: DataFrame,
}

impl ContextualizedDataFrame {
    pub fn new(context: TableContext, data: DataFrame) -> Self {
        ContextualizedDataFrame { context, data }
    }

    #[allow(unused)]
    pub fn context(&self) -> &TableContext {
        &self.context
    }

    #[allow(unused)]
    pub fn context_mut(&mut self) -> &mut TableContext {
        &mut self.context
    }

    #[allow(unused)]
    pub fn get_series_context(&self, identifier: &str) -> Option<&SeriesContext> {
        self.context
            .context
            .iter()
            .find(|ctx| ctx.matches_identifier(identifier))
    }

    pub fn get_series_context_mut(&mut self, identifier: &str) -> Option<&mut SeriesContext> {
        self.context
            .context
            .iter_mut()
            .find(|ctx| ctx.matches_identifier(identifier))
    }
}
