use crate::config::table_context::{MultiIdentifier, SeriesContext, SetId, TableContext};
use crate::extract::error::ContextError;
use polars::prelude::DataFrame;

/// A structure that combines a `DataFrame` with its corresponding `TableContext`.
///
/// This allows for processing the data within the `DataFrame` according to the
/// rules and semantic information defined in the context.
#[derive(Debug, PartialEq)]
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

    pub fn replace_context_id(&mut self, old: &str, new: &str) -> Result<(), ContextError> {
        let series_context = self.get_series_context_mut(old);
        let series_context_unwrapped = series_context.ok_or_else(|| {
            ContextError::NotFound(format!("No context found for identifier {old}."))
        })?;

        match series_context_unwrapped {
            SeriesContext::Single(_) => {
                series_context_unwrapped.set_id(SetId::Single(new.to_owned()))?;
            }
            SeriesContext::Multi(multi) => {
                if let MultiIdentifier::Multi(ref mut ids) = multi.multi_identifier {
                    if let Some(index) = ids.iter().position(|word| word == old) {
                        ids[index] = new.to_string();
                    } else {
                        return Err(ContextError::MultiIdNotFound(format!(
                            "Could not replace multi id: {old} not found."
                        )));
                    }
                } else if let MultiIdentifier::Regex(_) = multi.multi_identifier {
                    series_context_unwrapped.set_id(SetId::MultiRegex(new.to_owned()))?;
                }
            }
        }

        Ok(())
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
