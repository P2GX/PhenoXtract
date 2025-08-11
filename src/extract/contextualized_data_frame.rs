use crate::config::table_context::TableContext;
use polars::prelude::*;

pub struct ContextualizedDataFrame {
    context: TableContext,
    data: DataFrame,
}
