use crate::config::table_context::TableContext;
use polars::prelude::*;

pub struct ContextualizedDataFrame {
    #[allow(unused)]
    context: TableContext,
    #[allow(unused)]
    data: DataFrame,
}
