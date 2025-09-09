use std::collections::HashMap;
use crate::config::table_context::{CellValue, SeriesContext};
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::traits::Strategy;

pub struct AliasMapTransform {

}

impl Strategy for AliasMapTransform {
    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool {
        true
    }

    fn internal_transform(&self, table: &mut ContextualizedDataFrame)
                          -> Result<(), TransformError> {
        let mut col_name_alias_map_pairs = vec![];
        for series_context in &table.context().context {
            if let Some(cc) = series_context.get_cell_context_option() {
                //todo currently just implementing this for single_sc because I don't yet know how to work with regex
                if let SeriesContext::Single(single_sc) = series_context {
                    col_name_alias_map_pairs.push((&single_sc.identifier,cc.get_alias_map()));
                }
            }
        }

        for (col_name,alias_map) in col_name_alias_map_pairs {

        }

        Ok(())
    }
}