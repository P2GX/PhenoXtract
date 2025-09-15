use crate::config::table_context::{AliasMap, CellValue};
use crate::config::table_context::AliasMap::{ToBool, ToFloat, ToInt, ToString};
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::error::TransformError::StrategyError;
use crate::transform::traits::Strategy;
use log::info;
use polars::prelude::{col, lit, AnyValue, Column, FillNullStrategy, IntoLazy};
use crate::config::table_context::CellValue::{BoolValue, FloatValue, IntValue, StringValue};

/// Given a contextualised dataframe, this strategy will apply all the aliases
/// found in the SeriesContexts.
/// For example if the Contextualised Dataframe has a SeriesContext consisting of a SubjectSex column
/// and a ToString AliasMap which converts "M" to "Male" and "F" to "Female"
/// then the strategy will apply those aliases to each cell.
/// # NOTE
/// This does not transform the headers of the Dataframe.
#[allow(dead_code)]
pub struct FillMissingStrategy;

impl FillMissingStrategy {

    fn get_col_fill_value_pairs(cdf: &ContextualizedDataFrame) -> Vec<(Column, CellValue)> {
        let mut col_fill_value_pairs = vec![];
        for series_context in cdf.get_series_contexts() {
            if let Some(cv) = series_context.get_fill_value() {
                let cols = cdf.get_columns(&series_context.identifier);
                for col_ref in cols {
                    col_fill_value_pairs.push((col_ref.clone(), cv.clone()))
                }
            }
        }
        col_fill_value_pairs
    }
}

impl Strategy for FillMissingStrategy {
    fn is_valid(&self, _table: &ContextualizedDataFrame) -> bool {
        true
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let table_name = &table.context().name.clone();
        info!("Applying fill missing strategy to table: {table_name}");

        for (col1, fill_value) in FillMissingStrategy::get_col_fill_value_pairs(table) {
            let col_name = col1.name();
            info!("Applying fill missing strategy to column: {col_name}");
            let df = table.data.clone().lazy();

            match fill_value {
                StringValue(fill_string) => {
                    let df = df
                        .with_column(
                            col(col_name.into()).fill_null(fill_string.into()).alias(col_name)
                        );
                    Ok(())
                }
                IntValue(fill_int) => {
                    let transformed_vec = Self::map_values(vec_to_fill, hm, col_name, table_name)?;
                    table.replace_column(transformed_vec, col_name)?;
                    Ok(())
                }
                FloatValue(fill_float) => {
                    let transformed_vec = Self::map_values(vec_to_fill, hm, col_name, table_name)?;
                    table.replace_column(transformed_vec, col_name)?;
                    Ok(())
                }
                BoolValue(fill_bool) => {
                    let transformed_vec = Self::map_values(vec_to_fill, hm, col_name, table_name)?;
                    table.replace_column(transformed_vec, col_name)?;
                    Ok(())
                }
            }?;
        }

        info!("Alias mapping strategy successfully applied to table: {table_name}");
        Ok(())
    }
}