use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::{DataProcessingError, StrategyError};

use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::traits::Strategy;
use log::info;
use polars::datatypes::DataType;
use polars::prelude::{ChunkCast, StringNameSpaceImpl};
use std::string::ToString;

#[derive(Debug)]
pub struct StringCorrectionStrategy {
    header_context: Context,
    data_context: Context,
    chars_to_replace: String,
    new_chars: String,
}

impl StringCorrectionStrategy {
    pub fn new(
        header_context: Context,
        data_context: Context,
        chars_to_replace: String,
        new_chars: String,
    ) -> Self {
        Self {
            header_context,
            data_context,
            chars_to_replace,
            new_chars,
        }
    }
}

impl Strategy for StringCorrectionStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&self.data_context))
                .where_dtype(Filter::Is(&DataType::String))
                .collect()
                .is_empty()
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!("Applying StringCorrection strategy to data.");

        for table in tables.iter_mut() {
            info!(
                "Applying StringCorrection strategy to table: {}",
                table.context().name()
            );

            let col_names: Vec<String> = table
                .filter_columns()
                .where_header_context(Filter::Is(&self.header_context))
                .where_data_context(Filter::Is(&self.data_context))
                .collect()
                .iter()
                .map(|col| col.name().to_string())
                .collect();

            for col_name in col_names {
                let col = table.data().column(&col_name)?;

                let corrected_col = col
                    .str()?
                    .replace_literal_all(self.chars_to_replace.as_str(), self.new_chars.as_str())?;

                table
                    .builder()
                    .replace_column(
                        &col_name,
                        corrected_col.cast(&DataType::String).map_err(|_| {
                            DataProcessingError::CastingError {
                                col_name: col_name.to_string(),
                                from: corrected_col.dtype().clone(),
                                to: DataType::String,
                            }
                        })?,
                    )?
                    .build()?;
            }
        }

        Ok(())
    }
}
