use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::{DataProcessingError, StrategyError};
use std::borrow::Cow;

use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::traits::Strategy;
use log::info;
use polars::datatypes::DataType;
use polars::prelude::{ChunkApply, ChunkCast};
use std::string::ToString;

#[derive(Debug)]
pub struct HgvsCorrectionStrategy;

impl Strategy for HgvsCorrectionStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&Context::Hgvs))
                .where_dtype(Filter::Is(&DataType::String))
                .collect()
                .is_empty()
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!("Applying HgvsCorrection strategy to data.");
        info!(
            "Asterixes will be replaced by colons: NM_001173464.1*c.2860C>T -> NM_001173464.1:c.2860C>T."
        );

        for table in tables.iter_mut() {
            info!(
                "Applying HgvsCorrection strategy to table: {}",
                table.context().name()
            );

            let hgvs_col_names: Vec<String> = table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&Context::Hgvs))
                .collect()
                .iter()
                .map(|col| col.name().to_string())
                .collect();

            for hgvs_col_name in hgvs_col_names {
                let hgvs_col = table.data().column(&hgvs_col_name)?;

                let corrected_hgvs_col = hgvs_col
                    .str()?
                    .apply(|s_opt| s_opt.map(|hgvs| Cow::Owned(hgvs.replace('*', ":"))));

                table
                    .builder()
                    .replace_column(
                        &hgvs_col_name,
                        corrected_hgvs_col.cast(&DataType::String).map_err(|_| {
                            DataProcessingError::CastingError {
                                col_name: hgvs_col_name.to_string(),
                                from: corrected_hgvs_col.dtype().clone(),
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
