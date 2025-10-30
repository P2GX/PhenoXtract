use crate::config::table_context::Context;
use crate::extract::ContextualizedDataFrame;
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::error::StrategyError::MappingError;
use crate::transform::error::{MappingErrorInfo, MappingSuggestion, StrategyError};
use crate::transform::traits::Strategy;
use fenominal::fenominal::{Fenominal, FenominalHit};
use log::warn;
use ontolius::ontology::csr::FullCsrOntology;
use polars::datatypes::PlSmallStr;
use polars::prelude::{ChunkApply, Column, NamedFrom, Series};
use std::any::type_name;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug)]
pub struct FenominalStrategy {
    pub hpo: Arc<FullCsrOntology>,
}

impl Strategy for FenominalStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        true
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        let fenominal = Fenominal::new(self.hpo.clone());
        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();
        for table in tables.iter_mut() {
            let hpo_column_names: Vec<PlSmallStr> = table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&Context::HpoLabelOrId))
                .collect()
                .iter()
                .map(|col| col.name())
                .cloned()
                .collect();

            for hpo_col_name in hpo_column_names {
                let mut new_data: Vec<Option<String>> = Vec::new();

                let col = table.data().column(&hpo_col_name)?;

                let str_col_iter = col.str()?.iter();

                for val in str_col_iter {
                    if let Some(val_val) = val {
                        let mut fenominal_hits: Vec<FenominalHit> = fenominal.process(val_val);
                        fenominal_hits.sort_by_key(|hit| hit.span.start);

                        if !fenominal_hits.is_empty() {
                            let hit = fenominal_hits.first().unwrap().term_id.clone();
                            new_data.push(Some(hit));
                        } else {
                            let mapping_error_info = MappingErrorInfo {
                                column: col.name().to_string(),
                                table: table.context().name().to_string(),
                                old_value: val_val.to_string(),
                                possible_mappings: vec![],
                            };
                            if !error_info.contains(&mapping_error_info) {
                                error_info.insert(mapping_error_info);
                            }
                            new_data.push(None);
                        }
                    } else {
                        new_data.push(None);
                    }
                }
                let new_col = Series::new(col.name().clone(), new_data);

                table
                    .builder()
                    .replace_column(&hpo_col_name, new_col)?
                    .build()?;
            }
        }
        if !error_info.is_empty() {
            print!("{:?}", error_info.len());
            Err(MappingError {
                strategy_name: type_name::<Self>().split("::").last().unwrap().to_string(),
                info: error_info.into_iter().collect(),
            })
        } else {
            Ok(())
        }
    }
}
