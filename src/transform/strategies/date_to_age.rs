use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::ontology::ontology_bidict::OntologyBiDict;
use crate::transform::error::StrategyError::MappingError;
use crate::transform::error::{MappingErrorInfo, StrategyError};
use crate::transform::traits::Strategy;
use log::{info, warn};

use crate::extract::contextualized_dataframe_filters::Filter;

use polars::prelude::{DataType, IntoSeries, PlSmallStr};
use std::any::type_name;
use std::collections::HashSet;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug)]
/// todo!
pub struct DateToAgeStrategy;

impl DateToAgeStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl Strategy for DateToAgeStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        let exists_dob_column = tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&Context::None))
                .where_data_context(Filter::Is(&Context::DateOfBirth))
                .collect()
                .is_empty()
        });
        if exists_dob_column {
            true
        } else {
            warn!("No DateOfBirth column found in data. DateToAge Strategy cannot be applied.");
            false
        }
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!("Applying DateToAge strategy to data.");

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        let dob_table = tables.iter().find(|table|                !table
            .filter_columns()
            .where_data_context(Filter::Is(&Context::DateOfBirth))
            .collect()
            .is_empty()).expect("Unexpectedly could not find table with DateOfBirth data when applying DateToAge strategy.");

        let dob_column = dob_table
            .filter_columns()
            .where_data_context(Filter::Is(&Context::DateOfBirth))
            .collect()
            .first()
            .expect("Unexpectedly could not find DateOfBirth column in table.");

        let patient_dob_hash_map = dob_table.create_subject_id_string_data_hash_map(dob_column.str()?);

        for table in tables.iter_mut() {

            let stringified_subject_id_col = table.get_subject_id_col().str()?;

            let age_column_names = table
                .filter_columns()
                .where_data_context_is_age()
                .collect_owned_names();

            for age_col_name in age_column_names {
                let stringified_age_col = table.data().column(&age_col_name)?.str()?;
                let


                table
                    .builder()
                    .replace_column(&age_col_name, mapped_column.into_series())?
                    .build()?;
            }
        }

        // return an error if not every cell term could be parsed
        if !error_info.is_empty() {
            Err(MappingError {
                strategy_name: type_name::<Self>().split("::").last().unwrap().to_string(),
                info: error_info.into_iter().collect(),
            })
        } else {
            Ok(())
        }
    }
}
