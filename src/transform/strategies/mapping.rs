use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::{
    DataProcessingError, MappingErrorInfo, MappingSuggestion, StrategyError,
};

use crate::extract::contextualized_dataframe_filters::Filter;
use crate::transform::traits::Strategy;
use log::{debug, info, warn};
use phenopackets::schema::v2::core::Sex;
use phenopackets::schema::v2::core::vital_status::Status;
use polars::datatypes::DataType;
use polars::prelude::{ChunkCast, Column};
use serde::{Deserialize, Serialize};
use std::any::type_name;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::string::ToString;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DefaultMapping {
    SexMapping,
}

/// A strategy for mapping string values to standardized terms using a synonym dictionary.
///
/// `MappingStrategy` transforms data by replacing cell values with their corresponding
/// mapped values from a synonym map. It's commonly used for data normalization tasks
/// such as standardizing gender/sex values, categorical data, or controlled vocabulary.
///
/// # Fields
///
/// * `synonym_map` - A mapping from input values (lowercase, trimmed) to their standardized output values
/// * `data_context` - The context type of the data being transformed (e.g., `Context::SubjectSex`)
/// * `header_context` - The context type of the column headers to match
/// * `column_dtype` - The expected data type of the input columns
/// * `out_dtype` - The desired data type of the output columns after mapping
///
/// # Example
///
/// ```ignore
/// let sex_mapping = MappingStrategy::default_sex_mapping_strategy();
/// // Maps variations like "m", "male", "man" → "MALE"
/// // and "f", "female", "woman" → "FEMALE"
/// ```
///
/// # Errors
///
/// Returns `TransformError::MappingError` if any values in the data cannot be found
/// in the synonym map, providing details about unmapped values and suggestions.
#[derive(Debug)]
pub struct MappingStrategy {
    synonym_map: HashMap<String, String>,
    data_context: Context,
    header_context: Context,
    column_dtype: DataType,
    out_dtype: DataType,
}

impl MappingStrategy {
    #[allow(dead_code)]
    pub fn new(
        synonym_map: HashMap<String, String>,
        data_context: Context,
        header_context: Context,
        column_dtype: DataType,
        out_dtype: DataType,
    ) -> Self {
        Self {
            synonym_map,
            data_context,
            header_context,
            column_dtype,
            out_dtype,
        }
    }

    #[allow(unused)]
    pub fn add_alias(&mut self, alias: &str, term: &str) {
        self.synonym_map
            .insert(alias.trim().to_lowercase(), term.to_string());
    }

    #[allow(unused)]
    pub fn default_sex_mapping_strategy() -> MappingStrategy {
        MappingStrategy::new(
            HashMap::from([
                ("m".to_string(), Sex::Male.as_str_name().to_string()),
                ("male".to_string(), Sex::Male.as_str_name().to_string()),
                ("man".to_string(), Sex::Male.as_str_name().to_string()),
                ("f".to_string(), Sex::Female.as_str_name().to_string()),
                ("female".to_string(), Sex::Female.as_str_name().to_string()),
                ("woman".to_string(), Sex::Female.as_str_name().to_string()),
                (
                    "diverse".to_string(),
                    Sex::OtherSex.as_str_name().to_string(),
                ),
                (
                    "intersex".to_string(),
                    Sex::OtherSex.as_str_name().to_string(),
                ),
                ("other".to_string(), Sex::OtherSex.as_str_name().to_string()),
            ]),
            Context::SubjectSex,
            Context::None,
            DataType::String,
            DataType::String,
        )
    }

    #[allow(unused)]
    pub fn default_vital_status_mapping_strategy() -> MappingStrategy {
        MappingStrategy::new(
            HashMap::from([
                ("yes".to_string(), Status::Alive.as_str_name().to_string()),
                (
                    "living".to_string(),
                    Status::Alive.as_str_name().to_string(),
                ),
                ("alive".to_string(), Status::Alive.as_str_name().to_string()),
                ("no".to_string(), Status::Deceased.as_str_name().to_string()),
                (
                    "dead".to_string(),
                    Status::Deceased.as_str_name().to_string(),
                ),
                (
                    "deceased".to_string(),
                    Status::Deceased.as_str_name().to_string(),
                ),
                (
                    "unknown".to_string(),
                    Status::UnknownStatus.as_str_name().to_string(),
                ),
                (
                    "no data".to_string(),
                    Status::UnknownStatus.as_str_name().to_string(),
                ),
            ]),
            Context::VitalStatus,
            Context::None,
            DataType::String,
            DataType::String,
        )
    }
}

impl Strategy for MappingStrategy {
    fn is_valid(&self, tables: &[&mut ContextualizedDataFrame]) -> bool {
        tables.iter().any(|table| {
            !table
                .filter_columns()
                .where_header_context(Filter::Is(&self.header_context))
                .where_data_context(Filter::Is(&self.data_context))
                .where_dtype(Filter::Is(&self.column_dtype))
                .collect()
                .is_empty()
        })
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        info!(
            "Applying Mapping strategy to data. Applying synonyms to columns with header_context {} and data_context {}.",
            self.header_context, self.data_context
        );

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        for table in tables.iter_mut() {
            info!(
                "Applying Mapping strategy to table: {}",
                table.context().name()
            );

            let col_names: Vec<String> = table
                .filter_columns()
                .where_header_context(Filter::Is(&self.header_context))
                .where_data_context(Filter::Is(&self.data_context))
                .collect_owned_names();

            for col_name in col_names {
                let original_column = table.data().column(&col_name)?;

                let col: Cow<Column> = if original_column.dtype() != &DataType::String {
                    let casted_col = original_column.cast(&DataType::String).map_err(|_| {
                        DataProcessingError::CastingError {
                            col_name: col_name.clone(),
                            from: original_column.dtype().clone(),
                            to: DataType::String,
                        }
                    })?;
                    Cow::Owned(casted_col)
                } else {
                    Cow::Borrowed(original_column)
                };

                let mapped_column = col.str()?.apply_mut(|cell_value| {
                    if cell_value.is_empty() {
                        return cell_value;
                    }

                    match self.synonym_map.get(cell_value.to_lowercase().trim()) {
                        Some(alias) => {
                            debug!("Converted '{cell_value}' to '{alias}'");
                            alias
                        }
                        None => {
                            let mapping_error_info = MappingErrorInfo {
                                column: col.name().to_string(),
                                table: table.context().name().to_string(),
                                old_value: cell_value.to_string(),
                                possible_mappings: MappingSuggestion::from_hashmap(
                                    &self.synonym_map,
                                ),
                            };
                            if !error_info.contains(&mapping_error_info) {
                                warn!("Unable to convert map '{cell_value}'");
                                error_info.insert(mapping_error_info);
                            }
                            cell_value
                        }
                    }
                });
                table
                    .builder()
                    .replace_column(
                        &col_name,
                        mapped_column.cast(&self.out_dtype).map_err(|_| {
                            DataProcessingError::CastingError {
                                col_name: col_name.to_string(),
                                from: mapped_column.dtype().clone(),
                                to: self.out_dtype.clone(),
                            }
                        })?,
                    )?
                    .build()?;
            }
        }

        if !error_info.is_empty() {
            Err(StrategyError::MappingError {
                strategy_name: type_name::<Self>().split("::").last().unwrap().to_string(),
                info: error_info.into_iter().collect(),
            })
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
    use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
    use polars::prelude::*;
    use rstest::rstest;

    fn make_test_dataframe() -> ContextualizedDataFrame {
        let df = df![
            "sex" => &[AnyValue::String("m"), AnyValue::String("f"), AnyValue::String("male"), AnyValue::String("female"), AnyValue::String("man"), AnyValue::String("woman"), AnyValue::String("intersex"), AnyValue::String("mole"), AnyValue::Null],
            "sub_id" => &[AnyValue::String("1"), AnyValue::String("2"), AnyValue::String("3"), AnyValue::String("4"), AnyValue::String("5"), AnyValue::String("6"), AnyValue::String("7"), AnyValue::String("8"), AnyValue::Null]
        ]
        .unwrap();

        let tc = TableContext::new(
            "TestTable".to_string(),
            vec![
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("sex".to_string()))
                    .with_data_context(Context::SubjectSex),
                SeriesContext::default()
                    .with_identifier(Identifier::Regex("sub_id".to_string()))
                    .with_data_context(Context::SubjectId),
            ],
        );

        ContextualizedDataFrame::new(tc, df)
    }

    #[rstest]
    fn test_sex_mapping_strategy_success() {
        let table = make_test_dataframe();
        let filtered_table = table
            .clone()
            .into_data()
            .lazy()
            .filter(col("sex").eq(lit("mole")).not())
            .collect()
            .unwrap();

        let mut table = ContextualizedDataFrame::new(table.context().clone(), filtered_table);

        let strategy = MappingStrategy::default_sex_mapping_strategy();

        strategy.transform(&mut [&mut table]).unwrap();

        let sex_values: Vec<String> = table
            .data()
            .column("sex")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .map(ToOwned::to_owned)
            .collect();

        assert_eq!(
            sex_values,
            vec![
                "MALE",
                "FEMALE",
                "MALE",
                "FEMALE",
                "MALE",
                "FEMALE",
                "OTHER_SEX"
            ]
        );
    }

    #[rstest]
    fn test_float_cast() {
        let mut table = make_test_dataframe();

        let series = Series::new("sex".into(), vec![5.6]);
        table
            .builder()
            .replace_column("sex", series.clone())
            .unwrap()
            .build()
            .unwrap();

        let mut strategy = MappingStrategy::default_sex_mapping_strategy();
        strategy.synonym_map = HashMap::from([("5.6".to_string(), "male".to_string())]);
        strategy.column_dtype = DataType::Float64;
        strategy.out_dtype = DataType::String;

        strategy.transform(&mut [&mut table]).unwrap();
        assert_eq!(
            table.data().column("sex").unwrap().dtype(),
            &strategy.out_dtype
        );

        table
            .data()
            .column(series.name())
            .unwrap()
            .str()
            .unwrap()
            .apply_mut(|cell| {
                assert_eq!(cell, "male");
                cell
            });
    }

    #[rstest]
    fn test_sex_mapping_strategy_err() {
        let mut table = make_test_dataframe();
        let strategy = MappingStrategy::default_sex_mapping_strategy();

        let err = strategy.transform(&mut [&mut table]);

        match err {
            Err(StrategyError::MappingError {
                strategy_name,
                mut info,
            }) => {
                let i = info.pop().unwrap();
                assert_eq!(strategy_name, "MappingStrategy");
                assert_eq!(i.old_value, "mole");
                assert_eq!(i.column, "sex");
                assert_eq!(i.table, "TestTable");
                assert_eq!(
                    MappingSuggestion::suggestions_to_hashmap(i.possible_mappings),
                    strategy.synonym_map
                );
            }
            _ => panic!("Unexpected error"),
        }

        let sex_values: Vec<String> = table
            .data()
            .column("sex")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .map(ToOwned::to_owned)
            .collect();

        assert_eq!(
            sex_values,
            vec![
                "MALE",
                "FEMALE",
                "MALE",
                "FEMALE",
                "MALE",
                "FEMALE",
                "OTHER_SEX",
                "mole",
                "",
            ]
        );
    }
}
