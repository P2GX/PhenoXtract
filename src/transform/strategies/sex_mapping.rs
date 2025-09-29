use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::{MappingErrorInfo, MappingSuggestion, TransformError};

use crate::transform::traits::Strategy;
use log::{debug, info, warn};
use phenopackets::schema::v2::core::Sex;
use polars::datatypes::DataType;
use polars::prelude::ChunkCast;
use std::any::type_name;
use std::collections::{HashMap, HashSet};
use std::string::ToString;

struct MappingStrategy {
    synonym_map: HashMap<String, String>,
    data_context: Context,
    header_context: Context,
    column_dtype: DataType,
    out_dtype: DataType,
}

impl MappingStrategy {
    #[allow(unused)]
    pub fn add_alias(&mut self, alias: &str, term: &str) {
        self.synonym_map
            .insert(alias.trim().to_lowercase(), term.to_string());
    }

    #[allow(unused)]
    fn default_sex_mapping_strategy() -> MappingStrategy {
        MappingStrategy::new(
            HashMap::from([
                ("m".to_string(), Sex::Male.as_str_name().to_string()),
                ("f".to_string(), Sex::Female.as_str_name().to_string()),
                ("male".to_string(), Sex::Male.as_str_name().to_string()),
                ("female".to_string(), Sex::Female.as_str_name().to_string()),
                ("man".to_string(), Sex::Male.as_str_name().to_string()),
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
}

impl Strategy for MappingStrategy {
    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool {
        table.check_contexts_have_data_type(
            &self.header_context,
            &self.data_context,
            &self.column_dtype,
        )
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        info!(
            "Applying SexMapping strategy to table: {}",
            table.context().name
        );

        let col_names: Vec<String> = table
            .get_cols_with_contexts(&self.header_context, &self.data_context)
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();

        for col_name in col_names {
            let col = table.data.column(&col_name).unwrap();
            let mapped_column = col.str().unwrap().apply_mut(|cell_value| {
                if cell_value.is_empty() {
                    return cell_value;
                }

                match self.synonym_map.get(cell_value.to_lowercase().trim()) {
                    Some(alias) => {
                        debug!("Converted '{cell_value}' to '{alias}'");
                        alias
                    }
                    None => {
                        warn!("Unable to convert map '{cell_value}'");
                        error_info.insert(MappingErrorInfo {
                            column: col.name().to_string(),
                            table: table.context().clone().name,
                            old_value: cell_value.to_string(),
                            possible_mappings: MappingSuggestion::from_hashmap(&self.synonym_map),
                        });
                        cell_value
                    }
                }
            });

            table
                .data
                .replace(
                    &col_name,
                    mapped_column.cast(&self.out_dtype).map_err(|_| {
                        TransformError::StrategyError(format!(
                            "Unable to cast column from {} to {}",
                            self.column_dtype, self.out_dtype
                        ))
                    })?,
                )
                .map_err(|err| TransformError::StrategyError(err.to_string()))?;
        }
        if !error_info.is_empty() {
            Err(TransformError::MappingError {
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
            "sex" => &[AnyValue::String("m"), AnyValue::String("f"), AnyValue::String("male"), AnyValue::String("female"), AnyValue::String("man"), AnyValue::String("woman"), AnyValue::String("intersex"), AnyValue::String("mole"), AnyValue::Null]
        ]
        .unwrap();

        let tc = TableContext::new(
            "TestTable".to_string(),
            vec![SeriesContext::new(
                Identifier::Regex("sex".to_string()),
                Default::default(),
                Context::SubjectSex,
                None,
                None,
                vec![],
            )],
        );

        ContextualizedDataFrame::new(tc, df)
    }

    #[rstest]
    fn test_sex_mapping_strategy_success() {
        let mut table = make_test_dataframe();
        let filtered_table = table
            .data
            .lazy()
            .filter(col("sex").eq(lit("mole")).not())
            .collect()
            .unwrap();
        table.data = filtered_table;
        let strategy = MappingStrategy::default_sex_mapping_strategy();

        strategy.transform(&mut table).unwrap();

        let sex_values: Vec<String> = table
            .data
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
        let filtered_table = table
            .data
            .lazy()
            .filter(col("sex").eq(lit("male")))
            .collect()
            .unwrap();
        table.data = filtered_table;
        let mut strategy = MappingStrategy::default_sex_mapping_strategy();
        strategy.synonym_map = HashMap::from([("male".to_string(), "5.6".to_string())]);
        strategy.out_dtype = DataType::Float64;

        strategy.transform(&mut table).unwrap();
        assert_eq!(
            table.data.column("sex").unwrap().dtype(),
            &strategy.out_dtype
        );
    }

    #[rstest]
    fn test_sex_mapping_strategy_err() {
        let mut table = make_test_dataframe();
        let strategy = MappingStrategy::default_sex_mapping_strategy();

        let err = strategy.transform(&mut table);

        match err {
            Err(TransformError::MappingError {
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
            .data
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
