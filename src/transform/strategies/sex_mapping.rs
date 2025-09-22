use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::{MappingErrorInfo, MappingSuggestion, TransformError};
use crate::transform::traits::Strategy;
use std::any::type_name;

use crate::transform::strategies::utils::convert_col_to_string_vec;
use log::{debug, warn};
use phenopackets::schema::v2::core::Sex;
use polars::datatypes::DataType;
use polars::prelude::AnyValue;
use std::collections::{HashMap, HashSet};
use std::string::ToString;

/// A transformation strategy to map various string representations of sex to the
/// standardized `phenopackets::schema::v2::core::Sex` enum string representation.
///
/// This strategy identifies columns annotated with `Context::SubjectSex` and attempts
/// to convert their string values into a standard format (e.g., "MALE", "FEMALE").
/// It uses an internal `HashMap` for the mappings.
///
/// # Fields
///
/// * `map`: A `HashMap<String, String>` where the key is the input string (e.g., "m", "female")
///   and the value is the standardized string from the `phenopackets::schema::v2::core::Sex` enum
///   (e.g., "MALE", "FEMALE").
///
/// # Behavior
///
/// - The strategy processes each column identified by `Context::SubjectSex`.
/// - For each value in the column, it converts the value to lowercase before looking it up in the map.
/// - If a mapping is found, the original value is replaced with the standardized value.
/// - If no mapping is found, the value will be left unchanged, and an Err will be returned once the strategy has been applied to every SubjectSex column.
///
/// # Examples
///
/// The `default()` constructor provides a common set of mappings.
///
/// ```ignore
/// use crate::transform::sex_mapping::SexMappingStrategy;
/// use crate::transform::traits::Strategy;
/// use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
/// use crate::config::table_context::{TableContext, Context};
/// use polars::prelude::*;
/// use std::collections::HashMap;
///
/// // Assume we have a DataFrame like this
/// let df = df! {
///     "patient_id" => &[1, 2, 3, 4],
///     "gender" => &["m", "female", "MAN"],
/// }.unwrap();
///
/// // And a context mapping the "gender" column to SubjectSex
/// let mut table_context = TableContext::new("patients".to_string(), context: vec![SeriesContext::new(
///                     Identifier::Regex("gender".to_string()),
///                     Context::None,
///                     Context::SubjectSex,
///                     None,
///                     None,
///                     vec![],
///                 )]);
/// let mut cdf = ContextualizedDataFrame::new(df, table_context);
///
/// // Create and apply the strategy
/// let strategy = SexMappingStrategy::default();
/// strategy.transform(&mut cdf).unwrap();
///
/// // The "gender" column is now standardized
/// let expected_df = df! {
///     "patient_id" => &[1, 2, 3, 4],
///     "gender" => &[Some("MALE"), Some("FEMALE"), Some("MALE")],
/// }.unwrap();
///
/// assert_eq!(cdf.data, expected_df);
/// ```
struct SexMappingStrategy {
    synonym_map: HashMap<String, String>,
}

impl SexMappingStrategy {
    pub fn add_alias(&mut self, alias: String, term: Sex) {
        let term = term.as_str_name().to_string();
        self.synonym_map.insert(alias.trim().to_lowercase(), term);
    }

    fn default_synonym_map() -> HashMap<String, Sex> {
        HashMap::from([
            ("m".to_string(), Sex::Male),
            ("f".to_string(), Sex::Female),
            ("male".to_string(), Sex::Male),
            ("female".to_string(), Sex::Female),
            ("man".to_string(), Sex::Male),
            ("woman".to_string(), Sex::Female),
            ("diverse".to_string(), Sex::OtherSex),
            ("intersex".to_string(), Sex::OtherSex),
            ("other".to_string(), Sex::OtherSex),
        ])
    }
    #[allow(dead_code)]
    pub fn new(map: HashMap<String, Sex>) -> Self {
        let mut strategy = Self {
            synonym_map: HashMap::new(),
        };
        map.iter().for_each(|(k, v)| {
            strategy.add_alias(k.clone(), v.to_owned());
        });

        SexMappingStrategy::default_synonym_map()
            .iter()
            .for_each(|(k, v)| {
                strategy.add_alias(k.clone(), v.to_owned());
            });

        strategy
    }
    #[allow(dead_code)]
    pub fn default() -> Self {
        SexMappingStrategy::new(Self::default_synonym_map())
    }
}

impl Strategy for SexMappingStrategy {
    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool {
        let data_context = Context::SubjectSex;
        let dtype = DataType::String;

        let columns = table.get_cols_with_data_context(data_context.clone());
        let is_valid = columns.iter().all(|col| col.dtype() == &dtype);

        if !is_valid {
            warn!(
                "Not all columns with {} data context have {} type in table {}.",
                data_context,
                dtype,
                table.context().name
            );
        }
        is_valid
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let column_names: Vec<String> = table
            .get_cols_with_data_context(Context::SubjectSex)
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let mut error_info: HashSet<MappingErrorInfo> = HashSet::new();
        for col_name in column_names {
            let col_values: Vec<String> = convert_col_to_string_vec(
                table
                    .data
                    .column(&col_name)
                    .map_err(|err| TransformError::StrategyError(err.to_string()))?,
            )?;

            let mapped_column: Vec<AnyValue> = col_values
                .iter()
                .map(|s| match self.synonym_map.get(s.to_lowercase().trim()) {
                    Some(alias) => {
                        debug!("Converted {s} to {alias}");
                        AnyValue::String(alias)
                    }
                    None => {
                        if s == "null" {
                            return AnyValue::Null;
                        }

                        warn!("Unable to convert sex '{s}'");
                        error_info.insert(MappingErrorInfo {
                            column: col_name.clone(),
                            table: table.context().clone().name,
                            old_value: s.clone(),
                            possible_mappings: MappingSuggestion::from_hashmap(&self.synonym_map),
                        });
                        AnyValue::String(s)
                    }
                })
                .collect();
            table.replace_column(mapped_column, col_name.as_str())?;
        }

        if !error_info.is_empty() {
            return Err(TransformError::MappingError {
                strategy_name: type_name::<Self>().split("::").last().unwrap().to_string(),
                info: error_info.into_iter().collect(),
            });
        }

        Ok(())
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
        let strategy = SexMappingStrategy::default();

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
    fn test_sex_mapping_strategy_err() {
        let mut table = make_test_dataframe();
        let strategy = SexMappingStrategy::default();

        let err = strategy.transform(&mut table);

        match err {
            Err(TransformError::MappingError {
                strategy_name,
                mut info,
            }) => {
                let i = info.pop().unwrap();
                assert_eq!(strategy_name, "SexMappingStrategy");
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

    #[rstest]
    fn test_new_constructor_with_custom_and_default_mappings() {
        let mut user_map = HashMap::new();
        user_map.insert("gentleman".to_string(), Sex::Male);

        let strategy = SexMappingStrategy::new(user_map);

        assert_eq!(
            strategy.synonym_map.get("gentleman"),
            Some(&"MALE".to_string())
        );

        assert_eq!(strategy.synonym_map.get("f"), Some(&"FEMALE".to_string()));
        assert_eq!(strategy.synonym_map.get("m"), Some(&"MALE".to_string()));
        assert_eq!(
            strategy.synonym_map.len(),
            SexMappingStrategy::default_synonym_map().len() + 1
        );
    }

    #[rstest]
    fn test_new_constructor_with_empty_map() {
        let user_map: HashMap<String, Sex> = HashMap::new();

        let strategy = SexMappingStrategy::new(user_map);

        assert_eq!(
            strategy.synonym_map.len(),
            SexMappingStrategy::default_synonym_map().len()
        );
    }
}
