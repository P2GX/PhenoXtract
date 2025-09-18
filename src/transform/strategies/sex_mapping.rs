use crate::config::table_context::Context;
use crate::extract::contextualized_data_frame::ContextualizedDataFrame;
use crate::transform::error::TransformError;
use crate::transform::traits::Strategy;
use std::any::type_name;

use log::{debug, warn};
use phenopackets::schema::v2::core::Sex;
use polars::prelude::AnyValue;
use std::collections::HashMap;

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
/// - If no mapping is found, the value is replaced with `Null`, and a warning is logged.
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
///     "gender" => &["m", "female", "MAN", "other"],
/// }.unwrap();
///
/// // And a context mapping the "gender" column to SubjectSex
/// let mut table_context = TableContext::new("patients".to_string());
/// table_context.add_context("gender", Context::SubjectSex);
/// let mut cdf = ContextualizedDataFrame::new(df, table_context);
///
/// // Create and apply the strategy
/// let strategy = SexMappingStrategy::default();
/// strategy.transform(&mut cdf).unwrap();
///
/// // The "gender" column is now standardized
/// let expected_df = df! {
///     "patient_id" => &[1, 2, 3, 4],
///     "gender" => &[Some("MALE"), Some("FEMALE"), Some("MALE"), None],
/// }.unwrap();
///
/// assert_eq!(cdf.data, expected_df);
/// ```
struct SexMappingStrategy {
    map: HashMap<String, String>,
}

impl SexMappingStrategy {
    #[allow(dead_code)]
    pub fn new(map: HashMap<String, String>) -> Self {
        Self { map }
    }
    #[allow(dead_code)]
    pub fn default() -> Self {
        let map = HashMap::from([
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
        ]);
        SexMappingStrategy::new(map)
    }
}

impl Strategy for SexMappingStrategy {
    fn is_valid(&self, _table: &ContextualizedDataFrame) -> bool {
        true
    }

    fn internal_transform(
        &self,
        table: &mut ContextualizedDataFrame,
    ) -> Result<(), TransformError> {
        let column_names: Vec<String> = table
            .get_columns_with_data_context(Context::SubjectSex)
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        for col_name in column_names {
            let col_values: Vec<String> = table
                .data
                .column(&col_name)
                .map_err(TransformError::PolarsError)?
                .str()
                .map_err(TransformError::PolarsError)?
                .into_iter()
                .flatten()
                .map(|s| s.to_string())
                .collect();

            let mapped_column: Result<Vec<AnyValue>, TransformError> = col_values
                .iter()
                .map(|s| match self.map.get(s.to_lowercase().trim()) {
                    Some(alias) => {
                        debug!("Converted {s} to {alias}");
                        Ok(AnyValue::String(alias))
                    }
                    None => {
                        warn!("Unable to convert sex '{s}'");
                        Err(TransformError::MappingError {
                            strategy_name: type_name::<Self>()
                                .split("::")
                                .last()
                                .unwrap()
                                .to_string(),
                            old_value: s.to_string(),
                            possible_mappings: self.map.clone(),
                        })
                    }
                })
                .collect();
            table.replace_column(mapped_column?, col_name.as_str())?;
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

    fn make_test_dataframe() -> ContextualizedDataFrame {
        let df = df![
            "sex" => &["m", "f", "male", "female", "man", "woman", "diverse", "unknown"]
        ]
        .unwrap();

        let mut tc = TableContext::default();
        tc.context.push(SeriesContext::new(
            Identifier::Regex("sex".to_string()),
            Default::default(),
            Context::SubjectSex,
            None,
            None,
            vec![],
        ));
        ContextualizedDataFrame::new(tc, df)
    }

    #[test]
    fn test_sex_mapping_strategy_default() {
        let mut table = make_test_dataframe();
        let strategy = SexMappingStrategy::default();

        let err = strategy.transform(&mut table);

        match err {
            Err(TransformError::MappingError {
                strategy_name,
                old_value,
                    possible_mappings: possibles_mappings,
            }) => {
                assert_eq!(strategy_name, "SexMappingStrategy");
                assert_eq!(old_value, "unknown");
                assert_eq!(possibles_mappings, strategy.map);
            }
            _ => panic!("Unexpected error"),
        }
    }
}
