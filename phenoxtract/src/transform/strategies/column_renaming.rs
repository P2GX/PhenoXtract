use crate::config::table_context::{Identifier, SeriesContext};
use crate::extract::ContextualizedDataFrame;
use crate::transform::error::StrategyError;
use crate::transform::strategies::traits::Strategy;
use std::collections::HashMap;

/// Renames columns in one or more [`ContextualizedDataFrame`]s according to a provided mapping.
///
/// This strategy iterates over every `(old_name, new_name)` pair in the renaming map and applies
/// the rename to all supplied tables. The [`SeriesContext`] associated with the old column is
/// preserved in full (header context, data context, fill-missing rules, alias map, and building
/// block ID); only the [`Identifier`] is updated to reflect the new name.
///
/// # Fields
///
/// * `renaming` - A map from existing column names to their desired new names.
///
/// # Example
///
/// The table
///
/// ```csv
/// PatientId, conditions, dob
/// P001, HP:1234567, 1990-01-01
/// P002, Arachnodactyly, 1985-06-15
/// ```
///
/// with the renaming `{ "conditions" -> "phenotypes", "dob" -> "date_of_birth" }` becomes
///
/// ```csv
/// PatientId, phenotypes, date_of_birth
/// P001, HP:1234567, 1990-01-01
/// P002, Arachnodactyly, 1985-06-15
/// ```
#[derive(Debug)]
pub struct ColumnRenamingStrategy {
    renaming: HashMap<String, String>,
}

impl ColumnRenamingStrategy {
    pub fn new(renaming: HashMap<String, String>) -> Self {
        ColumnRenamingStrategy { renaming }
    }
}

impl Strategy for ColumnRenamingStrategy {
    fn is_valid(&self, _: &[&mut ContextualizedDataFrame]) -> bool {
        !self.renaming.is_empty()
    }

    fn internal_transform(
        &self,
        tables: &mut [&mut ContextualizedDataFrame],
    ) -> Result<(), StrategyError> {
        for (old, new) in self.renaming.iter() {
            for table in tables.iter_mut() {
                let (new_sc, old_id) = if let Some(old_sc) = table.get_sc_by_col_name(old) {
                    (
                        SeriesContext::new(
                            Identifier::Single(new.to_string()),
                            old_sc.get_header_context().clone(),
                            old_sc.get_data_context().clone(),
                            old_sc.get_fill_missing().cloned(),
                            old_sc.get_alias_map().cloned(),
                            old_sc.get_building_block_id().map(|id| id.to_string()),
                        ),
                        old_sc.get_identifier().clone(),
                    )
                } else {
                    continue;
                };

                let mut builder = table.builder().rename_col(old, new)?.insert_sc(new_sc)?;

                if builder.get_inner().get_dangling_scs().contains(&old_id) {
                    builder = builder.drop_sc(&old_id);
                }

                builder.build()?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::traits::SeriesContextBuilding;
    use crate::test_suite::cdf_generation::generate_minimal_cdf;
    use crate::transform::error::StrategyError;
    use crate::transform::strategies::traits::Strategy;
    use polars::prelude::Column;
    use rstest::rstest;
    use std::collections::HashMap;

    #[rstest]
    fn test_rename_single_column() {
        let mut cdf = generate_minimal_cdf(1, 3);

        // Discover whatever column name generate_minimal_cdf gave us
        let original_name = cdf.data().get_column_names()[0].to_string();
        let new_name = format!("{original_name}_renamed");

        let strategy =
            ColumnRenamingStrategy::new(HashMap::from([(original_name.clone(), new_name.clone())]));

        strategy.transform(&mut [&mut cdf]).unwrap();

        let result_names: Vec<&str> = cdf
            .data()
            .get_column_names()
            .iter()
            .map(|col_name| col_name.as_str())
            .collect();

        assert!(
            result_names.contains(&new_name.as_str()),
            "Expected renamed column '{new_name}' to exist"
        );
        assert!(
            !result_names.contains(&original_name.as_str()),
            "Old column '{original_name}' should no longer exist"
        );
    }

    #[rstest]
    fn test_rename_multiple_columns() {
        let mut cdf = generate_minimal_cdf(1, 1);

        let mut cdf = cdf
            .builder()
            .insert_scs_alongside_cols(&[
                (
                    SeriesContext::from_identifier("Some_col_1"),
                    vec![Column::new("Some_col_1".into(), vec!["Some_val"])],
                ),
                (
                    SeriesContext::from_identifier("Some_col_2"),
                    vec![Column::new("Some_col_2".into(), vec!["Some_val"])],
                ),
            ])
            .unwrap()
            .build()
            .unwrap();

        let col_names: Vec<String> = cdf
            .data()
            .get_column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        let (old_a, old_b) = (col_names[1].clone(), col_names[2].clone());
        let (new_a, new_b) = ("alpha".to_string(), "beta".to_string());

        let strategy = ColumnRenamingStrategy::new(HashMap::from([
            (old_a.clone(), new_a.clone()),
            (old_b.clone(), new_b.clone()),
        ]));

        strategy.transform(&mut [&mut cdf]).unwrap();

        let result_names: Vec<&str> = cdf
            .data()
            .get_column_names()
            .iter()
            .map(|col_name| col_name.as_str())
            .collect();

        assert!(result_names.contains(&"alpha"), "Expected column 'alpha'");
        assert!(result_names.contains(&"beta"), "Expected column 'beta'");
        assert!(
            !result_names.contains(&old_a.as_str()),
            "Old column '{old_a}' should be gone"
        );
        assert!(
            !result_names.contains(&old_b.as_str()),
            "Old column '{old_b}' should be gone"
        );
    }

    #[rstest]
    fn test_rename_preserves_series_context() {
        let mut cdf = generate_minimal_cdf(1, 3);

        let original_name = cdf.data().get_column_names()[0].to_string();
        let new_name = format!("{original_name}_renamed");

        let original_sc = cdf.get_sc_by_col_name(&original_name).unwrap().clone();
        let expected_data_ctx = original_sc.get_data_context().clone();
        let expected_header_ctx = original_sc.get_header_context().clone();

        let strategy =
            ColumnRenamingStrategy::new(HashMap::from([(original_name.clone(), new_name.clone())]));

        strategy.transform(&mut [&mut cdf]).unwrap();

        let renamed_sc = cdf
            .get_sc_by_col_name(&new_name)
            .expect("SeriesContext for renamed column should exist");

        assert_eq!(
            renamed_sc.get_data_context(),
            &expected_data_ctx,
            "Data context should be preserved after rename"
        );
        assert_eq!(
            renamed_sc.get_header_context(),
            &expected_header_ctx,
            "Header context should be preserved after rename"
        );
    }

    #[rstest]
    fn test_empty_renaming_map_is_noop() {
        let mut cdf = generate_minimal_cdf(1, 3);

        let before: Vec<String> = cdf
            .data()
            .get_column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        let strategy = ColumnRenamingStrategy::new(HashMap::new());
        strategy.transform(&mut [&mut cdf]).unwrap();

        let after: Vec<String> = cdf
            .data()
            .get_column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        assert_eq!(
            before, after,
            "Column names should be unchanged for empty map"
        );
    }

    #[rstest]
    fn test_rename_duplicate_name() {
        let mut cdf = generate_minimal_cdf(1, 3);

        let original_name = cdf.data().get_column_names()[0].to_string();
        let new_name = "subject_id";

        let strategy = ColumnRenamingStrategy::new(HashMap::from([(
            original_name.clone(),
            new_name.to_string(),
        )]));

        match strategy.transform(&mut [&mut cdf]) {
            Ok(_) => {
                panic!("Should have failed, because of duplicate column names")
            }
            Err(err) => {
                matches!(err, StrategyError::ValidationError(_))
            }
        };
    }
}
