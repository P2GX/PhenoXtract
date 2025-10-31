use crate::config::table_context::Identifier;
use crate::validation::error::ValidationError as PxValidationError;
use ordermap::OrderSet;
use regex::Regex;
use std::ops::Sub;

/// Validates that all columns are matched by the SeriesContext identifier.
///
/// Returns an error if any columns remain unmatched (orphaned).
pub(crate) fn check_orphaned_columns(
    col_names: &[&str],
    sc_identifier: &Identifier,
) -> Result<(), PxValidationError> {
    let orphaned_cols: Vec<&str> = match sc_identifier {
        Identifier::Regex(regex) => {
            let regex_obj = Regex::new(regex)
                .unwrap_or_else(|_| panic!("Regex should be valid. Invalid regex: '{}'", regex));
            col_names
                .iter()
                .filter(|&&col_name| !regex_obj.is_match(col_name))
                .copied()
                .collect()
        }
        Identifier::Multi(multi) => {
            let ids: OrderSet<&str> = multi.iter().map(|s| s.as_str()).collect();
            let unique_col_names: OrderSet<&str> = col_names.iter().copied().collect();
            unique_col_names.sub(&ids).iter().copied().collect()
        }
    };

    if !orphaned_cols.is_empty() {
        return Err(PxValidationError::OrphanedColumns {
            col_names: orphaned_cols.iter().map(|s| s.to_string()).collect(),
            when: format!("inserting SeriesContext with id '{}'.", sc_identifier,).to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_check_orphaned_columns_regex_all_matched() {
        let cols = vec!["col_1", "col_2", "col_3"];
        let identifier = Identifier::Regex("col_.*".to_string());

        assert!(check_orphaned_columns(&cols, &identifier).is_ok());
    }

    #[rstest]
    fn test_check_orphaned_columns_regex_some_orphaned() {
        let cols = vec!["col_1", "col_2", "other"];
        let identifier = Identifier::Regex("col_.*".to_string());

        let result = check_orphaned_columns(&cols, &identifier);
        assert!(result.is_err());

        if let Err(PxValidationError::OrphanedColumns { col_names, .. }) = result {
            assert_eq!(col_names, vec!["other"]);
        }
    }

    #[rstest]
    fn test_check_orphaned_columns_multi_all_matched() {
        let cols = vec!["a", "b", "c"];
        let identifier = Identifier::Multi(vec!["a".into(), "b".into(), "c".into()]);

        assert!(check_orphaned_columns(&cols, &identifier).is_ok());
    }

    #[rstest]
    fn test_check_orphaned_columns_multi_some_orphaned() {
        let cols = vec!["a", "b", "d"];
        let identifier = Identifier::Multi(vec!["a".into(), "b".into()]);

        let result = check_orphaned_columns(&cols, &identifier);
        assert!(result.is_err());

        if let Err(PxValidationError::OrphanedColumns { col_names, .. }) = result {
            assert_eq!(col_names, vec!["d"]);
        }
    }

    #[rstest]
    fn test_check_orphaned_columns_empty() {
        let cols: Vec<&str> = vec![];
        let identifier = Identifier::Regex(".*".to_string());

        assert!(check_orphaned_columns(&cols, &identifier).is_ok());
    }
}
