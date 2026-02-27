use crate::config::table_context::Identifier;
use crate::validation::error::ValidationError as PxValidationError;

/// Validates that all columns are matched by the SeriesContext identifier.
///
/// Returns an error if any columns remain unmatched (orphaned).
pub(crate) fn check_orphaned_columns(
    col_names: &[&str],
    sc_identifier: &Identifier,
) -> Result<(), PxValidationError> {
    let matched_cols = sc_identifier.identify(col_names);
    let orphaned_cols: Vec<&&str> = col_names
        .into_iter()
        .filter(|item| !matched_cols.contains(item))
        .collect();

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

        assert!(check_orphaned_columns(cols.as_slice(), &identifier).is_ok());
    }

    #[rstest]
    fn test_check_orphaned_columns_regex_some_orphaned() {
        let cols = vec!["col_1", "col_2", "other"];
        let identifier = Identifier::Regex("col_.*".to_string());

        let result = check_orphaned_columns(cols.as_slice(), &identifier);
        assert!(result.is_err());

        if let Err(PxValidationError::OrphanedColumns { col_names, .. }) = result {
            assert_eq!(col_names, vec!["other"]);
        }
    }

    #[rstest]
    fn test_check_orphaned_columns_multi_all_matched() {
        let cols = vec!["a", "b", "c"];
        let identifier = Identifier::Multi(vec!["a".into(), "b".into(), "c".into()]);

        assert!(check_orphaned_columns(cols.as_slice(), &identifier).is_ok());
    }

    #[rstest]
    fn test_check_orphaned_columns_multi_some_orphaned() {
        let cols = vec!["a", "b", "d"];
        let identifier = Identifier::Multi(vec!["a".into(), "b".into()]);

        let result = check_orphaned_columns(cols.as_slice(), &identifier);
        assert!(result.is_err());

        if let Err(PxValidationError::OrphanedColumns { col_names, .. }) = result {
            assert_eq!(col_names, vec!["d"]);
        }
    }

    #[rstest]
    fn test_check_orphaned_columns_empty() {
        let cols: Vec<&str> = vec![];
        let identifier = Identifier::Regex(".*".to_string());

        assert!(check_orphaned_columns(cols.as_slice(), &identifier).is_ok());
    }
}
