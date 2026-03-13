use crate::config::table_context::Identifier;
use std::borrow::Cow;
use validator::ValidationError;

/// Validates that all columns are matched by the SeriesContext identifier.
///
/// Returns an error if any columns remain unmatched (orphaned).
pub(crate) fn check_orphaned_columns(
    col_names: &[&str],
    sc_identifier: &Identifier,
) -> Result<(), ValidationError> {
    let matched_cols = sc_identifier.identify(col_names);
    let orphaned_cols: Vec<&&str> = col_names
        .iter()
        .filter(|item| !matched_cols.contains(item))
        .collect();

    if !orphaned_cols.is_empty() {
        let mut err = ValidationError::new("orphaned_columns");
        err.add_param(Cow::from("identifier"), &sc_identifier);
        err.add_param(Cow::from("col_names"), &col_names);
        err.add_param(Cow::from("orphaned_col_names"), &orphaned_cols);
        let error_message = "Not all columns were matched by the SeriesContext identifier.";

        return Err(err.with_message(Cow::Borrowed(error_message)));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::from_value;

    #[rstest]
    fn test_check_orphaned_columns_regex_all_matched() {
        let cols = vec!["col_1", "col_2", "col_3"];
        let identifier = Identifier::from("col_.*");

        assert!(check_orphaned_columns(cols.as_slice(), &identifier).is_ok());
    }

    #[rstest]
    fn test_check_orphaned_columns_regex_some_orphaned() {
        let cols = vec!["col_1", "col_2", "other"];
        let identifier = Identifier::from("col_.*");

        let result = check_orphaned_columns(cols.as_slice(), &identifier);
        assert!(result.is_err());

        if let Err(err) = result {
            let orphaned_col_names: Vec<String> =
                from_value(err.params.get("orphaned_col_names").unwrap().clone()).unwrap();
            assert_eq!(orphaned_col_names, vec!["other".to_string()]);
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

        if let Err(err) = result {
            let orphaned_col_names: Vec<String> =
                from_value(err.params.get("orphaned_col_names").unwrap().clone()).unwrap();
            assert_eq!(orphaned_col_names, vec!["d".to_string()]);
        }
    }

    #[rstest]
    fn test_check_orphaned_columns_empty() {
        let cols: Vec<&str> = vec![];
        let identifier = Identifier::from(".*");

        assert!(check_orphaned_columns(cols.as_slice(), &identifier).is_ok());
    }
}
