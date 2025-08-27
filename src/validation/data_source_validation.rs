use crate::config::table_context::TableContext;
use crate::validation::validation_utils::fail_validation_on_duplicates;
use std::collections::HashSet;
use validator::ValidationError;

pub(crate) fn validate_unique_sheet_names(sheets: &[TableContext]) -> Result<(), ValidationError> {
    let mut seen_names = HashSet::new();

    let duplicates: Vec<String> = sheets
        .iter()
        .filter_map(|s| {
            if !seen_names.insert(&s.name) {
                Some(s.name.clone())
            } else {
                None
            }
        })
        .collect();

    fail_validation_on_duplicates(duplicates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_validate_unique_sheet_names() {
        let table_context = vec![
            TableContext {
                name: "phenotypes".to_string(),
                context: vec![],
            },
            TableContext {
                name: "genotypes".to_string(),
                context: vec![],
            },
        ];
        let validation = validate_unique_sheet_names(&table_context);
        assert!(validation.is_ok());
    }

    #[rstest]
    fn test_validate_unique_sheet_names_error() {
        let table_context = vec![
            TableContext {
                name: "phenotypes".to_string(),
                context: vec![],
            },
            TableContext {
                name: "phenotypes".to_string(),
                context: vec![],
            },
        ];
        let validation = validate_unique_sheet_names(&table_context);
        assert!(validation.is_err());
    }
}
