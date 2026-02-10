use crate::config::table_context::TableContext;
use crate::extract::excel_data_source::ExcelDataSource;
use crate::extract::extraction_config::ExtractionConfig;
use crate::validation::validation_utils::fail_validation_on_duplicates;
use std::borrow::Cow;
use std::collections::HashSet;
use validator::ValidationError;

pub(crate) fn validate_unique_sheet_names(sheets: &[TableContext]) -> Result<(), ValidationError> {
    let mut seen_names = HashSet::new();

    let duplicates: Vec<String> = sheets
        .iter()
        .filter_map(|s| {
            if !seen_names.insert(s.name()) {
                Some(s.name().to_string())
            } else {
                None
            }
        })
        .collect();

    fail_validation_on_duplicates(
        &duplicates,
        "duplicates",
        "Found duplicate sheet names in TableContext",
    )
}

pub(crate) fn validate_extraction_config_unique_ids(
    extract_config: &[ExtractionConfig],
) -> Result<(), ValidationError> {
    let mut seen_names = HashSet::new();

    let duplicates: Vec<String> = extract_config
        .iter()
        .filter_map(|s| {
            if !seen_names.insert(&s.name) {
                Some(s.name.clone())
            } else {
                None
            }
        })
        .collect();

    fail_validation_on_duplicates(
        &duplicates,
        "duplicates",
        "Found duplicate extraction config names",
    )
}

pub(crate) fn validate_extraction_config_links(
    source: &ExcelDataSource,
) -> Result<(), ValidationError> {
    let extraction_ids: HashSet<&str> = source
        .extraction_configs
        .iter()
        .map(|s| s.name.as_str())
        .collect();
    let table_ids: HashSet<&str> = source.contexts.iter().map(|t| t.name()).collect();

    if extraction_ids.len() < table_ids.len() {
        let missing: Vec<&str> = table_ids.difference(&extraction_ids).cloned().collect();
        let mut error = ValidationError::new("linking");
        error.add_param(Cow::from("missing"), &missing);
        return Err(error.with_message(Cow::Owned(
            "More TableContext than ExtractionConfigs".to_string(),
        )));
    }
    if extraction_ids.len() > table_ids.len() {
        let missing: Vec<&str> = extraction_ids.difference(&table_ids).cloned().collect();
        let mut error = ValidationError::new("linking");
        error.add_param(Cow::from("missing"), &missing);
        return Err(error.with_message(Cow::Owned(
            "More ExtractionConfigs than TableContext".to_string(),
        )));
    }
    if extraction_ids != table_ids {
        let mut error = ValidationError::new("linking");
        error.add_param(Cow::from("mismatch"), &(extraction_ids, table_ids));
        return Err(error.with_message(Cow::Owned(
            "Extraction Config and Table names are not matching.".to_string(),
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::Value;

    #[rstest]
    fn test_validate_unique_sheet_names() {
        let table_context = vec![
            TableContext::new("phenotypes".to_string(), vec![]),
            TableContext::new("genotypes".to_string(), vec![]),
        ];
        let validation = validate_unique_sheet_names(&table_context);
        assert!(validation.is_ok());
    }

    #[rstest]
    fn test_validate_unique_sheet_names_error() {
        let table_context = vec![
            TableContext::new("phenotypes".to_string(), vec![]),
            TableContext::new("phenotypes".to_string(), vec![]),
        ];
        let validation = validate_unique_sheet_names(&table_context);
        assert!(validation.is_err());
    }

    fn mock_extraction_config(name: &str) -> ExtractionConfig {
        ExtractionConfig {
            name: name.to_string(),
            has_headers: false,
            patients_are_rows: false,
        }
    }

    fn mock_table_config(name: &str) -> TableContext {
        TableContext::new(name.to_string(), vec![])
    }

    #[rstest]
    fn test_unique_ids_success_with_no_duplicates() {
        let configs = vec![
            mock_extraction_config("ConfigA"),
            mock_extraction_config("ConfigB"),
            mock_extraction_config("ConfigC"),
        ];
        let result = validate_extraction_config_unique_ids(&configs);
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_unique_ids_success_on_empty_list() {
        let configs = vec![];
        let result = validate_extraction_config_unique_ids(&configs);
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_unique_ids_failure_with_one_duplicate() {
        let configs = vec![
            mock_extraction_config("ConfigA"),
            mock_extraction_config("ConfigB"),
            mock_extraction_config("ConfigA"), // Duplicate
        ];
        let result = validate_extraction_config_unique_ids(&configs);
        assert!(result.is_err());
        let error = result.unwrap_err();

        assert_eq!(error.code.to_string(), "duplicates");
    }

    #[rstest]
    fn test_unique_ids_failure_with_multiple_duplicates() {
        let configs = vec![
            mock_extraction_config("ConfigA"),
            mock_extraction_config("ConfigB"),
            mock_extraction_config("ConfigA"), // Duplicate 1
            mock_extraction_config("ConfigC"),
            mock_extraction_config("ConfigB"), // Duplicate 2
        ];
        let result = validate_extraction_config_unique_ids(&configs);
        assert!(result.is_err());
        let error = result.unwrap_err();

        let duplicates_str = error.params.get("duplicates").unwrap();

        if let Value::Array(arr) = duplicates_str {
            let reconstructed_duplicates: Result<Vec<&String>, String> = arr
                .iter()
                .map(|val| {
                    if let Value::String(s) = val {
                        Ok(s)
                    } else {
                        Err("Array element is not a string.".to_string())
                    }
                })
                .collect();
            let reconstructed_duplicates = reconstructed_duplicates.unwrap();

            assert_eq!(
                reconstructed_duplicates.first().unwrap().to_string(),
                "ConfigA".to_string()
            );
            assert_eq!(
                reconstructed_duplicates.last().unwrap().to_string(),
                "ConfigB".to_string()
            );
        }
    }

    #[rstest]
    fn test_links_success_with_matching_names() {
        let source = ExcelDataSource {
            source: Default::default(),
            extraction_configs: vec![
                mock_extraction_config("Sheet1"),
                mock_extraction_config("Sheet2"),
            ],
            contexts: vec![mock_table_config("Sheet1"), mock_table_config("Sheet2")],
        };
        let result = validate_extraction_config_links(&source);
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_links_success_with_empty_configs() {
        let source = ExcelDataSource {
            source: Default::default(),
            extraction_configs: vec![],
            contexts: vec![],
        };
        let result = validate_extraction_config_links(&source);
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_links_failure_missing_extraction_config() {
        let source = ExcelDataSource {
            source: Default::default(),
            extraction_configs: vec![mock_extraction_config("Sheet1")],
            contexts: vec![mock_table_config("Sheet1"), mock_table_config("Sheet2")],
        };
        let result = validate_extraction_config_links(&source);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.code, "linking");

        let mut vec = error
            .params
            .get("missing")
            .unwrap()
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(vec.pop().unwrap().as_str().unwrap(), "Sheet2");
    }

    #[rstest]
    fn test_links_failure_missing_table_config() {
        let source = ExcelDataSource {
            source: Default::default(),
            extraction_configs: vec![
                mock_extraction_config("Sheet1"),
                mock_extraction_config("Sheet2"),
            ],
            contexts: vec![mock_table_config("Sheet1")],
        };
        let result = validate_extraction_config_links(&source);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, "linking");

        let mut vec = error
            .params
            .get("missing")
            .unwrap()
            .as_array()
            .unwrap()
            .clone();

        assert_eq!(vec.pop().unwrap().as_str().unwrap(), "Sheet2");
    }

    #[rstest]
    fn test_links_failure_mismatched_names_same_count() {
        let source = ExcelDataSource {
            source: Default::default(),
            extraction_configs: vec![
                mock_extraction_config("SheetA"),
                mock_extraction_config("SheetB"),
            ],
            contexts: vec![mock_table_config("SheetA"), mock_table_config("SheetC")], // B vs C
        };
        let result = validate_extraction_config_links(&source);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code, "linking");
        assert_eq!(
            error.message.unwrap(),
            "Extraction Config and Table names are not matching."
        );
    }
}
