use crate::config::table_context::{Context, SeriesContext, TableContext};
use crate::validation::validation_utils::fail_validation_on_duplicates;
use std::borrow::Cow;
use std::collections::HashSet;
use validator::ValidationError;

pub(crate) fn validate_unique_identifiers(
    series_context: &[SeriesContext],
) -> Result<(), ValidationError> {
    let mut unique_identifiers: HashSet<String> = HashSet::new();

    let duplicates = series_context
        .iter()
        .filter_map(|context| match context {
            SeriesContext::Single(single) => match &single.identifier {
                crate::config::table_context::Identifier::Name(single_name) => {
                    if !unique_identifiers.insert(single_name.clone()) {
                        Some(single_name.clone())
                    } else {
                        None
                    }
                }
                crate::config::table_context::Identifier::Number(number) => {
                    if !unique_identifiers.insert(number.to_string()) {
                        Some(number.to_string())
                    } else {
                        None
                    }
                }
            },

            SeriesContext::Multi(multi) => match &multi.multi_identifier {
                crate::config::table_context::MultiIdentifier::Multi(multi_ids) => {
                    let duplicates = multi_ids
                        .iter()
                        .filter_map(|multi_ids| {
                            if !unique_identifiers.insert(multi_ids.clone()) {
                                Some(multi_ids.clone())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<String>>();

                    if !duplicates.is_empty() {
                        return Some(duplicates.join(","));
                    }
                    None
                }
                crate::config::table_context::MultiIdentifier::Regex(regex) => {
                    if !unique_identifiers.insert(regex.clone()) {
                        Some(regex.clone())
                    } else {
                        None
                    }
                }
            },
        })
        .collect::<Vec<String>>();
    fail_validation_on_duplicates(duplicates)
}

pub(crate) fn validate_at_least_one_subject_id(
    table_context: &TableContext,
) -> Result<(), ValidationError> {
    if let Some(columns) = &table_context.columns {
        for column in columns {
            if column.get_context() == Context::SubjectId
                || column.get_cell_context() == Context::SubjectId
            {
                return Ok(());
            }
        }
    }
    if let Some(rows) = &table_context.rows {
        for row in rows {
            if row.get_context() == Context::SubjectId
                || row.get_cell_context() == Context::SubjectId
            {
                return Ok(());
            }
        }
    }

    let mut error = ValidationError::new("missing_subject_id");
    error.add_param(Cow::from("table_name"), &table_context.name);
    Err(error.with_message(Cow::Owned(
        "Missing SubjectID on table. Every table needs to have at least one.".to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use super::{validate_at_least_one_subject_id, validate_unique_identifiers};
    use crate::config::table_context::{
        Context, Identifier, MultiIdentifier, MultiSeriesContext, SeriesContext,
        SingleSeriesContext, TableContext,
    };

    use rstest::rstest;

    fn single_name(name: &str) -> SeriesContext {
        SeriesContext::Single(SingleSeriesContext::new(
            Identifier::Name(name.to_string()),
            Context::None,
            None,
            None,
            None,
        ))
    }

    fn single_number(num: isize) -> SeriesContext {
        SeriesContext::Single(SingleSeriesContext::new(
            Identifier::Number(num),
            Context::None,
            None,
            None,
            None,
        ))
    }

    fn multi_ids(ids: Vec<&str>) -> SeriesContext {
        SeriesContext::Multi(MultiSeriesContext::new(
            MultiIdentifier::Multi(ids.into_iter().map(String::from).collect()),
            Context::None,
            None,
        ))
    }

    fn multi_regex(regex: &str) -> SeriesContext {
        SeriesContext::Multi(MultiSeriesContext::new(
            MultiIdentifier::Regex(regex.to_string()),
            Context::None,
            None,
        ))
    }

    #[rstest]
    #[case::empty_list(vec![], Ok(()))]
    #[case::single_name_ok(vec![single_name("a")], Ok(()))]
    #[case::single_number_ok(vec![single_number(1)], Ok(()))]
    #[case::multi_ids_ok(vec![multi_ids(vec!["a", "b"])], Ok(()))]
    #[case::multi_regex_ok(vec![multi_regex("a.*")], Ok(()))]
    #[case::multiple_unique_contexts(
        vec![
            single_name("name1"),
            single_number(123),
            multi_ids(vec!["id1", "id2"]),
            multi_regex("regex1")
        ],
        Ok(())
    )]
    #[case::duplicate_name(
        vec![single_name("dup"), single_name("dup")],
        Err("".to_string())
    )]
    #[case::duplicate_number(
        vec![single_number(123), single_number(123)],
        Err("".to_string())
    )]
    #[case::duplicate_regex(
        vec![multi_regex("dup"), multi_regex("dup")],
        Err("".to_string())
    )]
    #[case::duplicate_in_multi_list(
        vec![multi_ids(vec!["a", "b"]), single_name("a")],
        Err("".to_string())
    )]
    #[case::duplicate_between_number_and_name(
        vec![single_number(456), single_name("456")],
        Err("".to_string())
    )]
    #[case::internal_duplicate_in_multi(
        vec![multi_ids(vec!["a", "b", "a"])],
        Err("".to_string())
    )]
    #[case::multiple_duplicates(
        vec![single_name("a"), single_number(1), single_name("a"), single_number(1)],
        Err("".to_string())
    )]
    fn test_identifier_validation(
        #[case] series_contexts: Vec<SeriesContext>,
        #[case] expected: Result<(), String>,
    ) {
        let result = validate_unique_identifiers(&series_contexts);

        match (result, expected) {
            (Ok(_), Ok(_)) => {
                // Success case, do nothing
            }
            (Err(_), Err(_)) => {
                // Error case was correct.
            }
            _ => {
                panic!("Validation failed.");
            }
        }
    }

    #[rstest]
    #[case::subject_id_in_column_context(
        TableContext {
            name: "test".to_string(),
            columns: Some(vec![single_name("test").with_context(Context::SubjectId)]),
            rows: None,
            },
    )]
    #[case::subject_id_in_column_cell_context(
        TableContext {
            name: "test".to_string(),
            columns: Some(vec![single_name("test").with_cell_context(Context::SubjectId)]),
            rows: None,
            },
    )]
    #[case::subject_id_in_row_context(
        TableContext {
            name: "test".to_string(),
            columns: None,
            rows: Some(vec![single_name("test").with_context(Context::SubjectId)]),
            },
    )]
    #[case::subject_id_in_row_cell_context(
        TableContext {
            name: "test".to_string(),
            columns: None,
            rows: Some(vec![single_name("test").with_cell_context(Context::SubjectId)]),
            },
    )]
    fn test_validation_succeeds_when_subject_id_is_present(#[case] table_context: TableContext) {
        let result = validate_at_least_one_subject_id(&table_context);
        assert!(result.is_ok());
    }

    /// This test covers the failure scenario where columns and rows exist,
    /// but none of them are marked with the SubjectId context.
    #[rstest]
    fn test_validation_fails_when_subject_id_is_absent() {
        let table_context = TableContext {
            name: "table_without_subject_id".to_string(),
            columns: Some(vec![
                single_name("test").with_context(Context::HpoId),
                single_name("test").with_cell_context(Context::None),
            ]),
            rows: Some(vec![single_name("test").with_context(Context::HpoId)]),
        };

        let result = validate_at_least_one_subject_id(&table_context);
        assert!(result.is_err());
    }

    /// This test covers the edge case where the table context has no
    /// columns or rows defined at all.
    #[rstest]
    fn test_validation_fails_for_empty_table() {
        let table_context = TableContext {
            name: "empty_table".to_string(),
            columns: None,
            rows: None,
        };

        let result = validate_at_least_one_subject_id(&table_context);
        assert!(result.is_err());
    }

    /// This test covers the edge case where the column and row vectors are present but empty.
    #[rstest]
    fn test_validation_fails_for_table_with_empty_vectors() {
        let table_context = TableContext {
            name: "table_with_empty_vecs".to_string(),
            columns: Some(vec![]),
            rows: Some(vec![]),
        };

        let result = validate_at_least_one_subject_id(&table_context);
        assert!(result.is_err());
    }
}
