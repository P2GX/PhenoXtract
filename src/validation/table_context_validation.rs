use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
use crate::validation::validation_utils::fail_validation_on_duplicates;
use std::borrow::Cow;
use std::collections::HashSet;
use validator::ValidationError;

//todo NOTE: this does not check whether each column has a unique context assigned to it. But we need a regex search function on a CDF to do that.
// That validation function still needs to be written. When that is complete, it should replace this validation function.
pub(crate) fn validate_unique_identifiers(
    series_contexts: &[SeriesContext],
) -> Result<(), ValidationError> {
    let mut identifiers: Vec<String> = Vec::new();

    series_contexts
        .iter()
        .for_each(|sc| match sc.get_identifier() {
            Identifier::Regex(regex) => {
                identifiers.push(regex.to_string());
            }
            Identifier::Multi(multi_ids) => {
                multi_ids.iter().for_each(|id| {
                    identifiers.push(id.to_string());
                });
            }
        });

    let mut unique_identifiers: HashSet<String> = HashSet::new();
    let duplicates = identifiers
        .iter()
        .filter_map(|id| {
            if !unique_identifiers.insert(id.clone()) {
                Some(id.clone())
            } else {
                None
            }
        })
        .collect::<Vec<String>>();

    fail_validation_on_duplicates(duplicates)
}

pub(crate) fn validate_at_least_one_subject_id(
    table_context: &TableContext,
) -> Result<(), ValidationError> {
    for column in &table_context.context {
        if column.get_header_context() == &Context::SubjectId
            || column.get_data_context() == &Context::SubjectId
        {
            return Ok(());
        }
    }

    let mut error = ValidationError::new("missing_subject_id");
    error.add_param(Cow::from("table_name"), &table_context.name);
    Err(error.with_message(Cow::Owned(
        "Missing SubjectID on table. Every table needs to have at least one.".to_string(),
    )))
}

pub(crate) fn validate_series_linking(table_context: &TableContext) -> Result<(), ValidationError> {
    let all_ids: Vec<&Identifier> = table_context
        .context
        .iter()
        .map(|column| column.get_identifier())
        .collect();

    let all_linking_ids: Vec<Identifier> = table_context
        .context
        .iter()
        .flat_map(|column| column.get_links())
        .collect();

    for link_id in all_linking_ids {
        if !all_ids.contains(&&link_id) {
            let mut error = ValidationError::new("missing_link");
            error.add_param(Cow::from("linking_id"), &link_id);
            error.add_param(Cow::from("table_name"), &table_context.name);
            return Err(error.with_message(Cow::Owned(
                "Linking id does not link to any other series.".into(),
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        validate_at_least_one_subject_id, validate_series_linking, validate_unique_identifiers,
    };
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};

    use rstest::rstest;

    fn regex(regex: &str) -> SeriesContext {
        SeriesContext::new(
            Identifier::Regex(regex.to_string()),
            Context::None,
            Context::None,
            None,
            None,
            vec![],
        )
    }

    fn multi_ids(ids: Vec<&str>) -> SeriesContext {
        SeriesContext::new(
            Identifier::Multi(ids.iter().map(|id| id.to_string()).collect()),
            Context::None,
            Context::None,
            None,
            None,
            vec![],
        )
    }

    #[rstest]
    #[case::empty_list(vec![], Ok(()))]
    #[case::single_name_ok(vec![regex("a")], Ok(()))]
    #[case::multi_ids_ok(vec![multi_ids(vec!["a", "b"])], Ok(()))]
    #[case::regex_ok(vec![regex("a.*")], Ok(()))]
    #[case::multiple_unique_contexts(
        vec![
            regex("name1"),
            multi_ids(vec!["id1", "id2"]),
            regex("regex1")
        ],
        Ok(())
    )]
    #[case::duplicate_name(
        vec![regex("dup"), regex("dup")],
        Err("".to_string())
    )]
    #[case::duplicate_regex(
        vec![regex("dup"), regex("dup")],
        Err("".to_string())
    )]
    #[case::duplicate_in_multi_list(
        vec![multi_ids(vec!["a", "b"]), regex("a")],
        Err("".to_string())
    )]
    #[case::internal_duplicate_in_multi(
        vec![multi_ids(vec!["a", "b", "a"])],
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
        TableContext{
            name: "test".to_string(),
            context: vec![regex("test").with_header_context(Context::SubjectId)],
            },
    )]
    #[case::subject_id_in_column_cell_context(
        TableContext{
            name: "test".to_string(),
            context: vec![regex("test").with_data_context(Context::SubjectId)],
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
        let table_context = TableContext::new(
            "table_without_subject_id".to_string(),
            vec![
                regex("test").with_header_context(Context::HpoId),
                regex("test").with_data_context(Context::None),
            ],
        );

        let result = validate_at_least_one_subject_id(&table_context);
        assert!(result.is_err());
    }

    /// This test covers the edge case where the table context has no
    /// columns or rows defined at all.
    #[rstest]
    fn test_validation_fails_for_empty_table() {
        let table_context = TableContext::new("empty_table".to_string(), vec![]);

        let result = validate_at_least_one_subject_id(&table_context);
        assert!(result.is_err());
    }

    /// This test covers the edge case where the column and row vectors are present but empty.
    #[rstest]
    fn test_validation_fails_for_table_with_empty_vectors() {
        let table_context = TableContext::new("table_with_empty_vecs".to_string(), vec![]);

        let result = validate_at_least_one_subject_id(&table_context);
        assert!(result.is_err());
    }

    #[rstest]
    fn test_valid_linking() {
        let table_context = TableContext::new(
            "test_table".to_string(),
            vec![
                SeriesContext::new(
                    Identifier::Regex("A".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("B".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![Identifier::Regex("A".to_string())],
                ),
            ],
        );
        assert!(validate_series_linking(&table_context).is_ok());
    }

    /// Tests the primary failure case: a series tries to link to an ID that doesn't exist.
    /// Series "B" attempts to link to "non_existent_link", which is not defined anywhere.
    #[rstest]
    fn test_invalid_linking_missing_target() {
        let table_context = TableContext::new(
            "test_table".to_string(),
            vec![
                SeriesContext::new(
                    Identifier::Regex("A".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("B".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![Identifier::Regex("non_existent_link".to_string())],
                ),
            ],
        );

        let result = validate_series_linking(&table_context);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.code, "missing_link");
        assert_eq!(err.params.get("linking_id").unwrap(), "non_existent_link");
        assert_eq!(err.params.get("table_name").unwrap(), "test_table");
    }

    /// Tests that validation passes when there are no columns at all.
    #[rstest]
    fn test_no_columns() {
        let table_context = TableContext::new("test_table".to_string(), vec![]);

        assert!(validate_series_linking(&table_context).is_ok());
    }

    /// Tests that validation passes when the columns vector is empty.
    #[rstest]
    fn test_empty_columns() {
        let table_context = TableContext::new("test_table".to_string(), vec![]);
        assert!(validate_series_linking(&table_context).is_ok());
    }

    /// Tests that validation passes when columns exist but no linking is configured.
    #[rstest]
    fn test_no_links_defined() {
        let table_context = TableContext::new(
            "test_table".to_string(),
            vec![
                SeriesContext::new(
                    Identifier::Regex("A".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("B".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![],
                ),
            ],
        );
        assert!(validate_series_linking(&table_context).is_ok());
    }

    /// Tests that validation passes when there are other types of SeriesContext present.
    /// The function should correctly ignore them.
    #[rstest]
    fn test_with_other_series_types() {
        let table_context = TableContext::new(
            "test_table".to_string(),
            vec![
                SeriesContext::new(
                    Identifier::Regex("A".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Multi(vec!["test".to_string()]),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("B".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![Identifier::Regex("A".to_string())],
                ),
            ],
        );
        assert!(validate_series_linking(&table_context).is_ok());
    }

    /// Tests a more complex valid scenario with multiple links.
    /// C links to A and B. Both A and B have valid linking_ids.
    #[rstest]
    fn test_multiple_valid_links() {
        let table_context = TableContext::new(
            "test_table".to_string(),
            vec![
                SeriesContext::new(
                    Identifier::Regex("A".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("B".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("C".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![
                        Identifier::Regex("A".to_string()),
                        Identifier::Regex("B".to_string()),
                    ],
                ),
            ],
        );
        assert!(validate_series_linking(&table_context).is_ok());
    }

    /// Tests a scenario where one of multiple links is invalid.
    /// C links to A (valid) and "non_existent_link" (invalid).
    #[rstest]
    fn test_one_of_multiple_links_is_invalid() {
        let table_context = TableContext::new(
            "test_table".to_string(),
            vec![
                SeriesContext::new(
                    Identifier::Regex("A".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("B".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![],
                ),
                SeriesContext::new(
                    Identifier::Regex("C".to_string()),
                    Context::None,
                    Context::None,
                    None,
                    None,
                    vec![
                        Identifier::Regex("A".to_string()),
                        Identifier::Regex("non_existent_link".to_string()),
                    ],
                ),
            ],
        );

        let result = validate_series_linking(&table_context);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, "missing_link");
        assert_eq!(err.params.get("linking_id").unwrap(), "non_existent_link");
    }
}
