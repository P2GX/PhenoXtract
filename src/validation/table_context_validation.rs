use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};
use crate::extract::contextualized_dataframe_filters::Filter;
use crate::validation::validation_utils::fail_validation_on_duplicates;
use std::borrow::Cow;
use std::collections::HashSet;
use validator::ValidationError;

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

    fail_validation_on_duplicates(
        &duplicates,
        "duplicates",
        "Found duplicate identifiers in SeriesContexts",
    )
}

pub(crate) fn validate_subject_ids_context(
    table_context: &TableContext,
) -> Result<(), ValidationError> {
    let is_valid = (!table_context
        .filter_series_context()
        .where_header_context(Filter::Is(&Context::SubjectId))
        .collect()
        .is_empty())
        ^ (table_context
            .filter_series_context()
            .where_data_context(Filter::Is(&Context::SubjectId))
            .collect()
            .len()
            == 1);

    if is_valid {
        Ok(())
    } else {
        let mut error = ValidationError::new("missing_subject_id");
        error.add_param(Cow::from("table_name"), &table_context.name());
        Err(error.with_message(Cow::Owned(
            "SubjectID columns have unexpected configuration. If SubjectIDs are in the headers then at least one Series Context with header_context = SubjectID needs to be provided. If SubjectIDs are in the cells, then exactly one SeriesContext must be provided.".to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_subject_ids_context, validate_unique_identifiers};
    use crate::config::table_context::{Context, Identifier, SeriesContext, TableContext};

    use rstest::rstest;

    fn regex(regex: &str) -> SeriesContext {
        let context = SeriesContext::default();
        context.with_identifier(Identifier::Regex(regex.to_string()))
    }

    fn multi_ids(ids: Vec<&str>) -> SeriesContext {
        let context = SeriesContext::default();
        context.with_identifier(Identifier::Multi(
            ids.iter().map(|id| id.to_string()).collect(),
        ))
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
        TableContext::new(
      "test".to_string(),
      vec![regex("test").with_header_context(Context::SubjectId)],
    ),
    )]
    #[case::subject_id_in_column_cell_context(
        TableContext::new(
          "test".to_string(),
          vec![regex("test").with_data_context(Context::SubjectId)],
        ),
    )]
    fn test_validate_subject_ids_context(#[case] table_context: TableContext) {
        let result = validate_subject_ids_context(&table_context);
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_validate_subject_ids_context_err() {
        let table_context = TableContext::new(
            "test".to_string(),
            vec![
                regex("test").with_data_context(Context::SubjectId),
                regex("test_2").with_data_context(Context::SubjectId),
            ],
        );
        let result = validate_subject_ids_context(&table_context);
        assert!(result.is_err());
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

        let result = validate_subject_ids_context(&table_context);
        assert!(result.is_err());
    }

    /// This test covers the edge case where the table context has no
    /// columns or rows defined at all.
    #[rstest]
    fn test_validation_fails_for_empty_table() {
        let table_context = TableContext::new("empty_table".to_string(), vec![]);

        let result = validate_subject_ids_context(&table_context);
        assert!(result.is_err());
    }

    /// This test covers the edge case where the column and row vectors are present but empty.
    #[rstest]
    fn test_validation_fails_for_table_with_empty_vectors() {
        let table_context = TableContext::new("table_with_empty_vecs".to_string(), vec![]);

        let result = validate_subject_ids_context(&table_context);
        assert!(result.is_err());
    }
}
