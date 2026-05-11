use crate::config::context::Context;
use crate::config::table_context::{Identifier, SeriesContext, TableContext};
use crate::extract::enums::Filter;
use crate::validation::validation_utils::fail_validation_on_duplicates;
use std::borrow::Cow;
use std::collections::HashSet;
use validator::ValidationError;

pub(crate) fn validate_unique_identifiers(
    series_contexts: &[SeriesContext],
) -> Result<(), ValidationError> {
    let mut seen_single: HashSet<&str> = HashSet::new();
    let mut seen_regex: HashSet<&str> = HashSet::new();
    let mut duplicates: Vec<&str> = Vec::new();

    for sc in series_contexts {
        match sc.get_identifier() {
            Identifier::Single(s) => {
                if !seen_single.insert(s.as_str()) {
                    duplicates.push(s.as_str());
                }
            }
            Identifier::Multi(ids) => {
                for id in ids {
                    if !seen_single.insert(id.as_str()) {
                        duplicates.push(id.as_str());
                    }
                }
            }
            Identifier::Regex(r) => {
                if !seen_regex.insert(r.as_str()) {
                    duplicates.push(r.as_str());
                }
            }
        }
    }

    fail_validation_on_duplicates(
        &duplicates,
        "duplicates",
        "Found duplicate identifiers in SeriesContexts",
    )
}

pub(crate) fn validate_subject_ids_context(
    table_context: &TableContext,
) -> Result<(), ValidationError> {
    let is_valid = table_context
        .filter_series_context()
        .where_data_context(Filter::Is(&Context::SubjectId))
        .collect()
        .len()
        == 1;

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
    use crate::config::context::Context;
    use crate::config::table_context::{Identifier, SeriesContext, TableContext};

    use crate::config::traits::SeriesContextBuilding;
    use rstest::rstest;

    // TODO: Clean this up. These constructor functions are not needed

    fn single(id: &str) -> SeriesContext {
        SeriesContext::from_identifier(id)
    }

    fn multi_ids(ids: Vec<&str>) -> SeriesContext {
        let context = SeriesContext::default();
        context.with_identifier(Identifier::Multi(
            ids.iter().map(|id| id.to_string()).collect(),
        ))
    }

    fn regex(regex_str: &str) -> SeriesContext {
        SeriesContext::from_identifier(Identifier::regex_from_str(regex_str).unwrap())
    }

    #[rstest]
    #[case::empty_list(vec![], Ok(()))]
    #[case::single_name_ok(vec![single("a")], Ok(()))]
    #[case::multi_ids_ok(vec![multi_ids(vec!["a", "b"])], Ok(()))]
    #[case::regex_ok(vec![regex("a.*")], Ok(()))]
    #[case::multiple_unique_contexts(
        vec![
            single("name1"),
            multi_ids(vec!["id1", "id2"]),
            single("regex1")
        ],
        Ok(())
    )]
    #[case::duplicate_name(
        vec![single("dup"), single("dup")],
        Err("".to_string())
    )]
    #[case::duplicate_regex(
        vec![regex("dup"), regex("dup")],
        Err("".to_string())
    )]
    #[case::duplicate_in_multi_list(
        vec![multi_ids(vec!["a", "b"]), single("a")],
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
    fn test_validate_subject_ids_context() {
        let result = validate_subject_ids_context(&TableContext::new(
            "test".to_string(),
            vec![single("test").with_data_context(Context::SubjectId)],
        ));
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_validate_subject_ids_context_err() {
        let table_context = TableContext::new(
            "test".to_string(),
            vec![
                single("test").with_data_context(Context::SubjectId),
                single("test_2").with_data_context(Context::SubjectId),
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
                single("test").with_header_context(Context::Hpo),
                single("test").with_data_context(Context::None),
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
