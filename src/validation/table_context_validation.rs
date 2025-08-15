use crate::config::table_context::SeriesContext;
use crate::validation::validation_utils::fail_validation_on_duplicates;
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

#[cfg(test)]
mod tests {
    use super::validate_unique_identifiers;
    use crate::config::table_context::{
        Identifier, MultiIdentifier, MultiSeriesContext, SeriesContext, SingleSeriesContext,
    };

    use rstest::rstest;

    fn single_name(name: &str) -> SeriesContext {
        SeriesContext::Single(SingleSeriesContext::new(Identifier::Name(name.to_string())))
    }

    fn single_number(num: isize) -> SeriesContext {
        SeriesContext::Single(SingleSeriesContext::new(Identifier::Number(num)))
    }

    fn multi_ids(ids: Vec<&str>) -> SeriesContext {
        SeriesContext::Multi(MultiSeriesContext::new(MultiIdentifier::Multi(
            ids.into_iter().map(String::from).collect(),
        )))
    }

    fn multi_regex(regex: &str) -> SeriesContext {
        SeriesContext::Multi(MultiSeriesContext::new(MultiIdentifier::Regex(
            regex.to_string(),
        )))
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
}
