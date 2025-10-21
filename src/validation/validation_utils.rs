use regex::Regex;
use serde::Serialize;
use std::borrow::Cow;
use validator::ValidationError;

pub(crate) fn fail_validation_on_duplicates<T: Serialize>(
    duplicates: &[T],
    error_id: &'static str,
    reason: &str,
) -> Result<(), ValidationError> {
    if duplicates.is_empty() {
        Ok(())
    } else {
        let mut error = ValidationError::new(error_id);
        error.add_param(Cow::from("duplicates"), &duplicates);
        Err(error.with_message(Cow::Owned(reason.to_string())))
    }
}

pub(crate) fn validate_regex(regex: &str) -> Result<(), ValidationError> {
    let regex_validation = Regex::new(regex);
    match regex_validation {
        Ok(_) => Ok(()),
        Err(_) => {
            let mut error = ValidationError::new("invalid_regex");
            error.add_param(Cow::from("regex"), &regex);
            Err(error.with_message(Cow::Owned("Invalid Regex string.".to_string())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_fail_on_duplicates_fail() {
        let duplicates = vec!["A".to_string(), "A".to_string()];
        let res =
            fail_validation_on_duplicates(&duplicates, "duplicate_ids", "Too many duplicates");
        assert!(res.is_err());
    }

    #[rstest]
    fn test_fail_on_duplicates_pass() {
        let duplicates: Vec<String> = vec![];
        let res =
            fail_validation_on_duplicates(&duplicates, "duplicate_ids", "Too many duplicates");
        assert!(res.is_ok());
    }

    #[rstest]
    fn test_validate_regex() {
        let regex = "^[a-zA-Z0-9]*$";
        let res = validate_regex(regex);
        assert!(res.is_ok());
    }
    #[rstest]
    fn validate_regex_err() {
        let regex = "^[a-zA-Z0-9*$";
        let res = validate_regex(regex);
        assert!(res.is_err());
    }
}
