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
}
