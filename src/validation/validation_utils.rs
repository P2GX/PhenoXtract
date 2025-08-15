use std::borrow::Cow;
use validator::ValidationError;

pub(crate) fn fail_validation_on_duplicates(
    duplicates: Vec<String>,
) -> Result<(), ValidationError> {
    if duplicates.is_empty() {
        Ok(())
    } else {
        let mut error = ValidationError::new("unique");
        error.add_param(Cow::from("duplicates"), &duplicates);
        Err(error.with_message(Cow::Owned("Duplicate sheet name configured.".to_string())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_fail_on_duplicates_fail() {
        let duplicates = vec!["A".to_string(), "A".to_string()];
        let res = fail_validation_on_duplicates(duplicates);
        assert!(res.is_err());
    }

    #[rstest]
    fn test_fail_on_duplicates_pass() {
        let duplicates = vec![];
        let res = fail_validation_on_duplicates(duplicates);
        assert!(res.is_ok());
    }
}
