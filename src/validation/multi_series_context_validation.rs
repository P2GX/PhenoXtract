use crate::config::table_context::MultiIdentifier;
use crate::validation::validation_utils::validate_regex;
use std::borrow::Cow;
use validator::ValidationError;

pub(crate) fn validate_multi_identifier(
    identifier: &MultiIdentifier,
) -> Result<(), ValidationError> {
    match identifier {
        MultiIdentifier::Regex(r) => validate_regex(r),
        MultiIdentifier::Multi(vec) => {
            if vec.is_empty() {
                let mut error = ValidationError::new("invalid_multi_identifier");
                error.add_param(Cow::from("identifier"), vec);
                return Err(error.with_message(Cow::Owned(
                    "Multi identifier needs to have at least one ID.".to_string(),
                )));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_identifier_regex_delegates() {
        let id = MultiIdentifier::Regex("^[a-z]+$".to_string());
        let result = validate_multi_identifier(&id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_multi_identifier_multi_empty_vec_errors() {
        let id = MultiIdentifier::Multi(vec![]);
        let result = validate_multi_identifier(&id);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, "invalid_multi_identifier");
        assert!(err.message.unwrap().contains("at least one ID"));
    }

    #[test]
    fn test_multi_identifier_multi_non_empty_vec_ok() {
        let id = MultiIdentifier::Multi(vec!["abc".to_string()]);
        let result = validate_multi_identifier(&id);
        assert!(result.is_ok());
    }
}
