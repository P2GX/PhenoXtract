use crate::config::table_context::{Identifier, SeriesContext};
use crate::validation::validation_utils::validate_regex;
use std::borrow::Cow;
use validator::ValidationError;

pub(crate) fn validate_identifier(series_context: &SeriesContext) -> Result<(), ValidationError> {
    match series_context.get_identifier() {
        Identifier::Regex(r) => validate_regex(r),
        Identifier::Multi(vec) => {
            if vec.is_empty() {
                let mut error = ValidationError::new("invalid_multi_identifier");
                error.add_param(Cow::from("identifier"), &vec);
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
    use crate::config::traits::SeriesContextBuilding;
    use rstest::rstest;

    #[rstest]
    fn test_multi_identifier_regex_delegates() {
        let id = Identifier::Regex("^[a-z]+$".to_string());
        let sc = SeriesContext::from_identifier(id);
        let result = validate_identifier(&sc);
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_multi_identifier_multi_empty_vec_errors() {
        let id = Identifier::Multi(vec![]);
        let sc = SeriesContext::default().with_identifier(id);
        let result = validate_identifier(&sc);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, "invalid_multi_identifier");
        assert!(err.message.unwrap().contains("at least one ID"));
    }

    #[rstest]
    fn test_multi_identifier_multi_non_empty_vec_ok() {
        let id = Identifier::Multi(vec!["abc".to_string()]);
        let sc = SeriesContext::default().with_identifier(id);
        let result = validate_identifier(&sc);
        assert!(result.is_ok());
    }
}
