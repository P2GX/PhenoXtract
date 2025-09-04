use crate::config::series_context::MultiIdentifier;
use crate::validation::validation_utils::validate_regex;
use validator::ValidationError;

pub(crate) fn validate_regex_multi_identifier(
    regex: &MultiIdentifier,
) -> Result<(), ValidationError> {
    if let MultiIdentifier::Regex(r) = regex {
        return validate_regex(r);
    }
    Ok(())
}
