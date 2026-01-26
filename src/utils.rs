use crate::ontology::error::RegistryError;
use directories::ProjectDirs;
use regex::Regex;
use std::env::home_dir;
use std::fs;
use std::path::PathBuf;

pub(crate) fn get_cache_dir() -> Result<PathBuf, RegistryError> {
    let pkg_name = env!("CARGO_PKG_NAME");

    let phenox_cache_dir = if let Some(project_dir) = ProjectDirs::from("", "", pkg_name) {
        project_dir.cache_dir().to_path_buf()
    } else if let Some(home_dir) = home_dir() {
        home_dir.join(pkg_name)
    } else {
        return Err(RegistryError::CantEstablishRegistryDir);
    };

    if !phenox_cache_dir.exists() {
        fs::create_dir_all(&phenox_cache_dir)?;
    }
    Ok(phenox_cache_dir.to_owned())
}

/// Checks if the string contains a colon.
/// If the expected_prefix is Some, then everything before the first colon is compared to the expected_prefix.
/// If the reference_regex is Some, then everything after the first colon is compared to the reference_regex.
pub(crate) fn is_curie(
    query: &str,
    expected_prefix: Option<&str>,
    reference_regex: Option<&Regex>,
) -> bool {
    if let Some((found_prefix, found_reference)) = query.split_once(':') {
        let prefix_match = expected_prefix.map(|p| found_prefix == p).unwrap_or(true);
        let reference_match = reference_regex
            .map(|r| r.is_match(found_reference))
            .unwrap_or(true);
        prefix_match && reference_match
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::is_curie;
    use regex::Regex;
    use rstest::rstest;

    #[rstest]
    fn test_is_curie_valid() {
        let reference_regex = Regex::new(r"^\d{7}$").unwrap();
        let hpo_curie = "HP:1234567";
        assert!(is_curie(hpo_curie, None, None));
        assert!(is_curie(hpo_curie, Some("HP"), None));
        assert!(is_curie(hpo_curie, None, Some(&reference_regex)));
        assert!(is_curie(hpo_curie, Some("HP"), Some(&reference_regex)));
    }

    #[rstest]
    fn test_is_curie_invalid_prefix() {
        assert!(!is_curie(
            "HQ:1234567",
            Some("HP"),
            Some(&Regex::new(r"^\d{7}$").unwrap())
        ));
    }

    #[rstest]
    fn test_is_curie_invalid_reference() {
        assert!(!is_curie(
            "HQ:abcdefg",
            Some("HP"),
            Some(&Regex::new(r"^\d{7}$").unwrap())
        ));
    }
}
