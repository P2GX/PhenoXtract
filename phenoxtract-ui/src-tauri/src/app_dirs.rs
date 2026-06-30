use crate::error::PhenoxtractBackendError;
use directories::ProjectDirs;
use std::path::PathBuf;

#[derive(Clone)]
pub struct AppDirs {
    config_dir: PathBuf,
    _data_dir: PathBuf,
    _cache_dir: PathBuf,
}

impl AppDirs {
    pub fn new(config_dir: PathBuf, data_dir: PathBuf, cache_dir: PathBuf) -> Self {
        Self {
            config_dir,
            _data_dir: data_dir,
            _cache_dir: cache_dir,
        }
    }
    pub fn config_dir(&self) -> &PathBuf {
        &self.config_dir
    }
    pub fn _data_dir(&self) -> &PathBuf {
        &self._data_dir
    }
    pub fn _cache_dir(&self) -> &PathBuf {
        &self._cache_dir
    }
}
pub fn init_app_dirs() -> Result<AppDirs, PhenoxtractBackendError> {
    let proj_dirs = ProjectDirs::from("com", "RobinsonLab", env!("CARGO_PKG_NAME"))
        .expect("Could not determine project directories");

    std::fs::create_dir_all(proj_dirs.config_dir())
        .map_err(|err| PhenoxtractBackendError::UnableToInitStateDirs(err.to_string()))?;
    std::fs::create_dir_all(proj_dirs.data_dir())
        .map_err(|err| PhenoxtractBackendError::UnableToInitStateDirs(err.to_string()))?;
    std::fs::create_dir_all(proj_dirs.cache_dir())
        .map_err(|err| PhenoxtractBackendError::UnableToInitStateDirs(err.to_string()))?;

    Ok(AppDirs::new(
        proj_dirs.config_dir().to_path_buf(),
        proj_dirs.data_dir().to_path_buf(),
        proj_dirs.cache_dir().to_path_buf(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_app_dirs_new_and_getters() {
        let mock_config = PathBuf::from("/mock/config/path");
        let mock_data = PathBuf::from("/mock/data/path");
        let mock_cache = PathBuf::from("/mock/cache/path");

        let app_dirs = AppDirs::new(mock_config.clone(), mock_data.clone(), mock_cache.clone());

        assert_eq!(app_dirs.config_dir(), &mock_config);
        assert_eq!(app_dirs._data_dir(), &mock_data);
        assert_eq!(app_dirs._cache_dir(), &mock_cache);
    }

    #[test]
    fn test_init_app_dirs_succeeds() {
        let result = init_app_dirs();

        assert!(result.is_ok(), "Failed to initialize app directories");

        let dirs = result.unwrap();

        assert!(
            dirs.config_dir().exists(),
            "Config directory was not created"
        );
        assert!(dirs._data_dir().exists(), "Data directory was not created");
        assert!(
            dirs._cache_dir().exists(),
            "Cache directory was not created"
        );
    }
}
