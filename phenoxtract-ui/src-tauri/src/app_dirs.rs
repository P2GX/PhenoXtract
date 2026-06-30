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
    std::fs::create_dir_all(proj_dirs.config_dir())
        .map_err(|err| PhenoxtractBackendError::UnableToInitStateDirs(err.to_string()))?;

    Ok(AppDirs::new(
        proj_dirs.config_dir().to_path_buf(),
        proj_dirs.data_dir().to_path_buf(),
        proj_dirs.config_dir().to_path_buf(),
    ))
}
