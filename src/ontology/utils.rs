use crate::ontology::error::RegistryError;
use directories::ProjectDirs;
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
