#![allow(dead_code)]
use crate::ontology::traits::OntologyRegistry;

use crate::ontology::error::RegistryError;

use crate::ontology::BioRegistryClient;
use crate::ontology::obolibrary_client::ObolibraryClient;
use directories::ProjectDirs;
use log::debug;
use std::env;
use std::env::home_dir;
use std::fs::{File, remove_file};
use std::io::copy;
use std::path::PathBuf;

/// Manages the download and local caching of ontology files from a obolibrary repository.
///
/// This registry is responsible for fetching specific file assets from GitHub releases,
/// storing them in a local directory, and retrieving the path to the cached file.
pub struct ObolibraryOntologyRegistry {
    /// The local file system path where ontology files will be stored.
    registry_path: PathBuf,
    /// The specific file name to download from the release assets (e.g., "hp-base.json").
    file_name: String,
    /// The prefix of the ontology. For example HP for the Human Phenotype ontology.
    ontology_prefix: String,
    /// BioRegistryClient used to fetch metadata about ontologies
    bio_registry_client: BioRegistryClient,
    /// Obolibary client used to fetch ontologies
    obolib_client: ObolibraryClient,
}

impl ObolibraryOntologyRegistry {
    pub fn new(registry_path: PathBuf, file_name: String, ontology_prefix: String) -> Self {
        ObolibraryOntologyRegistry {
            registry_path,
            file_name,
            ontology_prefix,
            bio_registry_client: BioRegistryClient::default(),
            obolib_client: ObolibraryClient::default(),
        }
    }

    pub fn with_registry_path(mut self, registry_path: PathBuf) -> Self {
        self.registry_path = registry_path;
        self
    }

    pub fn with_file_name(mut self, file_name: String) -> Self {
        self.file_name = file_name;
        self
    }

    fn default_registry_path(id: &str) -> Result<PathBuf, RegistryError> {
        let pkg_name = env!("CARGO_PKG_NAME");
        ProjectDirs::from("", "", pkg_name)
            .map(|proj| proj.cache_dir().join(id))
            .or_else(|| home_dir().map(|dir| dir.join(format!(".{pkg_name}")).join(id)))
            .ok_or_else(|| {
                RegistryError::EnvironmentVarNotSet(
                    "Could not setup registry directory. No Home or Project directory found."
                        .to_string(),
                )
            })
    }

    /// Creates a default `ObolibraryOntologyRegistry` configured for the Human Phenotype Ontology (HPO).
    ///
    /// This is a convenience constructor that sets up a registry with standard settings
    /// for downloading the HPO in JSON format.
    ///
    /// # Configuration
    /// - **Ontology Prefix:** `"hp"`
    /// - **File Name:** `"hp.json"`
    /// - **Storage Path:**
    ///   - Primary: Platform-specific cache directory (e.g., `~/.cache/<crate_name>/hp` on Linux)
    ///   - Fallback: `$HOME/.<crate_name>/hp` if platform directories are unavailable
    ///
    /// The storage location is determined using the `directories` crate's project
    /// directories, with a fallback to the home directory if that fails.
    ///
    /// # Errors
    ///
    /// Returns `Err(RegistryError::EnvironmentVarNotSet)` if neither the platform-specific
    /// project directories nor the `HOME` environment variable can be determined.
    pub fn default_hpo_registry() -> Result<Self, RegistryError> {
        let hp_id = "hp".to_string();
        let registry_path = Self::default_registry_path(hp_id.as_str())?;

        Ok(ObolibraryOntologyRegistry::new(
            registry_path,
            "hp.json".to_string(),
            hp_id,
        ))
    }

    /// Creates a default `ObolibraryOntologyRegistry` configured for the Mondo Disease Ontology.
    ///
    /// This is a convenience constructor that sets up a registry with standard settings
    /// for downloading Mondo in JSON format.
    ///
    /// # Configuration
    /// - **Ontology Prefix:** `"mondo"`
    /// - **File Name:** `"mondo.json"`
    /// - **Storage Path:**
    ///   - Primary: Platform-specific cache directory (e.g., `~/.cache/<crate_name>/mondo` on Linux)
    ///   - Fallback: `$HOME/.<crate_name>/mondo` if platform directories are unavailable
    ///
    /// The storage location is determined using the `directories` crate's project
    /// directories, with a fallback to the home directory if that fails.
    ///
    /// # Errors
    ///
    /// Returns `Err(RegistryError::EnvironmentVarNotSet)` if neither the platform-specific
    /// project directories nor the `HOME` environment variable can be determined.
    pub fn default_mondo_registry() -> Result<Self, RegistryError> {
        let mondo_id = "mondo".to_string();
        let registry_path = Self::default_registry_path(mondo_id.as_str())?;

        Ok(ObolibraryOntologyRegistry::new(
            registry_path,
            "mondo.json".to_string(),
            mondo_id,
        ))
    }

    fn resolve_version(&self, version: &str) -> Result<String, RegistryError> {
        if version == "latest" {
            let meta_data = self
                .bio_registry_client
                .get_resource(&self.ontology_prefix)
                .expect("get latest tag failed");
            meta_data
                .version
                .ok_or(RegistryError::UnableToResolveVersion(format!(
                    "Could not resolve version for {} ",
                    self.file_name
                )))
        } else {
            Ok(version.to_string())
        }
    }

    fn construct_file_name(&self, version: &str) -> String {
        format!("{}_{}", version, self.file_name)
    }
}
impl OntologyRegistry for ObolibraryOntologyRegistry {
    /// Ensures an ontology file for a specific version is available in the local registry.
    ///
    /// This function acts as a local cache. It first checks if the file for the
    /// requested version already exists. If it does, it immediately returns the path
    /// to the existing file. This makes the operation idempotent.
    ///
    /// If the file is not found locally, the function proceeds to:
    /// 1. Ensure the local registry directory exists, creating it if necessary.
    /// 2. Resolve the version string (e.g., converting "latest" to a concrete version like "2025-10-07").
    /// 3. Download the ontology file from the OBO Library.
    /// 4. Stream the downloaded content directly to a file in the registry.
    /// 5. Return the path to the newly created file.
    ///
    /// # Parameters
    ///
    /// * `version`: A string slice representing the desired version of the ontology.
    ///   This can be a specific version tag (e.g., "2023-09-01") or a symbolic
    ///   name like "latest".
    ///
    /// # Returns
    ///
    /// A `Result` which contains:
    /// * `Ok(PathBuf)`: The absolute path to the ontology file in the local registry.
    /// * `Err(RegistryError)`: An error that occurred during the process.
    ///
    /// # Errors
    ///
    /// This function will return an error in the following cases:
    /// * The registry directory cannot be created (e.g., due to permissions).
    /// * The ontology download fails (e.g., network error, file not found on server).
    /// * The local file cannot be created or written to.
    fn register(&self, version: &str) -> Result<PathBuf, RegistryError> {
        if !self.registry_path.exists() {
            std::fs::create_dir_all(&self.registry_path)?;
        }

        let resolved_version = self.resolve_version(version)?;

        let mut out_path = self.registry_path.clone();
        out_path.push(self.construct_file_name(&resolved_version));

        if out_path.exists() {
            debug!(
                "Ontology version already registered. {}",
                out_path.display()
            );
            return Ok(out_path);
        }

        let mut resp = self.obolib_client.get_ontology(
            &self.ontology_prefix,
            &self.file_name,
            &resolved_version,
        )?;

        let mut out = File::create(out_path.clone())?;

        copy(&mut resp, &mut out)?;
        debug!(
            "Registered {} ({} bytes)",
            out_path.display(),
            out.metadata()?.len()
        );

        Ok(out_path)
    }
    #[allow(unused)]
    fn deregister(&self, version: &str) -> Result<(), RegistryError> {
        let resolved_version = self.resolve_version(version)?;
        let file_path = self
            .registry_path
            .clone()
            .join(self.construct_file_name(resolved_version.as_str()));
        if !file_path.exists() {
            debug!("Unable to deregistered: {}", file_path.display());
            return Err(RegistryError::NotRegistered(
                format!("Version: {resolved_version} not registered in registry").to_string(),
            ));
        }
        remove_file(file_path.clone())?;
        debug!("Deregistered {}", file_path.display());
        Ok(())
    }

    #[allow(unused)]
    fn get_location(&self, version: &str) -> Option<PathBuf> {
        let resolved_version = self.resolve_version(version).ok()?;
        let file_path = self
            .registry_path
            .clone()
            .join(self.construct_file_name(resolved_version.as_str()));
        if !file_path.exists() {
            debug!("Unable do getting location: {}", file_path.display());
            return None;
        }

        debug!("Returned register location {}", file_path.display());
        Some(file_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use std::fs;
    use tempfile::TempDir;

    #[fixture]
    fn temp_dir() -> TempDir {
        TempDir::new().expect("Failed to create temp dir")
    }

    #[rstest]
    fn test_register(temp_dir: TempDir) {
        let registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            "hp-base.json".to_string(),
            "hpo".to_string(),
        );

        let path = registry.register("latest").unwrap();

        assert!(path.exists());
    }

    #[rstest]
    fn test_new_registry_creation(temp_dir: TempDir) {
        let registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            "hp-base.json".to_string(),
            "hpo".to_string(),
        );

        assert_eq!(registry.registry_path, temp_dir.path());
        assert_eq!(registry.file_name, "hp-base.json");
        assert_eq!(registry.ontology_prefix, "hpo");
    }

    #[rstest]
    fn test_with_registry_path(temp_dir: TempDir) {
        let new_path = temp_dir.path().join("custom");
        let registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            "test.json".to_string(),
            "hp".to_string(),
        )
        .with_registry_path(new_path.clone());

        assert_eq!(registry.registry_path, new_path);
    }

    #[rstest]
    fn test_with_file_name(temp_dir: TempDir) {
        let registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            "test.json".to_string(),
            "hp".to_string(),
        )
        .with_file_name("custom.json".to_string());

        assert_eq!(registry.file_name, "custom.json");
    }

    #[rstest]
    fn test_default_hpo_registry() {
        let registry = ObolibraryOntologyRegistry::default_hpo_registry().unwrap();
        assert!(
            registry
                .registry_path
                .to_str()
                .unwrap()
                .contains(env!("CARGO_PKG_NAME"))
        );
        assert!(registry.registry_path.to_str().unwrap().contains("hp"));
        assert_eq!(registry.file_name, "hp.json");
        assert_eq!(registry.ontology_prefix, "hp");
    }

    #[rstest]
    fn test_default_mondo_registry() {
        let registry = ObolibraryOntologyRegistry::default_mondo_registry().unwrap();
        assert!(
            registry
                .registry_path
                .to_str()
                .unwrap()
                .contains(env!("CARGO_PKG_NAME"))
        );

        assert!(registry.registry_path.to_str().unwrap().contains("mondo"));
        assert_eq!(registry.file_name, "mondo.json");
        assert_eq!(registry.ontology_prefix, "mondo");
    }

    #[rstest]
    fn test_construct_file_name(temp_dir: TempDir) {
        let registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            "hp.json".to_string(),
            "hp".to_string(),
        );

        let file_name = registry.construct_file_name("2024-07-01");
        assert_eq!(file_name, "2024-07-01_hp.json");
    }

    #[rstest]
    fn test_register_returns_existing_file(temp_dir: TempDir) {
        let registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            "hp.json".to_string(),
            "hp".to_string(),
        );

        let cached_file_path = temp_dir.path().join("2024-07-01_hp.json");
        fs::write(&cached_file_path, b"fake ontology data").expect("Failed to write test file");

        let result = registry.register("2024-07-01");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), cached_file_path);
    }

    #[rstest]
    fn test_get_location_existing_file(temp_dir: TempDir) {
        let registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            "hp.json".to_string(),
            "hp".to_string(),
        );

        // Create a fake file
        let file_path = temp_dir.path().join("2024-07-01_hp.json");
        fs::write(&file_path, b"test data").expect("Failed to write test file");

        let location = registry.get_location("2024-07-01");

        assert!(location.is_some());
        assert_eq!(location.unwrap(), file_path);
    }

    #[rstest]
    fn test_get_location_non_existing_file(temp_dir: TempDir) {
        let registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            "hp.json".to_string(),
            "hp".to_string(),
        );

        let location = registry.get_location("2024-07-01");

        assert!(location.is_none());
    }

    #[rstest]
    fn test_deregister_removes_file(temp_dir: TempDir) {
        let registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            "hp.json".to_string(),
            "hp".to_string(),
        );

        let file_path = temp_dir.path().join("2024-07-01_hp.json");
        fs::write(&file_path, b"test data").expect("Failed to write test file");

        assert!(file_path.exists());

        let result = registry.deregister("2024-07-01");

        assert!(result.is_ok());
        assert!(!file_path.exists());
    }

    #[rstest]
    #[case("hp", "test.json")]
    #[case("mondo", "mondo.json")]
    #[case("go", "go-base.json")]
    fn test_registry_with_different_ontologies(
        temp_dir: TempDir,
        #[case] prefix: &str,
        #[case] file_name: &str,
    ) {
        let registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            file_name.to_string(),
            prefix.to_string(),
        );

        assert_eq!(registry.ontology_prefix, prefix);
        assert_eq!(registry.file_name, file_name);
    }
}
