use crate::ontology::error::RegistryError;
use crate::ontology::traits::OntologyRegistry;

use crate::ontology::BioRegistryClient;
use crate::ontology::obolibrary_client::ObolibraryClient;

use log::{debug, info};

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
    file_name: Option<String>,
    /// The prefix of the ontology. For example HP for the Human Phenotype ontology.
    ontology_prefix: String,
    /// BioRegistryClient used to fetch metadata about ontologies
    bio_registry_client: BioRegistryClient,
    /// Obolibary client used to fetch ontologies
    obolib_client: ObolibraryClient,
}

impl ObolibraryOntologyRegistry {
    pub fn new(registry_path: PathBuf, file_name: Option<&str>, ontology_prefix: &str) -> Self {
        ObolibraryOntologyRegistry {
            registry_path,
            file_name: file_name.map(str::to_string),
            ontology_prefix: ontology_prefix.to_string(),
            bio_registry_client: BioRegistryClient::default(),
            obolib_client: ObolibraryClient::default(),
        }
    }
    #[allow(dead_code)]
    pub fn with_ontology_prefix<T: Into<String>>(mut self, ontology_prefix: T) -> Self {
        self.ontology_prefix = ontology_prefix.into();
        self
    }
    #[allow(dead_code)]
    pub fn with_registry_path<T: Into<PathBuf>>(mut self, registry_path: T) -> Self {
        self.registry_path = registry_path.into();
        self
    }
    #[allow(dead_code)]
    pub fn with_file_name<T: Into<String>>(mut self, file_name: T) -> Self {
        self.file_name = Some(file_name.into());
        self
    }

    #[cfg(not(test))]
    pub fn default_registry_path(ontology_prefix: &str) -> Result<PathBuf, RegistryError> {
        use crate::ontology::utils::get_cache_dir;
        use std::fs;

        let cache_dir = get_cache_dir()?;
        let registry_dir = cache_dir.join(ontology_prefix);

        if !registry_dir.exists() {
            fs::create_dir_all(&registry_dir)?;
        }
        Ok(registry_dir)
    }

    #[cfg(test)]
    pub fn default_registry_path(_: &str) -> Result<PathBuf, RegistryError> {
        let mut mock_registry_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        mock_registry_dir = mock_registry_dir
            .join("tests")
            .join("assets")
            .join("ontologies");
        Ok(mock_registry_dir)
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
        let hp_id = "hp";
        let registry_path = Self::default_registry_path(hp_id)?;

        info!("Using HP registry at {}", registry_path.display());
        Ok(ObolibraryOntologyRegistry::new(
            registry_path,
            Some("hp.json"),
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
        let mondo_id = "mondo";
        let registry_path = Self::default_registry_path(mondo_id)?;

        info!("Using Mondo registry at {}", registry_path.display());
        Ok(ObolibraryOntologyRegistry::new(
            registry_path,
            Some("mondo.json"),
            mondo_id,
        ))
    }

    /// Creates a default `ObolibraryOntologyRegistry` configured for the Genotype Ontology (GENO).
    ///
    /// This is a convenience constructor that sets up a registry with standard settings
    /// for downloading GENO in JSON format.
    ///
    /// # Configuration
    /// - **Ontology Prefix:** `"geno"`
    /// - **File Name:** `"geno.json"`
    /// - **Storage Path:**
    ///   - Primary: Platform-specific cache directory (e.g., `~/.cache/<crate_name>/geno` on Linux)
    ///   - Fallback: `$HOME/.<crate_name>/geno` if platform directories are unavailable
    ///
    /// The storage location is determined using the `directories` crate's project
    /// directories, with a fallback to the home directory if that fails.
    ///
    /// # Errors
    ///
    /// Returns `Err(RegistryError::EnvironmentVarNotSet)` if neither the platform-specific
    /// project directories nor the `HOME` environment variable can be determined.
    pub fn default_geno_registry() -> Result<Self, RegistryError> {
        let geno_id = "geno";
        let registry_path = Self::default_registry_path(geno_id)?;

        Ok(ObolibraryOntologyRegistry::new(
            registry_path,
            Some("geno.json"),
            geno_id,
        ))
    }

    /// Creates a default `ObolibraryOntologyRegistry` configured for the Human Phenotype Ontology (HPO) annotations.
    ///
    /// This is a convenience constructor that sets up a registry with standard settings
    /// for downloading the HPO in JSON format.
    ///
    /// # Configuration
    /// - **Ontology Prefix:** `"hp"`
    /// - **File Name:** `"hpoa.json"`
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
    #[allow(dead_code)]
    pub fn default_hpoa_registry() -> Result<Self, RegistryError> {
        let hpo = "hp";
        let registry_path = Self::default_registry_path(hpo)?;

        info!("Using HPa registry at {}", registry_path.display());
        Ok(ObolibraryOntologyRegistry::new(
            registry_path,
            Some("phenotype.hpoa"),
            hpo,
        ))
    }

    fn resolve_filename_and_version(&mut self, version: &str) -> Result<String, RegistryError> {
        let needs_metadata = version == "latest" || self.file_name.is_none();

        if !needs_metadata {
            return Ok(version.to_string());
        }

        let metadata = self
            .bio_registry_client
            .get_resource(self.ontology_prefix.as_str())?;

        let resolved_version = if version == "latest" {
            metadata.version.ok_or_else(|| {
                RegistryError::UnableToResolveVersion(version.to_string(), self.file_name.clone())
            })?
        } else {
            version.to_string()
        };

        if self.file_name.is_none() {
            // we only check the json file, because ontolius only supports json.
            if let Some(json_file_url) = metadata.download_json {
                self.file_name = Some(json_file_url.split("/").last().unwrap().to_string());
            } else {
                return Err(RegistryError::JsonFileMissing(metadata.prefix));
            }
        }

        Ok(resolved_version)
    }

    fn construct_file_name(&self, version: &str) -> String {
        format!(
            "{}_{}",
            version,
            self.file_name
                .clone()
                .expect("Tried constructing file name, but file name was not set.")
        )
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
    fn register(&mut self, version: &str) -> Result<PathBuf, RegistryError> {
        if !self.registry_path.exists() {
            std::fs::create_dir_all(&self.registry_path)?;
        }
        let resolved_version = self.resolve_filename_and_version(version)?;

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
            &self.file_name.clone().unwrap_or_else(|| {
                panic!(
                    "Expected file name. file_name was None for {} in registry.",
                    self.ontology_prefix
                )
            }),
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

    fn deregister(&mut self, version: &str) -> Result<(), RegistryError> {
        let resolved_version = self.resolve_filename_and_version(version)?;
        let file_path = self
            .registry_path
            .clone()
            .join(self.construct_file_name(resolved_version.as_str()));
        if !file_path.exists() {
            debug!("Unable to deregister: {}", file_path.display());
            return Err(RegistryError::NotRegistered(
                file_path.display().to_string(),
            ));
        }
        remove_file(file_path.clone())?;
        debug!("Deregistered {}", file_path.display());
        Ok(())
    }

    fn get_location(&mut self, version: &str) -> Option<PathBuf> {
        let resolved_version = self.resolve_filename_and_version(version).ok()?;
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
        let mut registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            Some("geno.json"),
            "geno",
        );

        let path = registry.register("latest").unwrap();

        assert!(path.exists());
    }

    #[rstest]
    fn test_new_registry_creation(temp_dir: TempDir) {
        let registry = ObolibraryOntologyRegistry::new(
            temp_dir.path().to_path_buf(),
            Some("hp-base.json"),
            "hpo",
        );

        assert_eq!(registry.registry_path, temp_dir.path());
        assert_eq!(registry.file_name, Some("hp-base.json".to_string()));
        assert_eq!(registry.ontology_prefix, "hpo");
    }

    #[rstest]
    fn test_with_registry_path(temp_dir: TempDir) {
        let new_path = temp_dir.path().join("custom");
        let registry =
            ObolibraryOntologyRegistry::new(temp_dir.path().to_path_buf(), Some("test.json"), "hp")
                .with_registry_path(new_path.clone());

        assert_eq!(registry.registry_path, new_path);
    }

    #[rstest]
    fn test_with_file_name(temp_dir: TempDir) {
        let file_name = "custom.json";
        let registry =
            ObolibraryOntologyRegistry::new(temp_dir.path().to_path_buf(), Some("test.json"), "hp")
                .with_file_name(file_name);

        assert_eq!(registry.file_name, Some(file_name.to_string()));
    }

    #[rstest]
    fn test_default_hpo_registry() {
        let registry = ObolibraryOntologyRegistry::default_hpo_registry().unwrap();
        /*assert!(
            registry
                .registry_path
                .to_str()
                .unwrap()
                .contains(env!("CARGO_PKG_NAME"))
        );*/
        // assert!(registry.registry_path.to_str().unwrap().contains("hp"));
        assert_eq!(registry.file_name, Some("hp.json".to_string()));
        assert_eq!(registry.ontology_prefix, "hp");
    }

    #[rstest]
    fn test_default_mondo_registry() {
        let registry = ObolibraryOntologyRegistry::default_mondo_registry().unwrap();
        /*assert!(
            registry
                .registry_path
                .to_str()
                .unwrap()
                .contains(env!("CARGO_PKG_NAME"))
        );*/

        // assert!(registry.registry_path.to_str().unwrap().contains("mondo"));
        assert_eq!(registry.file_name, Some("mondo.json".to_string()));
        assert_eq!(registry.ontology_prefix, "mondo");
    }

    #[rstest]
    fn test_default_geno_registry() {
        let registry = ObolibraryOntologyRegistry::default_geno_registry().unwrap();
        /*
        assert!(
            registry
                .registry_path
                .to_str()
                .unwrap()
                .to_lowercase()
                .contains(env!("CARGO_PKG_NAME"))
        );*/
        assert_eq!(registry.file_name, Some("geno.json".to_string()));
        assert_eq!(registry.ontology_prefix, "geno");
    }

    #[rstest]
    fn test_default_hpoa_registry() {
        let registry = ObolibraryOntologyRegistry::default_hpoa_registry().unwrap();
        /*assert!(
            registry
                .registry_path
                .to_str()
                .unwrap()
                .contains(env!("CARGO_PKG_NAME"))
        );*/

        // assert!(registry.registry_path.to_str().unwrap().contains("hp"));
        // assert_eq!(registry.file_name, Some("phenotype.hpoa".to_string()));
        assert_eq!(registry.ontology_prefix, "hp");
    }

    #[rstest]
    fn test_construct_file_name(temp_dir: TempDir) {
        let registry =
            ObolibraryOntologyRegistry::new(temp_dir.path().to_path_buf(), Some("hp.json"), "hp");

        let file_name = registry.construct_file_name("2024-07-01");
        assert_eq!(file_name, "2024-07-01_hp.json");
    }

    #[rstest]
    fn test_register_returns_existing_file(temp_dir: TempDir) {
        let mut registry =
            ObolibraryOntologyRegistry::new(temp_dir.path().to_path_buf(), Some("hp.json"), "hp");

        let cached_file_path = temp_dir.path().join("2024-07-01_hp.json");
        fs::write(&cached_file_path, b"fake ontology data").expect("Failed to write test file");

        let result = registry.register("2024-07-01");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), cached_file_path);
    }

    #[rstest]
    fn test_get_location_existing_file(temp_dir: TempDir) {
        let mut registry =
            ObolibraryOntologyRegistry::new(temp_dir.path().to_path_buf(), Some("hp.json"), "hp");

        let file_path = temp_dir.path().join("2024-07-01_hp.json");
        fs::write(&file_path, b"test data").expect("Failed to write test file");

        let location = registry.get_location("2024-07-01");

        assert!(location.is_some());
        assert_eq!(location.unwrap(), file_path);
    }

    #[rstest]
    fn test_get_location_non_existing_file(temp_dir: TempDir) {
        let mut registry =
            ObolibraryOntologyRegistry::new(temp_dir.path().to_path_buf(), Some("hp.json"), "hp");

        let location = registry.get_location("2024-07-01");

        assert!(location.is_none());
    }

    #[rstest]
    fn test_deregister_removes_file(temp_dir: TempDir) {
        let mut registry =
            ObolibraryOntologyRegistry::new(temp_dir.path().to_path_buf(), Some("hp.json"), "hp");

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
        let registry =
            ObolibraryOntologyRegistry::new(temp_dir.path().to_path_buf(), Some(file_name), prefix);

        assert_eq!(registry.ontology_prefix, prefix);
        assert_eq!(registry.file_name, Some(file_name.to_string()));
    }
}
