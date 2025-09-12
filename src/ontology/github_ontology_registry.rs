#![allow(dead_code)]
use crate::ontology::traits::OntologyRegistry;

use crate::ontology::error::RegistryError;
use crate::ontology::github_release_client::GithubReleaseClient;
use log::debug;
use std::env;
use std::fs::{File, remove_file};
use std::io::copy;
use std::path::PathBuf;

/// Manages the download and local caching of ontology files from a GitHub repository.
///
/// This registry is responsible for fetching specific file assets from GitHub releases,
/// storing them in a local directory, and retrieving the path to the cached file.
pub(crate) struct GithubOntologyRegistry {
    /// The local file system path where ontology files will be stored.
    registry_path: PathBuf,
    /// The name of the GitHub repository to fetch releases from (e.g., "human-phenotype-ontology").
    repo_name: String,
    /// The owner or organization of the GitHub repository (e.g., "obophenotype").
    repo_owner: String,
    /// The specific file name to download from the release assets (e.g., "hp-base.json").
    file_name: String,
    /// The client used to interact with the GitHub Releases API.
    github_client: GithubReleaseClient,
}

#[doc = "Implementation of GithubOntologyRegistry."]
impl GithubOntologyRegistry {
    pub fn new(
        registry_path: PathBuf,
        repo_name: String,
        repo_owner: String,
        file_name: String,
    ) -> Self {
        GithubOntologyRegistry {
            registry_path,
            repo_name,
            repo_owner,
            file_name,
            github_client: GithubReleaseClient::default(),
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

    /// Creates a default registry specifically for the Human Phenotype Ontology (HPO).
    ///
    /// This constructor configures the registry to download `hp-base.json` from the
    /// `obophenotype/human-phenotype-ontology` repository. The local cache path is
    /// set to `$HOME/.<cargo_pkg_name>/`.
    ///
    /// # Panics
    ///
    /// Panics if the `HOME` environment variable is not set.
    ///
    /// # Returns
    ///
    /// A `GithubOntologyRegistry` instance pre-configured for HPO.
    pub fn default_hpo_registry() -> Result<Self, RegistryError> {
        let env_var = "HOME";
        let home_dir_result = env::var(env_var);

        if home_dir_result.is_err() {
            return Err(RegistryError::EnvironmentVarNotSet(env_var.to_string()));
        }

        let home_dir = home_dir_result.unwrap();
        let pkg_name = env!("CARGO_PKG_NAME");
        let path: PathBuf = [home_dir.as_str(), format!(".{pkg_name}").as_str()]
            .iter()
            .collect();

        Ok(GithubOntologyRegistry::new(
            path,
            "human-phenotype-ontology".to_string(),
            "obophenotype".to_string(),
            "hp-base.json".to_string(),
        ))
    }

    fn resolve_version(&self, version: &str) -> String {
        if version == "latest" {
            self.github_client
                .get_latest_release_tag(&self.repo_owner, &self.repo_name)
                .expect("get latest tag failed")
        } else {
            version.to_string()
        }
    }

    fn construct_file_name(&self, version: &str) -> String {
        format!("{}_{}_{}", self.repo_name, version, self.file_name)
    }
}
impl OntologyRegistry for GithubOntologyRegistry {
    /// Downloads and registers an ontology file from GitHub for a specific version.
    ///
    /// If the file for the specified version already exists in the local cache,
    /// it returns the path to that file without downloading it again.
    ///
    /// The `version` can be a specific release tag (e.g., "v2024-07-01") or "latest",
    /// in which case the registry will query the GitHub API for the most recent release tag.
    ///
    /// # Arguments
    ///
    /// * `version` - A string representing the release tag to download, or "latest".
    ///
    /// # Returns
    ///
    /// A `Result` containing the `PathBuf` to the locally cached ontology file on success,
    /// or an `anyhow::Error` if the download, file creation, or API interaction fails.
    fn register(&self, version: &str) -> Result<PathBuf, RegistryError> {
        if !self.registry_path.exists() {
            std::fs::create_dir_all(&self.registry_path)?;
        }

        let resolved_version = self.resolve_version(version);

        let mut out_path = self.registry_path.clone();
        out_path.push(self.construct_file_name(version));

        if out_path.exists() {
            debug!(
                "Ontology version already registered. {}",
                out_path.display()
            );
            return Ok(out_path);
        }

        let mut resp = self.github_client.get_release_file(
            self.repo_owner.as_str(),
            self.repo_name.as_str(),
            self.file_name.as_str(),
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
    #[allow(dead_code)]
    #[allow(unused)]
    fn deregister(&self, version: &str) -> Result<(), RegistryError> {
        let resolved_version = self.resolve_version(version);
        let file_path = self
            .registry_path
            .clone()
            .join(self.construct_file_name(resolved_version.as_str()));
        if !file_path.exists() {
            debug!("Unable do deregistered: {}", file_path.display());
            return Err(RegistryError::NotRegistered(
                format!("Version: {resolved_version} not registered in registry").to_string(),
            ));
        }
        remove_file(file_path.clone())?;
        debug!("Deregistered {}", file_path.display());
        Ok(())
    }
    #[allow(dead_code)]
    #[allow(unused)]
    fn get_location(&self, version: &str) -> Option<PathBuf> {
        let resolved_version = self.resolve_version(version);
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
    use mockito::ServerGuard;
    use rstest::{fixture, rstest};
    use serde_json::json;
    use tempfile::TempDir;

    #[fixture]
    fn latest_tag() -> String {
        "latest_tag".to_string()
    }
    #[fixture]
    fn release_version() -> String {
        "conch-street-124".to_string()
    }
    #[fixture]
    fn release_file_name() -> String {
        "bikini-bottom.json".to_string()
    }
    #[fixture]
    fn repo_owner() -> String {
        "patrick-star".to_string()
    }
    #[fixture]
    fn repo_name() -> String {
        "bikini-bottom".to_string()
    }

    #[fixture]
    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("Failed to create temporary directory")
    }

    #[fixture]
    fn mock_server(
        latest_tag: String,
        repo_owner: String,
        repo_name: String,
        release_version: String,
        release_file_name: String,
    ) -> ServerGuard {
        let mut server = mockito::Server::new();

        let latest_json_payload =
            serde_json::to_string(&crate::ontology::github_release_client::Release {
                tag_name: latest_tag.parse().unwrap(),
            })
            .unwrap();

        let _ = server
            .mock(
                "GET",
                format!("/repos/{repo_owner}/{repo_name}/releases/latest").as_str(),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(latest_json_payload)
            .create();

        let release_json_file = json!({
            "graphs": {"id": "some_id"},
            "version": release_version,
        });

        let _ = server
            .mock(
                "GET",
                format!("/{repo_owner}/{repo_name}/releases/download/{release_version}/{release_file_name}")
                    .as_str(),
            )
            .with_status(200)
            .with_header("content-type", "application/octet-stream")
            .with_header(
                "content-disposition",
                format!("attachment; filename={release_file_name}").as_str(),
            )
            .with_body(serde_json::to_vec(&release_json_file).unwrap())
            .create();

        let release_json_file = json!({
            "graphs": {"id": "some_id"},
            "version": latest_tag,
        });

        let _ = server
            .mock(
                "GET",
                format!(
                    "/{repo_owner}/{repo_name}/releases/download/{latest_tag}/{release_file_name}"
                )
                .as_str(),
            )
            .with_status(200)
            .with_header("content-type", "application/octet-stream")
            .with_header(
                "content-disposition",
                format!("attachment; filename={release_file_name}").as_str(),
            )
            .with_body(serde_json::to_vec(&release_json_file).unwrap())
            .create();

        server
    }

    fn build_registry(
        tempdir: &TempDir,
        server_url: String,
        repo_name: String,
        repo_owner: String,
        release_file_name: String,
    ) -> GithubOntologyRegistry {
        let mut mock_web_url = server_url.clone();
        mock_web_url.push_str("/{repo_owner}/{repo_name}/releases/download/{version}/{file_name}");
        let mut mock_release_url = server_url.clone();
        mock_release_url.push_str("/repos/{repo_owner}/{repo_name}/releases/latest");

        let github_client = GithubReleaseClient::default()
            .with_web_url_template(mock_web_url)
            .with_latest_release_url_template(mock_release_url);

        GithubOntologyRegistry {
            registry_path: tempdir.path().to_path_buf(),
            repo_name,
            repo_owner,
            file_name: release_file_name,
            github_client,
        }
    }

    #[rstest]
    fn test_register_creates_new_file(
        repo_name: String,
        repo_owner: String,
        release_file_name: String,
        release_version: String,
        mock_server: ServerGuard,
        temp_dir: TempDir,
    ) {
        let registry = build_registry(
            &temp_dir,
            mock_server.url(),
            repo_name,
            repo_owner,
            release_file_name,
        );

        let path = registry.register(release_version.as_str()).unwrap();
        assert!(path.exists());

        let contents = std::fs::read_to_string(path).unwrap();
        assert!(contents.contains("graphs"));
    }

    #[rstest]
    fn test_register_returns_existing_file_if_present(
        repo_name: String,
        repo_owner: String,
        release_file_name: String,
        mock_server: ServerGuard,
    ) {
        let tmp = TempDir::new().unwrap();
        let file_path = tmp
            .path()
            .join(format!("{repo_name}_v1.0.0_{release_file_name}"));

        std::fs::write(&file_path, "already here").unwrap();

        let registry = build_registry(
            &tmp,
            mock_server.url(),
            repo_name,
            repo_owner,
            release_file_name,
        );

        let result = registry.register("v1.0.0").unwrap();
        assert_eq!(result, file_path);

        let contents = std::fs::read_to_string(result).unwrap();
        assert_eq!(contents, "already here");
    }

    #[rstest]
    fn test_register_with_latest_version(
        repo_name: String,
        repo_owner: String,
        release_file_name: String,
        mock_server: ServerGuard,
        temp_dir: TempDir,
    ) {
        let registry = build_registry(
            &temp_dir,
            mock_server.url(),
            repo_name,
            repo_owner,
            release_file_name,
        );

        let path = registry.register("latest").unwrap();
        assert!(path.exists());

        let contents = std::fs::read_to_string(path.clone()).unwrap();
        assert!(contents.contains("latest_tag"));
        assert!(contents.contains("graphs"));
    }

    #[rstest]
    fn test_construct_file_name() {
        let reg = GithubOntologyRegistry::default_hpo_registry().unwrap();

        let file_name = reg.construct_file_name("1.0.0");
        assert_eq!(file_name, "human-phenotype-ontology_1.0.0_hp-base.json");
    }

    #[rstest]
    fn test_get_location_success(temp_dir: TempDir, mock_server: ServerGuard) {
        let registry_path = temp_dir.path().to_path_buf();

        let reg = build_registry(
            &temp_dir,
            mock_server.url(),
            "repo_name".to_string(),
            "repo_owner".to_string(),
            "release_file_name.json".to_string(),
        );

        let file_name = reg.construct_file_name("1.0.0");
        let file_path = registry_path.join(&file_name);
        File::create(&file_path).unwrap();

        let result = reg.get_location("1.0.0");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), file_path);
    }

    #[rstest]
    fn test_get_location_not_registered(temp_dir: TempDir, mock_server: ServerGuard) {
        let reg = build_registry(
            &temp_dir,
            mock_server.url(),
            "repo_name".to_string(),
            "repo_owner".to_string(),
            "release_file_name.json".to_string(),
        );

        let result = reg.get_location("1.0.0");
        assert!(result.is_none());
    }

    #[rstest]
    fn test_deregister_success(temp_dir: TempDir, mock_server: ServerGuard) {
        let reg = build_registry(
            &temp_dir,
            mock_server.url(),
            "repo_name".to_string(),
            "repo_owner".to_string(),
            "release_file_name.json".to_string(),
        );

        let file_name = reg.construct_file_name("1.0.0");
        let file_path = temp_dir.path().to_path_buf().join(&file_name);
        File::create(&file_path).unwrap();
        assert!(file_path.exists());

        let result = reg.deregister("1.0.0");
        assert!(result.is_ok());
        assert!(!file_path.exists());
    }

    #[rstest]
    fn test_deregister_not_registered(temp_dir: TempDir, mock_server: ServerGuard) {
        let reg = build_registry(
            &temp_dir,
            mock_server.url(),
            "repo_name".to_string(),
            "repo_owner".to_string(),
            "release_file_name.json".to_string(),
        );

        let result = reg.deregister("1.0.0");
        assert!(matches!(result, Err(RegistryError::NotRegistered(_))));
    }

    #[rstest]
    fn test_resolve_version(
        repo_name: String,
        repo_owner: String,
        release_file_name: String,
        latest_tag: String,
        release_version: String,
        mock_server: ServerGuard,
        temp_dir: TempDir,
    ) {
        let registry = build_registry(
            &temp_dir,
            mock_server.url(),
            repo_name,
            repo_owner,
            release_file_name,
        );
        let resolved_version = registry.resolve_version("latest");
        assert_eq!(resolved_version, latest_tag);

        let resolved_version = registry.resolve_version(release_version.as_str());
        assert_eq!(resolved_version, release_version);
    }
}
