#![allow(dead_code)]
use crate::ontology::traits::OntologyRegistry;
use anyhow::Error;

use crate::ontology::github_release_client::GithubReleaseClient;
use log::debug;
use std::env;
use std::fs::File;
use std::io::copy;
use std::path::PathBuf;

struct GithubOntologyRegistry {
    registry_path: PathBuf,
    repo_name: String,
    repo_owner: String,
    file_name: String,
    github_client: GithubReleaseClient,
}

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

    pub fn default_hpo_registry() -> Self {
        let home_dir = env::var("HOME").expect("HOME not set");
        let pkg_name = env!("CARGO_PKG_NAME");
        let path: PathBuf = [home_dir.as_str(), format!(".{pkg_name}").as_str()]
            .iter()
            .collect();

        GithubOntologyRegistry::new(
            path,
            "human-phenotype-ontology".to_string(),
            "obophenotype".to_string(),
            "hp-base.json".to_string(),
        )
    }
}
impl OntologyRegistry for GithubOntologyRegistry {
    fn register(&self, version: &str) -> Result<PathBuf, anyhow::Error> {
        if !self.registry_path.exists() {
            std::fs::create_dir_all(&self.registry_path)?;
        }

        let mut out_path = self.registry_path.clone();
        out_path.push(format!("hp_{version}.json"));

        if out_path.exists() {
            debug!("HPO version already registered. {}", out_path.display());
            return Ok(out_path);
        }

        let resolved_version = if version == "latest" {
            self.github_client
                .get_latest_release_tag(self.repo_owner.as_str(), self.repo_name.as_str())?
        } else {
            version.to_string()
        };

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
    fn deregister(&self, version: &str) -> Result<bool, anyhow::Error> {
        todo!()
    }

    fn get_location(&self, version: &str) -> Result<PathBuf, Error> {
        todo!()
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
    ) {
        let tmp = TempDir::new().unwrap();

        let registry = build_registry(
            &tmp,
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
        let file_path = tmp.path().join("hp_v1.0.0.json");

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
    ) {
        let tmp = TempDir::new().unwrap();

        let registry = build_registry(
            &tmp,
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
}
