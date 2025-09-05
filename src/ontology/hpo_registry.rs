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
}
impl Default for GithubOntologyRegistry {
    fn default() -> GithubOntologyRegistry {
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
            debug!("HPO version already downloaded. {}", out_path.display());
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
