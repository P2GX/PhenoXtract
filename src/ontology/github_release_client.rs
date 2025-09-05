#![allow(dead_code)]
use log::debug;
use reqwest::blocking::{Client, Response, get};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Release {
    pub tag_name: String,
}

/// A client for interacting with Github releases.
///
/// This client provides methods to fetch the latest release information
/// and download release files from a specified Github repository.
pub(crate) struct GithubReleaseClient {
    /// URL template for downloading a specific release file. The client will replace the substrings {repo_owner}, {repo_name}, {version}, {file_name} to assemble the url.
    web_url: String,
    /// URL template for fetching the latest release information from the Github API. The client will replace the substrings {repo_owner}, {repo_name} to assemble the url.
    latest_release_url: String,
}

impl GithubReleaseClient {
    /// Creates a new `GithubReleaseClient` with default URL templates.
    ///
    /// The templates are pre-configured for Github's release and API endpoints.
    pub fn new() -> GithubReleaseClient {
        GithubReleaseClient {
            web_url: "https://github.com/{repo_owner}/{repo_name}/releases/download/{version}/{file_name}"
                .to_string(),
            latest_release_url:
                "https://api.github.com/repos/{repo_owner}/{repo_name}/releases/latest".to_string(),
        }
    }

    pub fn with_web_url_template(mut self, url: impl Into<String>) -> Self {
        self.web_url = url.into();
        self
    }

    pub fn with_latest_release_url_template(mut self, url: impl Into<String>) -> Self {
        self.latest_release_url = url.into();
        self
    }

    /// Fetches the tag name of the latest release for a given repository.
    ///
    /// # Arguments
    ///
    /// * `repo_owner` - The owner of the Github repository (e.g., "rust-lang").
    /// * `repo_name` - The name of the Github repository (e.g., "rust").
    ///
    /// # Returns
    ///
    /// A `Result` containing the latest release tag name as a `String` on success,
    /// or an `reqwest::Error` on failure.
    pub fn get_latest_release_tag(
        &self,
        repo_ower: &str,
        repo_name: &str,
    ) -> Result<String, reqwest::Error> {
        let mut url = self.latest_release_url.clone();
        url = url
            .replace("{repo_owner}", repo_ower)
            .replace("{repo_name}", repo_name);

        let client = Client::new();
        let response = client
            .get(url.clone())
            .header("User-Agent", "phenoxtractor")
            .send()?;

        let release: Release = response.json()?;
        debug!("Got Release version {:#?} from {url}", release);
        Ok(release.tag_name)
    }

    /// Downloads a specific file from a specified release version of a repository.
    ///
    /// # Arguments
    ///
    /// * `repo_owner` - The owner of the Github repository.
    /// * `repo_name` - The name of the Github repository.
    /// * `file_name` - The name of the file to download from the release assets.
    /// * `version` - The release version tag (e.g., "v1.0.0").
    ///
    /// # Returns
    ///
    /// A `Result` containing the `reqwest::blocking::Response` on success,
    /// which can be used to stream the file content. Returns an `reqwest::Error`
    pub fn get_release_file(
        &self,
        repo_ower: &str,
        repo_name: &str,
        file_name: &str,
        version: &str,
    ) -> Result<Response, reqwest::Error> {
        let mut url = self.web_url.clone();
        url = url
            .replace("{repo_owner}", repo_ower)
            .replace("{repo_name}", repo_name)
            .replace("{file_name}", file_name)
            .replace("{version}", version);

        let resp = get(url.clone())?;

        debug!("GithubReleaseClient got file from {url}");
        Ok(resp)
    }
}

impl Default for GithubReleaseClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::ServerGuard;
    use rstest::{fixture, rstest};
    use serde_json::{Value, json};

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

        let latest_json_payload = serde_json::to_string(&Release {
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

        server
    }

    #[rstest]
    fn test_get_latest_release_tag(
        mock_server: ServerGuard,
        latest_tag: String,
        repo_owner: String,
        repo_name: String,
    ) {
        let mut mock_client = GithubReleaseClient::new();

        let mut mock_url = mock_server.url();
        mock_url.push_str("/repos/{repo_owner}/{repo_name}/releases/latest");
        mock_client.latest_release_url = mock_url;

        let version = mock_client
            .get_latest_release_tag(repo_owner.as_ref(), repo_name.as_ref())
            .unwrap();
        assert_eq!(version, latest_tag);
    }

    #[rstest]
    fn test_get_release_file(
        mock_server: ServerGuard,
        repo_owner: String,
        repo_name: String,
        release_version: String,
        release_file_name: String,
    ) {
        let mut mock_client = GithubReleaseClient::new();

        let mut mock_url = mock_server.url();
        mock_url.push_str("/{repo_owner}/{repo_name}/releases/download/{version}/{file_name}");

        mock_client.web_url = mock_url;

        let response = mock_client
            .get_release_file(
                repo_owner.as_ref(),
                repo_name.as_str(),
                release_file_name.as_str(),
                release_version.as_str(),
            )
            .unwrap();

        assert_eq!(
            response.json::<Value>().unwrap(),
            json!({
                "graphs": {"id": "some_id"},
                "version": release_version,
            })
        );
    }
}
