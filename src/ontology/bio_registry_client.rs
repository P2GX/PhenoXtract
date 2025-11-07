use crate::ontology::error::ClientError;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BioRegistryResource {
    pub prefix: String,
    pub name: Option<String>,
    pub uri_format: Option<String>,
    pub homepage: Option<String>,
    pub version: Option<String>,
    pub download_owl: Option<String>,
    pub download_obo: Option<String>,
    pub download_json: Option<String>,
    pub download_rdf: Option<String>,
    pub preferred_prefix: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BioRegistryClient {
    api_url: String,
}

impl BioRegistryClient {
    pub fn new(api_url: &str) -> Self {
        let mut url = api_url.to_string();
        if !url.ends_with("/") {
            url += "/";
        }
        BioRegistryClient { api_url: url }
    }

    /// Fetches resource metadata for a given prefix from the Bioregistry.
    ///
    /// This method sends a GET request to the `/api/registry/{prefix}` endpoint.
    /// It sets a custom `User-Agent` header for the request. The JSON response
    /// from the API is automatically deserialized into a `BioBankResource` struct.
    ///
    /// # Parameters
    ///
    /// * `prefix` - The prefix for the resource to look up (e.g., "doid", "go").
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    /// * `Ok(BioBankResource)`: The successfully deserialized resource information.
    /// * `Err(reqwest::Error)`: An error if the request fails. This can happen due to
    ///   network issues, if the server returns a non-success status code, or if the
    ///   response body cannot be deserialized into a `BioBankResource`.
    ///
    /// # Errors
    ///
    /// This function will return an error if the HTTP request fails or if the
    /// JSON deserialization of the response body fails.
    pub fn get_resource(&self, prefix: &str) -> Result<BioRegistryResource, ClientError> {
        let url = self.api_url.clone() + "registry/" + prefix;

        let client = Client::new();
        let response = client
            .get(url.clone())
            .header("User-Agent", "phenoxtractor")
            .send()?;

        Ok(response.json()?)
    }
}

impl Default for BioRegistryClient {
    fn default() -> Self {
        BioRegistryClient::new("https://bioregistry.io/api/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn test_get_resource() {
        let client = BioRegistryClient::default();

        let hpo_resource = client.get_resource("hp").unwrap();

        assert_eq!(hpo_resource.prefix, "hp".to_string());
    }
}
