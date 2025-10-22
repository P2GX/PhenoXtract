use crate::ontology::error::ClientError;
use reqwest::blocking::{Response, get};

pub struct ObolibraryClient {
    base_url: String,
}

impl ObolibraryClient {
    /// Fetches an ontology file from the configured OBO PURL server.
    ///
    /// This method constructs the appropriate URL based on the provided version.
    /// - If `version` is `"latest"`, it constructs a URL like:
    ///   `{base_url}/{file_name}`
    /// - For any other `version` string, it constructs a URL for a specific release:
    ///   `{base_url}/{ontology_prefix}/releases/{version}/{file_name}`
    ///
    /// # Parameters
    ///
    /// * `ontology_prefix` - The short prefix for the ontology (e.g., "go", "mondo", "hp").
    ///   This is used to build the path for versioned releases.
    /// * `file_name` - The full name of the file to download (e.g., "go.owl", "mondo.json").
    /// * `version` - The version of the ontology to fetch. Use `"latest"` for the current
    ///   version, or a specific release tag (e.g., "2023-09-01") for a versioned file.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    /// * `Ok(Response)`: A `reqwest::blocking::Response` object on success. The caller is
    ///   responsible for processing this response (e.g., checking status codes and reading
    ///   the body with methods like `.text()` or `.bytes()`).
    /// * `Err(reqwest::Error)`: An error if the HTTP request fails due to network issues,
    ///   DNS problems, or other `reqwest`-related errors.
    ///
    /// # Errors
    ///
    /// This function will return an error if the underlying HTTP GET request fails.
    pub fn get_ontology(
        &self,
        ontology_prefix: &str,
        file_name: &str,
        version: &str,
    ) -> Result<Response, ClientError> {
        let url = match version {
            "latest" => format!("{}/{}", self.base_url, file_name),
            _ => format!(
                "{}/{}/releases/{}/{}",
                self.base_url, ontology_prefix, version, file_name
            ),
        };

        let resp = get(url.clone())?;

        Ok(resp)
    }
}

impl Default for ObolibraryClient {
    fn default() -> Self {
        ObolibraryClient {
            base_url: "https://purl.obolibrary.org/obo/".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::Server;

    #[test]
    fn test_get_ontology_latest_version() {
        let mut server = Server::new();
        let url = server.url();

        let mock = server
            .mock("GET", "/go.owl")
            .with_status(200)
            .with_body("ontology content")
            .create();

        let client = ObolibraryClient {
            base_url: url.to_string(),
        };

        let result = client.get_ontology("go", "go.owl", "latest");

        mock.assert();
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().unwrap(), "ontology content");
    }

    #[test]
    fn test_get_ontology_specific_version() {
        let mut server = Server::new();
        let url = server.url();

        let mock = server
            .mock("GET", "/mondo/releases/2023-09-01/mondo.json")
            .with_status(200)
            .with_body("specific version content")
            .create();

        let client = ObolibraryClient {
            base_url: url.to_string(),
        };

        let result = client.get_ontology("mondo", "mondo.json", "2023-09-01");

        mock.assert();
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().unwrap(), "specific version content");
    }

    #[test]
    fn test_get_ontology_request_error() {
        let client = ObolibraryClient {
            base_url: "http://localhost:1".to_string(), // Invalid port
        };

        let result = client.get_ontology("go", "go.owl", "latest");

        assert!(result.is_err());
    }
}
