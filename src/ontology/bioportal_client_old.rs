/*
use crate::ontology::error::BiDictError;
use crate::ontology::resource_references::{KnownResourcePrefixes, ResourceRef};
use crate::ontology::traits::BiDict;
use crate::utils::{check_curie_format, is_curie};

use ratelimit::Ratelimiter;
use regex::Regex;
use reqwest::Url;
use reqwest::blocking::Client;
use securiety::curie::Curie;
use securiety::curie_parser::CurieParser;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::sync::RwLock;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct BioPortalClass {
    #[serde(default, alias = "prefLabel")]
    pub label: String,

    #[serde(default, rename = "@id")]
    pub at_id: String,

    #[serde(default)]
    pub synonym: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct BioPortalSearchResponse {
    #[serde(default)]
    collection: Vec<BioPortalClass>,
}

#[derive(Debug)]
pub struct BioPortalClient {
    client: Client,
    base_url: String,
    api_key: String,

    ontology: String,
    prefix: String,

    local_id_regex: Option<Regex>,
    iri_prefix: String,

    cache: RwLock<HashMap<String, String>>,
    rate_limiter: Ratelimiter,

    resource_ref: ResourceRef,
}

/// BioPortalClient implementation
///
/// 1) construction & configuration
/// -----------------
impl BioPortalClient {
    pub fn new_with_key(
        api_key: String,
        ontology: String,
        prefix: KnownResourcePrefixes,
        reference: Option<ResourceRef>,
    ) -> Result<Self, BiDictError> {
        let base_url = "https://data.bioontology.org".to_string();

        let iri_prefix = format!("http://purl.bioontology.org/ontology/{}/", ontology);

        let rate_limiter = Ratelimiter::builder(4, Duration::from_secs(1))
            .max_tokens(4)
            .initial_available(4)
            .build()
            .map_err(|e| BiDictError::Other(format!("Failed to build ratelimiter: {e}")))?;

        let resource_ref = reference.unwrap_or_else(|| {
            ResourceRef::from(prefix)
        });

        Ok(Self {
            client: Client::new(),
            base_url,
            api_key,
            ontology,
            prefix,
            local_id_regex: None,
            iri_prefix,
            cache: RwLock::new(HashMap::new()),
            rate_limiter,
            resource_ref,
        })
    }

    // 2) helper functions
    // -----------------

    fn prefix_str(&self) -> &str {
        self.prefix.as_ref()
    }
    fn format_curie(&self, local_id: &str) -> String {
        format!("{}:{}", self.prefix_str(), local_id)
    }
    /// check whether input is a CURIE
    ///
    fn check_curie(&self, id: &str) -> Result<Curie, BiDictError> {
        match CurieParser::from_prefix(&self.prefix) {
            Some(parser) => parser
                .parse(id)
                .map_err(|_err| BiDictError::InvalidId(id.into())),
            None => CurieParser::general()
                .parse(id)
                .map_err(|_err| BiDictError::InvalidId(id.into())),
        }
    }

    // waiting helper for rate limiter
    fn wait_for_rate_limit(&self) {
        loop {
            match self.rate_limiter.try_wait() {
                Ok(_) => return,s
                Err(sleep) => std::thread::sleep(sleep),
            }
        }
    }

    fn cache_read(&self, key: &str) -> Option<String> {
        let cache = self.cache.read().unwrap();
        cache.get(key).cloned()
    }

    fn cache_write(&self, key: &str, value: &str) {
        if let Ok(mut cache) = self.cache.write() {
            cache.insert(key.to_string(), value.to_string());
        }
    }

    // build BioPortal API URL
    fn class_url(&self, local_id: &str) -> String {
        let iri = format!("{}/{}", self.iri_prefix, local_id);
        let base = format!("{}/ontologies/{}/classes", self.base_url, self.ontology);
        let mut url = reqwest::Url::parse(&base).expect("Invalid BioPortal base URL");

        url.path_segments_mut()
            .expect("BioPortal base URL cannot be a base")
            .push(&iri);
        url.query_pairs_mut().append_pair("apikey", &self.api_key);
        url.to_string()
    }
    // search BioPortal URL
    fn search_url(&self, query: &str) -> String {
        let base = format!("{}/search", self.base_url);
        let mut url = reqwest::Url::parse(&base).expect("Invalid BioPortal base URL");

        url.query_pairs_mut()
            .append_pair("q", query)
            .append_pair("ontologies", &self.ontology)
            .append_pair("require_exact_match", "true")
            .append_pair("apikey", &self.api_key);

        url.to_string()
    }
    // takes last segment from IRI
    fn extract_local_id_from_iri(iri: &str) -> Option<&str> {
        iri.split('/').next_back().filter(|s| !s.is_empty())
    }

    // 3) network calls
    // -----------------
    fn query_by_id(&self, local_id: &str) -> Result<BioPortalClass, BiDictError> {
        let url = self.class_url(local_id);

        self.wait_for_rate_limit();
        let resp = self.client.get(url).send().map_err(BiDictError::Request)?;

        if !resp.status().is_success() {
            return Err(BiDictError::NotFound(local_id.to_string()));
        }

        resp.json::<BioPortalClass>().map_err(BiDictError::Request)
    }

    fn query_by_label(&self, label: &str) -> Result<BioPortalClass, BiDictError> {
        let url = self.search_url(label);

        self.wait_for_rate_limit();
        let resp = self.client.get(url).send().map_err(BiDictError::Request)?;

        if !resp.status().is_success() {
            return Err(BiDictError::NotFound(label.to_string()));
        }

        let search = resp
            .json::<BioPortalSearchResponse>()
            .map_err(BiDictError::Request)?;

        search
            .collection
            .into_iter()
            .next()
            .ok_or_else(|| BiDictError::NotFound(label.to_string()))
    }
}

impl Default for BioPortalClient {
    fn default() -> Self {
        let api_key = env::var("BIOPORTAL_API_KEY")
            .expect("BIOPORTAL_API_KEY must be set in .env or environment");
        Self::new(api_key, None)
    }
}

impl BiDict for BioPortalClient {
    fn get(&self, id_or_label: &str) -> Result<&str, BiDictError> {
        if self.is_expected_curie(id_or_label)
            || self
                .local_id_regex
                .as_ref()
                .map(|re| re.is_match(id_or_label))
                .unwrap_or(false)
        {
            self.get_label(id_or_label)
        } else {
            self.get_id(id_or_label)
        }
    }

    fn get_label(&self, id: &str) -> Result<&str, BiDictError> {
        let (curie, local_id) = self.check_curie(id)?;

        if let Some(label) = self.cache_read(&id) {
            return Ok(&label);
        }

        let result = self.query_by_id(&local_id)?;
        if result.pref_label.is_empty() {
            return Err(BiDictError::NotFound(curie));
        }

        // Store mappings
        self.cache_write(&result.id, &result.pref_label); // id -> label
        self.cache_write(&result.pref_label, &result.id); // label -> id
        for syn in &result.synonym {
            self.cache_write(syn, &result.id); // synonym -> id
        }

        self.cache_read(&curie)
            .ok_or_else(|| BiDictError::NotFound(curie))
    }

    fn get_id(&self, label_or_synonym: &str) -> Result<&str, BiDictError> {
        if let Some(id) = self.cache_read(label_or_synonym) {
            return Ok(&id);
        }

        let result = self.query_by_label(label_or_synonym)?;
        if result.pref_label.is_empty() {
            return Err(BiDictError::NotFound(label_or_synonym.to_string()));
        }

        let local_id = Self::extract_local_id_from_iri(&result.at_id)
            .ok_or_else(|| BiDictError::NotFound(label_or_synonym.to_string()))?;

        let curie = self.format_curie(local_id);

        self.cache_write(&curie, &result.pref_label); // id -> label
        self.cache_write(&result.pref_label, &curie); // label -> id
        for syn in &result.synonym {
            self.cache_write(syn, &curie); // synonym -> id
        }

        self.cache_read(label_or_synonym)
            .ok_or_else(|| BiDictError::NotFound(label_or_synonym.to_string()))
    }

    fn reference(&self) -> &ResourceRef {
        &self.resource_ref
    }
}

/// tests for BioPortalClient
#[cfg(test)]
mod tests {
    use super::*;
    use dotenvy::dotenv;

    #[test]
    fn test_cache_roundtrip_like_loinc_style() {
        let client = BioPortalClient::new_with_key(
            "dummy".to_string(),
            "DUMMY".to_string(),
            KnownResourcePrefixes::OMIM,
        );

        client.cache_write("A", "B");
        assert_eq!(client.cache_read("A").unwrap(), "B");
    }

    /// Live test is generic: configure ontology/prefix in env (so OMIM is only “hardcoded” in your local .env).
    ///
    /// Required env vars:
    /// - BIOPORTAL_API_KEY
    /// - BIOPORTAL_TEST_ONTOLOGY (e.g. "OMIM")
    /// - BIOPORTAL_TEST_PREFIX   (must match a KnownResourcePrefixes variant name)
    /// - BIOPORTAL_TEST_CURIE    (e.g. "OMIM:614200")
    /// - BIOPORTAL_TEST_LABEL    (expected prefLabel)
    /// Optional:
    /// - BIOPORTAL_TEST_SYNONYM
    #[test]
    fn test_live_bioportal_generic() {
        dotenv().ok();

        let api_key = match std::env::var("BIOPORTAL_API_KEY") {
            Ok(v) => v,
            Err(_) => {
                println!("Skipping live test: BIOPORTAL_API_KEY not set");
                return;
            }
        };

        let ontology = match std::env::var("BIOPORTAL_TEST_ONTOLOGY") {
            Ok(v) => v,
            Err(_) => {
                println!("Skipping live test: BIOPORTAL_TEST_ONTOLOGY not set");
                return;
            }
        };

        let prefix_str = match std::env::var("BIOPORTAL_TEST_PREFIX") {
            Ok(v) => v,
            Err(_) => {
                println!("Skipping live test: BIOPORTAL_TEST_PREFIX not set");
                return;
            }
        };

        // If KnownResourcePrefixes implements FromStr in your repo, this works.
        // Otherwise, replace with a small match helper.
        let prefix: KnownResourcePrefixes = match prefix_str.parse() {
            Ok(p) => p,
            Err(_) => {
                println!("Skipping live test: BIOPORTAL_TEST_PREFIX not parseable");
                return;
            }
        };

        let test_curie = match std::env::var("BIOPORTAL_TEST_CURIE") {
            Ok(v) => v,
            Err(_) => {
                println!("Skipping live test: BIOPORTAL_TEST_CURIE not set");
                return;
            }
        };

        let expected_label = match std::env::var("BIOPORTAL_TEST_LABEL") {
            Ok(v) => v,
            Err(_) => {
                println!("Skipping live test: BIOPORTAL_TEST_LABEL not set");
                return;
            }
        };

        let client = BioPortalClient::new_with_key(api_key, ontology, prefix, None);

        let label = client.get_label(&test_curie).expect("get_label failed");
        assert_eq!(label, expected_label);

        let id = client
            .get_id(&expected_label)
            .expect("get_id(label) failed");
        assert_eq!(id, test_curie);

        if let Ok(syn) = std::env::var("BIOPORTAL_TEST_SYNONYM") {
            let id2 = client.get_id(&syn).expect("get_id(synonym) failed");
            assert_eq!(id2, test_curie);
        }
    }
}
 */