// OMIM client for querying BioPortal (including BIDict)
use crate::ontology::traits::BIDict;
use crate::ontology::error::BiDictError;
use reqwest::blocking::Client;
use urlencoding::encode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::sync::RwLock;

#[derive(Debug, Serialize, Deserialize)]
pub struct OmimResult {
    #[serde(default)]
    pub id: String,
    #[serde(default, alias = "prefLabel")]
    pub label: String,
    #[serde(default)]
    pub synonym: Vec<String>,
    #[serde(default, rename = "@id")]
    pub at_id: String,
}

// client for querying OMIM terms via BioPortal with caching
// Supports querying by ID (numeric or `OMIM:` CURIE) or by label/synonym.
// Implements the `BIDict` trait for bidirectional lookups.
#[derive(Debug)]
pub struct OmimClient {
    client: Client,
    api_key: String,
    cache: RwLock<HashMap<String, String>>,
}

impl OmimClient {
    /// Creates a new OMIM client.
    /// Expects: `BIOPORTAL_API_KEY` environment variable to be set
    pub fn new() -> Self {
        let api_key = env::var("BIOPORTAL_API_KEY")
            .expect("BIOPORTAL_API_KEY must be set in environment");

        OmimClient {
            client: Client::new(),
            api_key,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Creates a new OMIM client with an explicit API key
    pub fn new_with_key(api_key: String) -> Self {
        OmimClient {
            client: Client::new(),
            api_key,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Validate that an OMIM id is numeric, optionally prefixed with `OMIM:`.
    fn validate_id_format(id: &str) -> Result<&str, BiDictError> {
        let trimmed = id.strip_prefix("OMIM:").unwrap_or(id);
        if !trimmed.is_empty() && trimmed.chars().all(|c| c.is_ascii_digit()) {
            Ok(trimmed)
        } else {
            Err(BiDictError::InvalidId(id.to_string()))
        }
    }

    /// Query BioPortal for an OMIM term by ID (numeric or `OMIM:` prefixed)
    fn query_by_id(&self, id: &str) -> Result<OmimResult, BiDictError> {
        let id_trimmed = Self::validate_id_format(id)?;
        let url = format!(
            "https://data.bioontology.org/ontologies/OMIM/classes/http%3A%2F%2Fpurl.bioontology.org%2Fontology%2FOMIM%2F{}",
            id_trimmed
        );

        let resp = self.client
            .get(&url)
            .header("Authorization", format!("apikey token={}", self.api_key))
            .send()
            .map_err(|e| BiDictError::Request(e))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().unwrap_or_default();
            eprintln!("BioPortal API error for {}: status={}, body={}", url, status, body);
            return Err(BiDictError::NotFound(id.to_string()));
        }

        let mut result: OmimResult = resp.json().map_err(|e| BiDictError::Request(e))?;

        // Extract ID from @id field if not present in id field
        if result.id.is_empty() && !result.at_id.is_empty() {
            // Extract the numeric ID from the URI: http://purl.bioontology.org/ontology/OMIM/147920 -> 147920
            if let Some(last_part) = result.at_id.split('/').last() {
                result.id = last_part.to_string();
            }
        }
        if result.id.is_empty() {
            result.id = id_trimmed.to_string();
        }

        Ok(result)
    }

    /// Query BioPortal for an OMIM term by label (returns first exact match)
    fn query_by_label(&self, label: &str) -> Result<OmimResult, BiDictError> {
        let encoded_label = encode(label);
        let url = format!(
            "https://data.bioontology.org/search?q={}&ontologies=OMIM&require_exact_match=true&apikey={}",
            encoded_label, self.api_key
        );

        let resp = self.client
            .get(&url)
            .send()
            .map_err(|e| BiDictError::Request(e))?;

        if !resp.status().is_success() {
            return Err(BiDictError::NotFound(label.to_string()));
        }

        #[derive(Deserialize)]
        struct SearchResponse {
            collection: Vec<OmimResult>,
        }

        let search_resp: SearchResponse = resp.json().map_err(|e| BiDictError::Request(e))?;

        // Return first matching term and extract ID from @id if necessary
        let mut result = search_resp.collection.into_iter().next().ok_or(BiDictError::NotFound(label.to_string()))?;
        
        // Extract ID from @id field if not present
        if result.id.is_empty() && !result.at_id.is_empty() {
            if let Some(last_part) = result.at_id.split('/').last() {
                result.id = last_part.to_string();
            }
        }
        
        Ok(result)
    }

    /// Read a value from the internal cache.
    fn cache_read(&self, key: &str) -> Option<String> {
        self.cache.read().ok()?.get(key).cloned()
    }

    /// Write a value to the internal cache.
    fn cache_write(&self, key: &str, value: &str) {
        if let Ok(mut cache) = self.cache.write() {
            cache.insert(key.to_string(), value.to_string());
        }
    }
}

impl BIDict for OmimClient {
    /// Get the label or OMIM ID for a given query.
    fn get(&self, id_or_label: &str) -> Result<&str, BiDictError> {
        if id_or_label.chars().all(|c| c.is_ascii_digit()) || id_or_label.starts_with("OMIM:") {
            self.get_label(id_or_label)
        } else {
            self.get_id(id_or_label)
        }
    }

    /// Get the official label for an OMIM ID (stores synonyms in cache).
    fn get_label(&self, id: &str) -> Result<&str, BiDictError> {
        // Fail fast for invalid ids before performing network requests.
        Self::validate_id_format(id)?;

        if let Some(label) = self.cache_read(id) {
            return Ok(Box::leak(label.into_boxed_str()));
        }

        let result = self.query_by_id(id)?;
        self.cache_write(id, &result.label);
        self.cache_write(&result.label, &format!("OMIM:{}", result.id));

        // Cache all synonyms for reverse lookup
        for syn in &result.synonym {
            self.cache_write(syn, &format!("OMIM:{}", result.id));
        }

        Ok(Box::leak(result.label.into_boxed_str()))
    }

    /// Get the OMIM ID for a given label or synonym (stores label and synonyms in cache).
    fn get_id(&self, label: &str) -> Result<&str, BiDictError> {
        if let Some(id) = self.cache_read(label) {
            return Ok(Box::leak(id.into_boxed_str()));
        }

        let result = self.query_by_label(label)?;
        self.cache_write(&result.label, &format!("OMIM:{}", result.id));
        self.cache_write(&format!("OMIM:{}", result.id), &result.label);

        // Cache all synonyms
        for syn in &result.synonym {
            self.cache_write(syn, &format!("OMIM:{}", result.id));
        }

        Ok(Box::leak(format!("OMIM:{}", result.id).into_boxed_str()))
    }
}
