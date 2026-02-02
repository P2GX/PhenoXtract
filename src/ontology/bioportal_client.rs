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
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Duration;

impl fmt::Debug for BioPortalClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cache_len = self.cache.read().map(|c| c.len()).unwrap_or(0);

        f.debug_struct("BioPortalClient")
            .field("base_url", &self.base_url)
            .field("ontology", &self.ontology)
            .field("prefix", &self.prefix)
            .field("iri_prefix", &self.iri_prefix)
            .field(
                "local_id_regex",
                &self.local_id_regex.as_ref().map(|re| re.as_str()),
            )
            .field("cache_len", &cache_len)
            .field("api_key", &"<redacted>")
            .field("rate_limiter", &"<ratelimited>")
            .field("resource_ref", &self.resource_ref)
            .finish()
    }
}

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

pub struct BioPortalClient {
    client: Client,
    base_url: String,
    api_key: String,

    ontology: String,
    prefix: String, // alternativ KnownResourcePrefixes? but then less flexible
    // for custom ontologies
    local_id_regex: Option<Regex>,
    iri_prefix: String,

    cache: RwLock<HashMap<String, Arc<str>>>,
    rate_limiter: Ratelimiter,

    resource_ref: ResourceRef,
}

impl BioPortalClient {
    /// Parse + validate CURIE using securiety.
    /// - input prefix case-insensitive
    /// - must match self.prefix
    /// - returns local id (reference) as owned String
    fn check_curie_local_id(&self, input: &str) -> Result<String, BiDictError> {
        let prefix_key = self.prefix.to_lowercase();

        let curie = match CurieParser::from_prefix(&prefix_key) {
            Some(parser) => parser
                .parse(input)
                .map_err(|_e| BiDictError::InvalidId(input.to_owned()))?,
            None => CurieParser::general()
                .parse(input)
                .map_err(|_e| BiDictError::InvalidId(input.to_owned()))?,
        };

        if !curie.prefix().eq_ignore_ascii_case(&self.prefix) {
            return Err(BiDictError::InvalidId(input.to_owned()));
        }

        Ok(curie.reference().to_string())
    }

    fn format_curie(&self, local_id: &str) -> String {
        format!("{}:{}", self.prefix, local_id)
    }

    fn wait_for_rate_limit(&self) {
        loop {
            match self.rate_limiter.try_wait() {
                Ok(_) => return,
                Err(sleep) => std::thread::sleep(sleep),
            }
        }
    }

    /// Caching
    /// --------------------
    /// Append-only cache read.
    /// SAFETY: Entries are never removed or overwritten, so the underlying `Arc<str>`
    /// remains alive for the lifetime of `self`. We can therefore return `&str` tied to `&self`.
    fn cache_read<'a>(&'a self, key: &str) -> Option<&'a str> {
        let cache = self.cache.read().ok()?;
        let arc_str = cache.get(key)?.clone();

        // Extend lifetime from lock-guard to &'a self.
        // This is sound under the invariant: cache is append-only (no overwrite/remove).
        Some(unsafe { &*(arc_str.as_ref() as *const str) })
    }

    /// Insert only if absent. Never overwrite.
    fn cache_write(&self, key: &str, value: &str) {
        if let Ok(mut cache) = self.cache.write() {
            cache
                .entry(key.to_string())
                .or_insert_with(|| Arc::<str>::from(value));
        }
    }
}

impl BioPortalClient {
    /// Build a configured BioPortal client.
    ///
    /// - `api_key`: BioPortal API key
    /// - `ontology`: BioPortal ontology acronym (e.g. "OMIM", "HP")
    /// - `prefix`: canonical CURIE prefix you want to output (we keep it as given; you want uppercase)
    /// - `reference`: optional ResourceRef override (otherwise derived from prefix, version=latest)
    /// - `local_id_regex`: optional regex to treat bare local IDs as IDs (e.g. OMIM: digits-only)
    pub fn new_with_key(
        api_key: String,
        ontology: String,
        prefix: impl Into<String>,
        reference: Option<ResourceRef>,
        local_id_regex: Option<Regex>,
    ) -> Result<Self, BiDictError> {
        let base_url = "https://data.bioontology.org".to_string();

        // keep prefix exactly as configured (you want canonical uppercase output)
        let prefix: String = prefix.into();

        // BioPortal class IRI pattern (common across ontologies hosted there)
        let iri_prefix = format!("http://purl.bioontology.org/ontology/{}/", ontology);

        // Rate limiter: 4 requests / second (token bucket)
        let rate_limiter = Ratelimiter::builder(4, Duration::from_secs(1))
            .max_tokens(4)
            .initial_available(4)
            .build()
            .map_err(|e| BiDictError::Caching {
                reason: format!("Failed to build ratelimiter: {e}"),
            })?;

        // ResourceRef is what BiDict::reference() returns.
        // If caller did not provide one, derive from prefix and use latest.
        let resource_ref = reference.unwrap_or_else(|| ResourceRef::from(prefix.as_str()));

        Ok(Self {
            client: Client::new(),
            base_url,
            api_key,
            ontology,
            prefix,
            local_id_regex,
            iri_prefix,
            cache: RwLock::new(HashMap::<String, Arc<str>>::new()),
            rate_limiter,
            resource_ref,
        })
    }
}
