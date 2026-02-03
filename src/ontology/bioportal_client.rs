use crate::ontology::error::BiDictError;
use crate::ontology::resource_references::ResourceRef;
use crate::ontology::traits::BiDict;
use crate::utils::is_curie;

use ratelimit::Ratelimiter;
use regex::Regex;
use reqwest::blocking::Client;
use reqwest::{StatusCode, Url};
use securiety::curie_parser::CurieParser;
use serde::Deserialize;
use std::collections::HashMap;
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
    prefix: String, // alternativ KnownResourcePrefixes?
    // but could be less flexible for other ontologies
    local_id_regex: Option<Regex>,
    iri_prefix: String,

    cache: RwLock<HashMap<String, Arc<str>>>,
    rate_limiter: Ratelimiter,

    resource_ref: ResourceRef,
}

impl BioPortalClient {
    /* Parse + validate CURIE using securiety.
     - Normalise prefix casing for parsing:
     - input prefix case-insensitive
     - must match self.prefix
     - returns local id (reference) as owned String
    */
    fn check_curie_local_id(&self, input: &str) -> Result<String, BiDictError> {
        let normalised = if let Some((p, r)) = crate::utils::is_curie(input) {
            if p.eq_ignore_ascii_case(&self.prefix) && p != self.prefix {
                // rebuild with canonical prefix
                format!("{}:{}", self.prefix, r)
            } else {
                input.to_string()
            }
        } else {
            input.to_string()
        };

        let prefix_key = self.prefix.to_lowercase();

        let curie = match CurieParser::from_prefix(&prefix_key) {
            Some(parser) => parser
                .parse(&normalised)
                .map_err(|_e| BiDictError::InvalidId(input.to_owned()))?,
            None => CurieParser::general()
                .parse(&normalised)
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
        // Blocks until a token is available.
        loop {
            match self.rate_limiter.try_wait() {
                Ok(_) => return,
                Err(sleep) => std::thread::sleep(sleep),
            }
        }
    }
}

impl BioPortalClient {
    /*
    Read from append-only cache. SAFETY rationale:
    - The cache stores `Arc<str>` values.
    - We guarantee the cache is append-only: no overwrite, no remove.
    - Therefore, once a value is inserted, its backing `str` remains alive for the lifetime of `self`.
    - We can safely return `&str` tied to `&self`.
    */
    fn cache_read<'a>(&'a self, key: &str) -> Option<&'a str> {
        let cache = self.cache.read().ok()?;
        let s: &str = cache.get(key)?.as_ref();

        // Extend lifetime from lock guard to &'a self under the append-only invariant.
        Some(unsafe { &*(s as *const str) })
    }

    /// Insert only if absent (append-only).
    /// This avoids invalidating previously returned `&str` references.
    fn cache_write(&self, key: &str, value: &str) {
        if let Ok(mut cache) = self.cache.write() {
            cache
                .entry(key.to_string())
                .or_insert_with(|| Arc::<str>::from(value));
        }
    }
}

impl BioPortalClient {
    /* Build a configured BioPortal client.
     - `api_key`: BioPortal API key
     - `ontology`: BioPortal ontology acronym (e.g. "OMIM", "HP")
     - `prefix`: canonical CURIE prefix you want to output (we keep it as given; you want uppercase)
     - `reference`: optional ResourceRef override (otherwise derived from prefix, version=latest)
     - `local_id_regex`: optional regex to treat bare local IDs as IDs (e.g. OMIM: digits-only)
    */
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

impl BioPortalClient {
    // Build BioPortal "class" endpoint URL.
    fn class_url(&self, local_id: &str) -> Result<Url, BiDictError> {
        let iri = format!("{}{}", self.iri_prefix, local_id);
        let base = format!("{}/ontologies/{}/classes", self.base_url, self.ontology);

        let mut url = Url::parse(&base).map_err(|e| BiDictError::Caching {
            reason: format!("Invalid BioPortal base URL '{base}': {e}"),
        })?;

        url.path_segments_mut()
            .map_err(|_| BiDictError::Caching {
                reason: "BioPortal base URL cannot be a base".to_string(),
            })?
            .push(&iri);

        Ok(url)
    }

    // Build BioPortal search URL.
    fn search_url(&self, query: &str) -> Result<Url, BiDictError> {
        let base = format!("{}/search", self.base_url);

        let mut url = Url::parse(&base).map_err(|e| BiDictError::Caching {
            reason: format!("Invalid BioPortal search URL '{base}': {e}"),
        })?;

        url.query_pairs_mut()
            .append_pair("q", query)
            .append_pair("ontologies", &self.ontology)
            .append_pair("require_exact_match", "true");

        Ok(url)
    }

    // Takes the last path segment from an IRI, e.g.
    // "http://purl.bioontology.org/ontology/OMIM/147920" -> Some("147920")
    fn extract_local_id_from_iri(iri: &str) -> Option<&str> {
        iri.trim_end_matches('/')
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
    }
}

// Network calls to BioPortal
impl BioPortalClient {
    // Fetch a class by local id via BioPortal "classes" endpoint.
    fn query_by_id(&self, local_id: &str) -> Result<BioPortalClass, BiDictError> {
        let url = self.class_url(local_id)?;

        self.wait_for_rate_limit();

        let resp = self
            .client
            .get(url)
            .header("Authorization", format!("apikey token={}", self.api_key))
            .send()
            .map_err(BiDictError::Request)?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Err(BiDictError::NotFound(local_id.to_string()));
        }

        let resp = resp.error_for_status().map_err(BiDictError::Request)?;
        resp.json::<BioPortalClass>().map_err(BiDictError::Request)
    }

    // Search a class by preferred label via BioPortal /search.
    fn query_by_label(&self, label: &str) -> Result<BioPortalClass, BiDictError> {
        let url = self.search_url(label)?;

        self.wait_for_rate_limit();

        let resp = self
            .client
            .get(url)
            .header("Authorization", format!("apikey token={}", self.api_key))
            .send()
            .map_err(BiDictError::Request)?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Err(BiDictError::NotFound(label.to_string()));
        }

        let resp = resp.error_for_status().map_err(BiDictError::Request)?;
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
        let api_key = std::env::var("BIOPORTAL_API_KEY")
            .expect("BIOPORTAL_API_KEY must be set in .env or environment");

        // Beispielwerte: musst du passend setzen oder einen weiteren Default-Builder nutzen
        BioPortalClient::new_with_key(
            api_key,
            "OMIM".to_string(),
            "OMIM",
            None,
            Some(regex::Regex::new(r"^\d+$").unwrap()),
        )
        .expect("Failed to build BioPortalClient")
    }
}

impl BiDict for BioPortalClient {
    fn get(&self, id_or_label: &str) -> Result<&str, BiDictError> {
        /*
        Dispatch helper for BiDict lookups.

        Determines whether the input should be treated as an identifier (CURIE or bare local id)
        or as a label/synonym, and routes to `get_label` (id -> label) or `get_id` (label -> id).
        This function performs only lightweight heuristics for routing; strict CURIE validation
        is handled by `securiety` inside `get_label` when needed.
        */
        if is_curie(id_or_label).is_some()
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
        /*
        Resolves an identifier to its preferred label (id -> label).

        Accepts either a CURIE (validated and parsed via `securiety`) or a bare local id
        (when configured via `local_id_regex`). Uses the in-memory cache first; on cache miss,
        fetches the class record from BioPortal, then caches canonical mappings:
        - canonical CURIE -> label
        - label -> canonical CURIE
        - each synonym -> canonical CURIE
        Returns the label as `&str` backed by an append-only cache.
        */
        if let Some(label) = self.cache_read(id) {
            return Ok(label);
        }

        let local_id: String = if is_curie(id).is_some() {
            self.check_curie_local_id(id)?.to_string()
        } else {
            id.to_string()
        };

        let canonical_curie = self.format_curie(&local_id);

        if let Some(label) = self.cache_read(&canonical_curie) {
            return Ok(label);
        }

        let result = self.query_by_id(&local_id)?;

        if result.label.is_empty() {
            return Err(BiDictError::NotFound(canonical_curie));
        }

        self.cache_write(&canonical_curie, &result.label);
        self.cache_write(&result.label, &canonical_curie);
        for syn in &result.synonym {
            self.cache_write(syn, &canonical_curie);
        }

        self.cache_read(&canonical_curie)
            .ok_or_else(|| BiDictError::NotFound(canonical_curie))
    }

    fn get_id(&self, term: &str) -> Result<&str, BiDictError> {
        /*
        Resolves a label or synonym to the canonical identifier (label -> id).

        Uses the in-memory cache first; on cache miss, performs an exact-match BioPortal search,
        extracts the local id from the returned `@id` IRI, and constructs the canonical CURIE.
        Caches canonical mappings:
        - canonical CURIE -> label
        - label -> canonical CURIE
        - each synonym -> canonical CURIE
        Returns the canonical CURIE as `&str` backed by an append-only cache.
        */
        if let Some(id) = self.cache_read(term) {
            return Ok(id);
        }

        let result = self.query_by_label(term)?;

        if result.label.is_empty() {
            return Err(BiDictError::NotFound(term.to_string()));
        }

        let local_id = Self::extract_local_id_from_iri(&result.at_id)
            .ok_or_else(|| BiDictError::NotFound(term.to_string()))?;
        let canonical_curie = self.format_curie(local_id);

        self.cache_write(&canonical_curie, &result.label);
        self.cache_write(&result.label, &canonical_curie);
        for syn in &result.synonym {
            self.cache_write(syn, &canonical_curie);
        }

        self.cache_read(term)
            .ok_or_else(|| BiDictError::NotFound(term.to_string()))
    }

    fn reference(&self) -> &crate::ontology::resource_references::ResourceRef {
        &self.resource_ref
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::{Matcher, Server};
    use regex::Regex;

    // -------------------------
    // Test helper: build client
    // -------------------------
    fn test_client(base_url: String) -> BioPortalClient {
        dotenvy::dotenv().ok();

        let api_key =
            std::env::var("BIOPORTAL_API_KEY").expect("BIOPORTAL_API_KEY must be set for tests");

        let rate_limiter = Ratelimiter::builder(1000, Duration::from_secs(1))
            .max_tokens(1000)
            .initial_available(1000)
            .build()
            .unwrap();

        BioPortalClient {
            client: Client::new(),
            base_url,
            api_key, // <- now in scope
            ontology: "OMIM".to_string(),
            prefix: "OMIM".to_string(),
            local_id_regex: Some(Regex::new(r"^\d+$").unwrap()),
            iri_prefix: "http://purl.bioontology.org/ontology/OMIM/".to_string(),
            cache: RwLock::new(HashMap::new()),
            rate_limiter,
            resource_ref: ResourceRef::from("OMIM"),
        }
    }

    // -------------------------
    // 1) Unit tests (pure logic)
    // -------------------------

    #[test]
    fn test_extract_local_id_from_iri() {
        let iri = "http://purl.bioontology.org/ontology/OMIM/147920";
        assert_eq!(
            BioPortalClient::extract_local_id_from_iri(iri),
            Some("147920")
        );

        let trailing = "http://purl.bioontology.org/ontology/OMIM/147920/";
        assert_eq!(
            BioPortalClient::extract_local_id_from_iri(trailing),
            Some("147920")
        );
    }

    #[test]
    fn test_check_curie_local_id_case_insensitive_prefix() {
        // prefix is OMIM, but input uses lowercase
        let server = Server::new();
        let client = test_client(server.url());

        let local = client.check_curie_local_id("omim:147920").unwrap();
        assert_eq!(local, "147920");
    }

    #[test]
    fn test_check_curie_local_id_rejects_wrong_prefix() {
        let server = Server::new();
        let client = test_client(server.url());

        let err = client.check_curie_local_id("HP:1234567").unwrap_err();
        match err {
            BiDictError::InvalidId(_) => {}
            other => panic!("expected InvalidId, got {other:?}"),
        }
    }

    #[test]
    fn test_class_url_contains_encoded_iri() {
        let server = Server::new();
        let client = test_client(server.url());

        let url = client.class_url("147920").unwrap();
        let path = url.path().to_string();

        // Should include encoded IRI segment (http%3A%2F%2F...)
        assert!(path.ends_with("OMIM%2F147920"));
    }

    #[test]
    fn test_search_url_contains_expected_query_params() {
        let server = Server::new();
        let client = test_client(server.url());

        let url = client.search_url("Kabuki syndrome 1").unwrap();
        let q = url.query().unwrap_or("");

        // order is not guaranteed, so we just check the presence
        assert!(q.contains("q=Kabuki"));
        assert!(q.contains("ontologies=OMIM"));
        assert!(q.contains("require_exact_match=true"));
    }

    // -----------------------------------------
    // 2) Network-call tests (mocked HTTP)
    // -----------------------------------------

    #[test]
    fn test_query_by_id_success() {
        let mut server = Server::new();
        let client = test_client(server.url());

        let url = client.class_url("147920").unwrap();
        let path = url.path().to_string();

        let body = r#"
        {
          "prefLabel": "KABUKI SYNDROME 1",
          "@id": "http://purl.bioontology.org/ontology/OMIM/147920",
          "synonym": ["Kabuki syndrome type 1"]
        }
        "#;

        let _m = server
            .mock("GET", path.as_str())
            .match_header(
                "authorization",
                Matcher::Exact(format!("apikey token={}", client.api_key)),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .expect(1)
            .create();

        let res = client.query_by_id("147920").unwrap();
        assert_eq!(res.label, "KABUKI SYNDROME 1");
        assert_eq!(
            res.at_id,
            "http://purl.bioontology.org/ontology/OMIM/147920"
        );
        assert_eq!(res.synonym, vec!["Kabuki syndrome type 1".to_string()]);
    }

    #[test]
    fn test_query_by_id_not_found() {
        let mut server = Server::new();
        let client = test_client(server.url());

        let url = client.class_url("0000000").unwrap();
        let path = url.path().to_string();

        let _m = server
            .mock("GET", path.as_str())
            .match_header(
                "authorization",
                Matcher::Exact(format!("apikey token={}", client.api_key)),
            )
            .with_status(404)
            .expect(1)
            .create();

        let err = client.query_by_id("0000000").unwrap_err();
        match err {
            BiDictError::NotFound(x) => assert_eq!(x, "0000000"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn test_query_by_label_success_first_hit() {
        let mut server = Server::new();
        let client = test_client(server.url());

        let body = r#"
        {
          "collection": [
            {
              "prefLabel": "KABUKI SYNDROME 1",
              "@id": "http://purl.bioontology.org/ontology/OMIM/147920",
              "synonym": ["Kabuki syndrome type 1"]
            }
          ]
        }
        "#;

        let _m = server
            .mock("GET", "/search")
            .match_header(
                "authorization",
                Matcher::Exact(format!("apikey token={}", client.api_key)),
            )
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("q".into(), "Kabuki syndrome 1".into()),
                Matcher::UrlEncoded("ontologies".into(), "OMIM".into()),
                Matcher::UrlEncoded("require_exact_match".into(), "true".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .expect(1)
            .create();

        let res = client.query_by_label("Kabuki syndrome 1").unwrap();
        assert_eq!(res.label, "KABUKI SYNDROME 1");
    }

    #[test]
    fn test_query_by_label_empty_collection_is_not_found() {
        let mut server = Server::new();
        let client = test_client(server.url());

        let body = r#"{ "collection": [] }"#;

        let _m = server
            .mock("GET", "/search")
            .match_header(
                "authorization",
                Matcher::Exact(format!("apikey token={}", client.api_key)),
            )
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("q".into(), "DoesNotExist".into()),
                Matcher::UrlEncoded("ontologies".into(), "OMIM".into()),
                Matcher::UrlEncoded("require_exact_match".into(), "true".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .expect(1)
            .create();

        let err = client.query_by_label("DoesNotExist").unwrap_err();
        match err {
            BiDictError::NotFound(x) => assert_eq!(x, "DoesNotExist"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    // -------------------------------------------------
    // 3) BiDict behaviour tests (routing + caching)
    // -------------------------------------------------

    #[test]
    fn test_get_label_from_curie_caches_result() {
        let mut server = Server::new();
        let client = test_client(server.url());

        // mock: query_by_id("147920") will be called exactly once
        let url = client.class_url("147920").unwrap();
        let path = url.path().to_string();

        let body = r#"
        {
          "prefLabel": "KABUKI SYNDROME 1",
          "@id": "http://purl.bioontology.org/ontology/OMIM/147920",
          "synonym": ["Kabuki syndrome type 1"]
        }
        "#;

        let _m = server
            .mock("GET", path.as_str())
            .match_header(
                "authorization",
                Matcher::Exact(format!("apikey token={}", client.api_key)),
            )
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .expect(1)
            .create();

        // 1st call: hits network
        let label1 = client.get_label("OMIM:147920").unwrap();
        assert_eq!(label1, "KABUKI SYNDROME 1");

        // 2nd call: should be served from cache (no extra HTTP calls)
        let label2 = client.get_label("OMIM:147920").unwrap();
        assert_eq!(label2, "KABUKI SYNDROME 1");
    }

    #[test]
    fn test_get_id_from_label_caches_synonym() {
        let mut server = Server::new();
        let client = test_client(server.url());

        let body = r#"
        {
          "collection": [
            {
              "prefLabel": "KABUKI SYNDROME 1",
              "@id": "http://purl.bioontology.org/ontology/OMIM/147920",
              "synonym": ["Kabuki syndrome type 1"]
            }
          ]
        }
        "#;

        let _m = server
            .mock("GET", "/search")
            .match_header(
                "authorization",
                Matcher::Exact(format!("apikey token={}", client.api_key)),
            )
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("q".into(), "KABUKI SYNDROME 1".into()),
                Matcher::UrlEncoded("ontologies".into(), "OMIM".into()),
                Matcher::UrlEncoded("require_exact_match".into(), "true".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(body)
            .expect(1)
            .create();

        // 1st: network search
        let id = client.get_id("KABUKI SYNDROME 1").unwrap();
        assert_eq!(id, "OMIM:147920");

        // 2nd: synonym should now resolve from cache without another search
        let id2 = client.get_id("Kabuki syndrome type 1").unwrap();
        assert_eq!(id2, "OMIM:147920");
    }
}
