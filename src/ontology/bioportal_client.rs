use crate::ontology::error::BiDictError;
use crate::ontology::resource_references::ResourceRef;
use crate::ontology::traits::BiDict;

use elsa::sync::FrozenMap;
use ratelimit::Ratelimiter;
use reqwest::blocking::Client;
use reqwest::{StatusCode, Url};
use securiety::CurieRegexValidator;
use securiety::curie::Curie;
use securiety::curie_parser::CurieParser;
use securiety::traits::CurieParsing;
use serde::Deserialize;
use std::fmt;
use std::time::Duration;

impl fmt::Debug for BioPortalClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cache_len = self.cache.len();

        f.debug_struct("BioPortalClient")
            .field("base_url", &self.base_url)
            .field("bioportal_acronym", &self.bioportal_acronym)
            .field("curie_prefix", &self.curie_prefix)
            .field("iri_prefix", &self.iri_prefix)
            .field("curie_parser", &"<curie-parser>")
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

    bioportal_acronym: String,
    curie_prefix: String,
    curie_parser: CurieParser<CurieRegexValidator>,
    iri_prefix: String,

    cache: FrozenMap<String, Box<str>>,
    rate_limiter: Ratelimiter,

    resource_ref: ResourceRef,
}

impl BioPortalClient {
    /// Parse + validate CURIE using securiety.
    /// - input must be a CURIE
    /// - prefix must match this client's configured CURIE prefix (case-insensitive)
    fn parse_curie(&self, input: &str) -> Result<Curie, BiDictError> {
        let curie = self
            .curie_parser
            .parse(input)
            .map_err(|_e| BiDictError::InvalidId(input.to_owned()))?;

        // Enforce prefix matches this client (case-insensitive)
        if !curie.prefix().eq_ignore_ascii_case(&self.curie_prefix) {
            return Err(BiDictError::InvalidId(input.to_owned()));
        }

        Ok(curie)
    }

    fn format_curie(&self, local_id: &str) -> String {
        format!("{}:{}", self.curie_prefix, local_id)
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
    /// Convenience constructor: assume CURIE prefix == BioPortal acronym (pragmatic default).
    pub fn new_with_key_for_ontology(
        api_key: String,
        bioportal_acronym: String,
        reference: Option<ResourceRef>,
    ) -> Result<Self, BiDictError> {
        let curie_prefix = bioportal_acronym.clone();
        Self::new_with_key(api_key, bioportal_acronym, curie_prefix, reference)
    }

    /// Build a configured BioPortal client.
    /// - `api_key`: BioPortal API key
    /// - `ontology`: BioPortal ontology acronym used in API paths (e.g. "SNOMEDCT", "HP")
    /// - `prefix`: CURIE namespace used for input/output (often same as `ontology`, may differ)
    /// - `reference`: optional ResourceRef override (otherwise derived from prefix, version=latest)
    /// - `local_id_regex`: optional regex to treat bare local IDs as IDs (e.g. OMIM: digits-only)
    pub fn new_with_key(
        api_key: String,
        ontology: String,
        prefix: impl Into<String>,
        reference: Option<ResourceRef>,
    ) -> Result<Self, BiDictError> {
        let base_url = "https://data.bioontology.org".to_string();

        // keep prefix exactly as configured (you want canonical uppercase output)
        let curie_prefix: String = prefix.into();

        // Build parser once, tied to the configured CURIE namespace
        let prefix_key = curie_prefix.to_lowercase();
        let curie_parser = match CurieParser::from_prefix(prefix_key.as_str()) {
            Some(parser) => parser,
            None => CurieParser::general(),
        };

        // BioPortal class IRI pattern (common across ontologies hosted there)
        let iri_prefix = format!("http://purl.bioontology.org/ontology/{}/", ontology);

        // Rate limiter: 4 requests / second (token bucket)
        let rate_limiter = Ratelimiter::builder(4, Duration::from_secs(1))
            .max_tokens(15)
            .initial_available(15)
            .build()
            .map_err(|e| BiDictError::Caching {
                reason: format!("Failed to build ratelimiter: {e}"),
            })?;

        // ResourceRef is what BiDict::reference() returns.
        // If caller did not provide one, derive from prefix and use latest.
        let resource_ref = reference.unwrap_or_else(|| ResourceRef::from(curie_prefix.as_str()));

        Ok(Self {
            client: Client::new(),
            base_url,
            api_key,
            bioportal_acronym: ontology,
            curie_prefix,
            curie_parser,
            iri_prefix,
            cache: FrozenMap::<String, Box<str>>::new(),
            rate_limiter,
            resource_ref,
        })
    }

    fn class_url(&self, local_id: &str) -> Result<Url, BiDictError> {
        // Build BioPortal "class" endpoint URL.
        let iri = format!("{}{}", self.iri_prefix, local_id);
        let base = format!(
            "{}/ontologies/{}/classes",
            self.base_url, self.bioportal_acronym
        );

        let mut url = Url::parse(&base).map_err(|e| BiDictError::Caching {
            reason: format!("Invalid BioPortal base URL '{base}': {e}"),
        })?;

        url.path_segments_mut()
            .map_err(|_| BiDictError::Caching {
                reason: format!("Invalid BioPortal class URL (URL does not support path segment mutation): {base}"),
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
            .append_pair("ontologies", &self.bioportal_acronym)
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

    fn query_by_id(&self, local_id: &str) -> Result<BioPortalClass, BiDictError> {
        // Fetch a class by local id via BioPortal "classes" endpoint.
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

        let resp = resp.error_for_status()?;
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
            .send()?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Err(BiDictError::NotFound(label.to_string()));
        }

        let resp = resp.error_for_status()?;
        let search = resp.json::<BioPortalSearchResponse>()?;

        search
            .collection
            .into_iter()
            .next()
            .ok_or_else(|| BiDictError::NotFound(label.to_string()))
    }
}

impl BiDict for BioPortalClient {
    fn get(&self, id_or_label: &str) -> Result<&str, BiDictError> {
        // Dispatch helper for BiDict lookups.

        // Determines whether the input should be treated as an identifier (CURIE or bare local id)
        // or as a label/synonym, and routes to `get_label` (id -> label) or `get_id` (label -> id).
        // This function performs only lightweight heuristics for routing; strict CURIE validation
        // is handled by `securiety` inside `get_label` when needed.
        let _ = self.parse_curie(id_or_label)?;
        self.get_label(id_or_label)
    }

    fn get_label(&self, id: &str) -> Result<&str, BiDictError> {
        // Resolves a CURIE identifier to its preferred label (id -> label).
        // STRICT: `id` must be a CURIE with a prefix matching `self.prefix`.
        let _ = self.parse_curie(id)?;

        if let Some(label) = self.cache.get(id) {
            return Ok(label);
        }

        let curie = self.parse_curie(id)?;
        let local_id = curie.reference();
        let canonical_curie = self.format_curie(local_id);

        if let Some(label) = self.cache.get(&canonical_curie) {
            return Ok(label);
        }

        let result = self.query_by_id(local_id)?;
        if result.label.is_empty() {
            return Err(BiDictError::NotFound(canonical_curie));
        }

        self.cache
            .insert(canonical_curie.to_string(), result.label.to_string().into());
        self.cache
            .insert(result.label.to_string(), canonical_curie.to_string().into());

        // Optional: cache synonyms -> canonical CURIE (keep if you want)
        for syn in result.synonym {
            self.cache.insert(syn, canonical_curie.to_string().into());
        }

        self.cache
            .get(&canonical_curie)
            .ok_or_else(|| BiDictError::NotFound(canonical_curie))
    }

    /// Resolves a label or synonym to the canonical identifier (label -> id).
    /// Uses the in-memory cache first; on cache miss, performs an exact-match BioPortal search,
    /// extracts the local id from the returned `@id` IRI, and constructs the canonical CURIE.
    /// Caches canonical mappings:
    /// - canonical CURIE -> label
    /// - label -> canonical CURIE
    /// - each synonym -> canonical CURIE
    /// 
    /// Returns the canonical CURIE as `&str` backed by an append-only cache.
    fn get_id(&self, term: &str) -> Result<&str, BiDictError> {
        if let Some(id) = self.cache.get(term) {
            return Ok(id);
        }

        let result = self.query_by_label(term)?;

        if result.label.is_empty() {
            return Err(BiDictError::NotFound(term.to_string()));
        }

        let local_id = Self::extract_local_id_from_iri(&result.at_id)
            .ok_or_else(|| BiDictError::NotFound(term.to_string()))?;
        let canonical_curie = self.format_curie(local_id);

        self.cache
            .insert(canonical_curie.to_string(), result.label.to_string().into());
        self.cache
            .insert(result.label, canonical_curie.to_string().into());
        for syn in result.synonym {
            self.cache.insert(syn, canonical_curie.to_string().into());
        }

        self.cache
            .get(term)
            .ok_or_else(|| BiDictError::NotFound(term.to_string()))
    }

    fn reference(&self) -> &ResourceRef {
        &self.resource_ref
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::{Matcher, Server};

    // Test helper: build client
    // -------------------------
    fn test_client(base_url: String) -> BioPortalClient {
        let api_key = "TEST_KEY".to_string();

        let rate_limiter = Ratelimiter::builder(1000, Duration::from_secs(1))
            .max_tokens(1000)
            .initial_available(1000)
            .build()
            .unwrap();

        let bioportal_acronym = "OMIM".to_string();
        let curie_prefix = "OMIM".to_string();

        // Build the parser once (same approach as in new_with_key)
        let prefix_key = curie_prefix.to_lowercase();
        let curie_parser = match CurieParser::from_prefix(prefix_key.as_str()) {
            Some(parser) => parser,
            None => CurieParser::general(),
        };

        BioPortalClient {
            client: Client::new(),
            base_url,
            api_key,
            bioportal_acronym,
            curie_prefix,
            iri_prefix: "http://purl.bioontology.org/ontology/OMIM/".to_string(),
            curie_parser,
            cache: FrozenMap::new(),
            rate_limiter,
            resource_ref: ResourceRef::from("OMIM"),
        }
    }

    // Demonstrate that CURIE prefix may differ from BioPortal acronym (e.g. identifiers.org style).
    fn test_client_snomed_like(base_url: String) -> BioPortalClient {
        let api_key = "TEST_KEY".to_string();

        let rate_limiter = Ratelimiter::builder(1000, Duration::from_secs(1))
            .max_tokens(1000)
            .initial_available(1000)
            .build()
            .unwrap();

        let bioportal_acronym = "SNOMEDCT".to_string();
        let curie_prefix = "snomedct".to_string();

        let prefix_key = curie_prefix.to_lowercase();
        let curie_parser = match CurieParser::from_prefix(prefix_key.as_str()) {
            Some(parser) => parser,
            None => CurieParser::general(),
        };

        BioPortalClient {
            client: Client::new(),
            base_url,
            api_key,
            bioportal_acronym: bioportal_acronym.clone(),
            curie_prefix: curie_prefix.clone(),
            iri_prefix: format!(
                "http://purl.bioontology.org/ontology/{}/",
                bioportal_acronym
            ),
            curie_parser,
            cache: FrozenMap::new(),
            rate_limiter,
            resource_ref: ResourceRef::from(curie_prefix.as_str()),
        }
    }
    // 1) Unit tests
    // --------------

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
    fn test_parse_curie_rejects_wrong_prefix() {
        let server = Server::new();
        let client = test_client(server.url());

        let err = client.parse_curie("HP:1234567").unwrap_err();
        matches!(err, BiDictError::InvalidId(_));
    }

    #[test]
    fn test_prefix_can_differ_from_ontology() {
        let server = Server::new();
        let client = test_client_snomed_like(server.url());

        // Accept both cases
        assert!(client.parse_curie("snomedct:90391002").is_ok());
        assert!(client.parse_curie("SNOMEDCT:90391002").is_ok());
    }

    #[test]
    fn test_check_curie_local_id_rejects_wrong_prefix() {
        let server = Server::new();
        let client = test_client(server.url());

        let err = client.parse_curie("HP:1234567").unwrap_err();
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

    // 3) BiDict behaviour tests (caching + strict CURIE handling)
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

    #[test]
    fn test_get_rejects_non_curie() {
        let server = Server::new();
        let client = test_client(server.url());

        let err = client.get("147920").unwrap_err();
        match err {
            BiDictError::InvalidId(_) => {}
            other => panic!("expected InvalidId, got {other:?}"),
        }

        let err2 = client.get("Kabuki syndrome 1").unwrap_err();
        match err2 {
            BiDictError::InvalidId(_) => {}
            other => panic!("expected InvalidId, got {other:?}"),
        }
    }
}
