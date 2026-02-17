#![allow(unused)]
use crate::ontology::error::BiDictError;
use crate::ontology::resource_references::{KnownResourcePrefixes, ResourceRef};
use crate::ontology::traits::{BiDict, HasVersion};
use elsa::FrozenMap;
use regex::Regex;
use reqwest::blocking::Client;
use securiety::{CurieParser, CurieParsing, CurieRegexValidator, CurieValidation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{OnceLock, RwLock};
use std::{env, fmt};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoincRelease {
    pub version: String,
    pub release_date: String,
    pub relma_version: String,
    pub number_of_loincs: i64,
    pub max_loinc: String,
    pub download_url: String,
    #[serde(rename = "downloadMD5Hash")]
    pub download_md5_hash: String,
}
impl From<LoincRelease> for ResourceRef {
    fn from(value: LoincRelease) -> Self {
        ResourceRef::from(KnownResourcePrefixes::LOINC).with_version(&value.version)
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct LoincResponse {
    pub response_summary: ResponseSummary,
    pub results: Vec<LoincResult>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseSummary {
    pub records_found: u32,
    pub starting_offset: u32,
    pub rows_returned: u32,
    pub loinc_version: String,
    pub copyright: String,
    pub query_url: String,
    pub query_execution_time: String,
    pub query_duration: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoincResult {
    #[serde(rename = "LOINC_NUM")]
    pub loinc_num: String,
    #[serde(rename = "COMPONENT")]
    pub component: String,
    #[serde(rename = "PROPERTY")]
    pub property: String,
    #[serde(rename = "TIME_ASPCT")]
    pub time_aspect: String,
    #[serde(rename = "SYSTEM")]
    pub system: String,
    #[serde(rename = "SCALE_TYP")]
    pub scale_type: String,
    #[serde(rename = "METHOD_TYP")]
    pub method_type: Option<String>,
    #[serde(rename = "CLASS")]
    pub class: String,
    #[serde(rename = "VersionLastChanged")]
    pub version_last_changed: String,
    #[serde(rename = "CHNG_TYPE")]
    pub change_type: String,
    #[serde(rename = "STATUS")]
    pub status: String,
    #[serde(rename = "CLASSTYPE")]
    pub class_type: i32,
    #[serde(rename = "RELATEDNAMES2")]
    pub related_names: String,
    #[serde(rename = "SHORTNAME")]
    pub short_name: String,
    #[serde(rename = "LONG_COMMON_NAME")]
    pub long_common_name: String,
    #[serde(rename = "LHCForms")]
    pub lhc_forms: String,
    #[serde(rename = "FormalName")]
    pub formal_name: String,
    #[serde(rename = "Tags")]
    pub tags: Vec<String>,
    #[serde(rename = "Link")]
    pub link: String,

    #[serde(rename = "DefinitionDescription")]
    pub definition_description: Option<String>,
    #[serde(rename = "FORMULA")]
    pub formula: Option<String>,
    #[serde(rename = "EXAMPLE_UNITS")]
    pub example_units: Option<String>,
    #[serde(rename = "PanelType")]
    pub panel_type: Option<String>,
    #[serde(rename = "VersionFirstReleased")]
    pub version_first_released: Option<String>,
}

pub struct LoincClient {
    client: Client,
    base_url: String,
    user_name: String,
    password: String,
    cache: FrozenMap<String, Box<str>>,
    reference: OnceLock<ResourceRef>,
    curie_validator: CurieRegexValidator,
}

impl fmt::Debug for LoincClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoincClient")
            .field("base_url", &self.base_url)
            .field("user_name", &self.user_name)
            .field("password", &"********")
            .field("client", &"reqwest::Client")
            .field("cache_size", &self.cache.len())
            .field("reference_initialized", &self.reference.get().is_some())
            .finish()
    }
}

impl LoincClient {
    pub fn new(user_name: String, password: String, reference: Option<ResourceRef>) -> Self {
        let reference_lock = match reference {
            Some(r) if r.version() != "latest" && !r.version().is_empty() => OnceLock::from(r),
            _ => OnceLock::new(),
        };

        Self {
            client: Client::new(),
            base_url: "https://loinc.regenstrief.org/searchapi/".to_string(),
            user_name,
            password,
            cache: FrozenMap::default(),
            reference: reference_lock,
            curie_validator: CurieRegexValidator::loinc(),
        }
    }

    fn query(&self, id_or_label: &str) -> Result<Vec<LoincResult>, BiDictError> {
        let url = format!("{}loincs", self.base_url);
        let params = [("query", id_or_label), ("rows", "10")];

        let loinc_response: LoincResponse = self
            .client
            .get(url)
            .basic_auth(self.user_name.clone(), Some(self.password.clone()))
            .query(&params)
            .send()?
            .json()?;

        Ok(loinc_response.results)
    }

    fn format_loinc_curie(loinc_number: &str) -> String {
        format!("{}:{}", KnownResourcePrefixes::LOINC, loinc_number)
    }
}

impl Default for LoincClient {
    fn default() -> Self {
        let username =
            env::var("LOINC_USERNAME").expect("LOINC_USERNAME must be set in .env or environment");
        let password =
            env::var("LOINC_PASSWORD").expect("LOINC_PASSWORD must be set in .env or environment");

        Self::new(username, password, None)
    }
}

impl BiDict for LoincClient {
    fn get(&self, id_or_label: &str) -> Result<&str, BiDictError> {
        if self.curie_validator.validate(id_or_label) {
            self.get_label(id_or_label)
        } else {
            self.get_id(id_or_label)
        }
    }

    fn get_label(&self, id: &str) -> Result<&str, BiDictError> {
        if let Some(label) = self.cache.get(id) {
            return Ok(label);
        }
        let loinc_search_results = self.query(id)?;

        let result = loinc_search_results
            .into_iter()
            .find(|r| Self::format_loinc_curie(&r.loinc_num) == id || r.loinc_num == id)
            .ok_or_else(|| BiDictError::NotFound(id.into()))?;

        let label_ref = self
            .cache
            .insert(id.to_string(), Box::from(result.long_common_name));

        Ok(label_ref)
    }

    fn get_id(&self, term: &str) -> Result<&str, BiDictError> {
        if let Some(loinc_number) = self.cache.get(term) {
            return Ok(loinc_number);
        }

        let cleaned: String = term.chars().filter(|c| !c.is_ascii_punctuation()).collect();

        let loinc_search_results = self.query(&cleaned)?;

        for loinc_result in loinc_search_results {
            if loinc_result.long_common_name.to_lowercase() == term.to_lowercase() {
                self.cache.insert(
                    term.to_string(),
                    Box::from(Self::format_loinc_curie(&loinc_result.loinc_num)),
                );
            }
        }
        match self.cache.get(term) {
            None => Err(BiDictError::NotFound(term.into())),
            Some(id) => Ok(id),
        }
    }

    fn reference(&self) -> &ResourceRef {
        self.reference.get_or_init(|| {
            let res: LoincRelease = self
                .client
                .get("https://loinc.regenstrief.org/api/v1/Loinc")
                .basic_auth(&self.user_name, Some(&self.password))
                .send()
                .expect("Loinc client request failed")
                .json()
                .expect("Loinc client request failed");

            ResourceRef::from(res)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_suite::phenopacket_component_generation::{
        default_qual_loinc, default_quant_loinc,
    };
    use dotenvy::dotenv;
    use rstest::{fixture, rstest};
    use std::env;

    #[fixture]
    fn loinc_client() -> LoincClient {
        dotenv().ok();

        LoincClient::default()
    }

    #[rstest]
    fn test_get_label(loinc_client: LoincClient) {
        let res = loinc_client.get_label(default_quant_loinc().id.as_str());
        assert_eq!(res.unwrap(), default_quant_loinc().label);
    }

    #[rstest]
    fn test_get_id(loinc_client: LoincClient) {
        let label = default_qual_loinc().label;
        let res = loinc_client.get_id(label.as_str());

        assert!(res.is_ok(), "Should find an ID for term: {}", label);
        assert_eq!(res.unwrap(), default_qual_loinc().id);
    }

    #[rstest]
    fn test_get_bidirectional(loinc_client: LoincClient) {
        let id_input = "LOINC:97062-4";

        let label_res = loinc_client.get(&id_input);

        assert!(
            label_res.is_ok(),
            "Should find an ID for input: {}",
            id_input
        );
        let found_label = label_res.unwrap();

        let id_res = loinc_client.get(found_label);
        assert!(
            id_res.is_ok(),
            "Should find an ID for output: {}",
            found_label
        );

        assert_eq!(id_res.unwrap(), id_input);
    }
}
