#![allow(unused)]

use crate::ontology::traits::BIDict;
use regex::bytes::Regex;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::RwLock;

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

pub(crate) struct LoincClient {
    client: Client,
    base_url: String,
    user_name: String,
    password: String,
    cache: RwLock<HashMap<String, Vec<LoincResult>>>,
    loinc_id_regex: Regex,
}

impl LoincClient {
    const LOINC_PREFIX: &'static str = "LOINC:";
    pub fn new(user_name: &str, password: &str) -> Self {
        Self {
            client: Client::new(),
            base_url: "https://loinc.regenstrief.org/searchapi/".to_string(),
            user_name: user_name.to_string(),
            password: password.to_string(),
            cache: RwLock::new(HashMap::new()),
            loinc_id_regex: Regex::from_str(r"^\d{1,8}-\d$").unwrap(),
        }
    }

    fn query(&self, id_or_label: &str) -> Option<Vec<LoincResult>> {
        {
            let cache_read = self.cache.read().ok()?;
            if let Some(value) = cache_read.get(id_or_label) {
                return Some(value.to_vec());
            }
        }

        let url = format!("{}loincs", self.base_url);
        let params = [("query", id_or_label), ("rows", "10")];

        let loinc_response: LoincResponse = self
            .client
            .get(url)
            .basic_auth(self.user_name.clone(), Some(self.password.clone()))
            .query(&params)
            .send()
            .ok()?
            .json()
            .ok()?;

        if let Ok(mut cache_write) = self.cache.write() {
            cache_write.insert(id_or_label.to_string(), loinc_response.results.clone());

            if self.is_loinc_curie(id_or_label) {
                let loinc_number = id_or_label.split(":").last().unwrap().to_string();
                cache_write.insert(loinc_number, loinc_response.results.clone());
            } else if self.loinc_id_regex.is_match(id_or_label.as_bytes()) {
                let loinc_curie = Self::format_loinc_curie(id_or_label);
                cache_write.insert(loinc_curie, loinc_response.results.clone());
            }
        }

        Some(loinc_response.results)
    }

    fn format_loinc_curie(loinc_numer: &str) -> String {
        format!("{}{}", Self::LOINC_PREFIX, loinc_numer)
    }
    fn is_loinc_curie(&self, query: &str) -> bool {
        match query.split(':').next_back() {
            None => false,
            Some(loinc_number) => {
                query.starts_with(Self::LOINC_PREFIX)
                    && self.loinc_id_regex.is_match(loinc_number.as_bytes())
            }
        }
    }
}

impl BIDict for LoincClient {
    fn get(&self, id_or_label: &str) -> Option<String> {
        if self.is_loinc_curie(id_or_label) || self.loinc_id_regex.is_match(id_or_label.as_ref()) {
            self.get_term(id_or_label)
        } else {
            self.get_id(id_or_label)
        }
    }

    fn get_term(&self, id: &str) -> Option<String> {
        let loinc_search_results = self.query(id)?;

        if loinc_search_results.len() == 1 {
            let loinc_result = loinc_search_results.first().unwrap();
            Some(loinc_result.long_common_name.clone())
        } else {
            None
        }
    }

    fn get_id(&self, term: &str) -> Option<String> {
        let cleaned: String = term.chars().filter(|c| !c.is_ascii_punctuation()).collect();

        let loinc_search_results = self.query(&cleaned)?;

        for loinc_result in loinc_search_results {
            if loinc_result.long_common_name.to_lowercase() == term.to_lowercase() {
                return Some(Self::format_loinc_curie(&loinc_result.loinc_num));
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenvy::dotenv;
    use std::env;

    fn setup_client() -> LoincClient {
        dotenv().ok();
        let user_name =
            env::var("LOINC_USERNAME").expect("LOINC_USERNAME must be set in .env or environment");
        let password =
            env::var("LOINC_PASSWORD").expect("LOINC_PASSWORD must be set in .env or environment");

        LoincClient::new(&user_name, &password)
    }

    #[test]
    fn test_get_term() {
        let loinc_client = setup_client();

        let res = loinc_client.get_term("LOINC:97062-4");
        assert_eq!(res.unwrap(), "History of High blood glucose");
    }

    #[test]
    fn test_get_id() {
        let loinc_client = setup_client();

        let term = "Glucose [Measurement] in Urine";
        let res = loinc_client.get_id(term);

        assert!(res.is_some(), "Should find an ID for term: {}", term);
        assert!(
            res.unwrap().starts_with("LOINC:"),
            "ID should have the LOINC: prefix"
        );
    }

    #[test]
    fn test_get_id_prefix() {
        let loinc_client = setup_client();

        let id_input = "97062-4";
        let id_input_with_prefix = format!("LOINC:{}", id_input);

        let label_res = loinc_client.get(id_input);
        let label_res_with_prefix = loinc_client.get(&id_input_with_prefix);
        assert_eq!(label_res, label_res_with_prefix);
    }

    #[test]
    fn test_get_term_id_prefix() {
        let loinc_client = setup_client();

        let id_input = "97062-4";
        let id_input_with_prefix = format!("LOINC:{}", id_input);

        let label_res = loinc_client.get_term(id_input);
        let label_res_with_prefix = loinc_client.get_term(&id_input_with_prefix);
        assert_eq!(label_res, label_res_with_prefix);
    }

    #[test]
    fn test_get_bidirectional() {
        let loinc_client = setup_client();

        let id_input = "97062-4";
        let id_input_with_prefix = format!("LOINC:{}", id_input);
        let label_res = loinc_client.get(&id_input_with_prefix);

        assert!(label_res.is_some());
        let found_label = label_res.unwrap();

        let id_res = loinc_client.get(&found_label);
        assert!(id_res.is_some());

        assert_eq!(id_res.unwrap(), id_input_with_prefix);
    }
}
