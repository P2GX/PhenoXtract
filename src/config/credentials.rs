use dotenvy::dotenv;
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq, Default)]
pub struct Credentials {
    pub loinc_credentials: LoincCredentials,
}

#[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
pub struct LoincCredentials {
    pub username: String,
    pub password: String,
}

impl Default for LoincCredentials {
    fn default() -> Self {
        dotenv().ok();
        let username =
            env::var("LOINC_USERNAME").expect("LOINC_USERNAME must be set in .env or environment");
        let password =
            env::var("LOINC_PASSWORD").expect("LOINC_PASSWORD must be set in .env or environment");
        LoincCredentials { username, password }
    }
}
